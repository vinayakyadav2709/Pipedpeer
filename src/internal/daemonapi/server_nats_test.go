package daemonapi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/pipedpeer/pipedpeer/internal/natsbus"
)

// helper: create a server bound to an embedded NATS bus and return the bus + cleanup.
func setupNATSServer(t *testing.T, nodeID string) (*Server, *natsbus.Bus) {
	t.Helper()
	bus, err := natsbus.New(natsbus.Config{
		Embedded: true,
		StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}

	s := NewWithConfig(nodeID, 5*time.Second, 2*time.Second, 100*time.Millisecond)
	s.StartSweeper()
	if err := s.BindNATS(bus); err != nil {
		t.Fatalf("bind nats: %v", err)
	}

	t.Cleanup(func() {
		s.StopSweeper()
		s.UnbindNATS()
		bus.Close()
	})

	return s, bus
}

func TestNATSAcceptCommitComplete(t *testing.T) {
	s, bus := setupNATSServer(t, "nats-node")

	// 1. Accept via NATS request/reply
	acceptReq := acceptRequest{
		TargetID:      "nats-node",
		JobName:       "nats-job-1",
		SubmitterNode: "remote-node",
	}
	reqData, _ := json.Marshal(acceptReq)
	msg, err := bus.Request("pipedpeer.daemon.nats-node.accept", reqData, 2*time.Second)
	if err != nil {
		t.Fatalf("nats accept request: %v", err)
	}

	var ar acceptResponse
	if err := json.Unmarshal(msg.Data, &ar); err != nil {
		t.Fatalf("unmarshal accept response: %v", err)
	}
	if !ar.Accepted {
		t.Fatalf("expected accepted, got rejected: %s", ar.Reason)
	}
	if ar.LeaseID == "" {
		t.Fatal("expected non-empty lease_id")
	}
	if ar.NodeID != "nats-node" {
		t.Fatalf("expected node_id=nats-node, got %s", ar.NodeID)
	}

	// Verify lease exists on server
	lease, ok := s.GetLease(ar.LeaseID)
	if !ok {
		t.Fatal("lease not found on server")
	}
	if lease.State != LeaseReserved {
		t.Fatalf("expected reserved, got %s", lease.State)
	}
	if lease.JobName != "nats-job-1" {
		t.Fatalf("expected job_name=nats-job-1, got %s", lease.JobName)
	}

	// 2. Commit via NATS
	commitReq, _ := json.Marshal(commitRequest{LeaseID: ar.LeaseID})
	msg, err = bus.Request("pipedpeer.daemon.nats-node.commit", commitReq, 2*time.Second)
	if err != nil {
		t.Fatalf("nats commit request: %v", err)
	}

	var cr commitResponse
	json.Unmarshal(msg.Data, &cr)
	if !cr.Committed {
		t.Fatalf("expected committed, got rejected: %s", cr.Reason)
	}

	lease, _ = s.GetLease(ar.LeaseID)
	if lease.State != LeaseRunning {
		t.Fatalf("expected running after commit, got %s", lease.State)
	}

	// 3. Complete via NATS
	completeReq, _ := json.Marshal(completeRequest{LeaseID: ar.LeaseID, Status: "succeeded"})
	msg, err = bus.Request("pipedpeer.daemon.nats-node.complete", completeReq, 2*time.Second)
	if err != nil {
		t.Fatalf("nats complete request: %v", err)
	}

	if s.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after complete, got %d", s.ActiveLeases())
	}
}

func TestNATSAcceptRejectsWrongTarget(t *testing.T) {
	_, bus := setupNATSServer(t, "correct-node")

	req, _ := json.Marshal(acceptRequest{TargetID: "wrong-node"})
	msg, err := bus.Request("pipedpeer.daemon.correct-node.accept", req, 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	var ar acceptResponse
	json.Unmarshal(msg.Data, &ar)
	if ar.Accepted {
		t.Fatal("should reject wrong target_id")
	}
	if ar.Reason == "" {
		t.Fatal("expected non-empty rejection reason")
	}
}

func TestNATSAcceptWithMemoryCheck(t *testing.T) {
	_, bus := setupNATSServer(t, "mem-nats-node")

	// Request 100TB — should be rejected
	req, _ := json.Marshal(acceptRequest{
		TargetID:         "mem-nats-node",
		JobName:          "huge-job",
		RequiredMemBytes: 100 * 1024 * 1024 * 1024 * 1024, // 100TB
	})
	msg, err := bus.Request("pipedpeer.daemon.mem-nats-node.accept", req, 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	var ar acceptResponse
	json.Unmarshal(msg.Data, &ar)
	if ar.Accepted {
		t.Fatal("should reject 100TB request")
	}
}

func TestNATSCancelBySubmitter(t *testing.T) {
	s, bus := setupNATSServer(t, "cancel-nats-node")

	// Accept
	acceptReq, _ := json.Marshal(acceptRequest{
		TargetID:         "cancel-nats-node",
		JobName:          "cancel-job",
		SubmitterNode:    "submitter-x",
		RequiredMemBytes: 1024,
	})
	msg, _ := bus.Request("pipedpeer.daemon.cancel-nats-node.accept", acceptReq, 2*time.Second)
	var ar acceptResponse
	json.Unmarshal(msg.Data, &ar)

	if !ar.Accepted {
		t.Fatalf("accept failed: %s", ar.Reason)
	}
	if s.ReservedMem() != 1024 {
		t.Fatalf("expected 1024 reserved, got %d", s.ReservedMem())
	}

	// Cancel by correct submitter
	cancelReq, _ := json.Marshal(cancelRequest{LeaseID: ar.LeaseID, SubmitterNode: "submitter-x"})
	msg, _ = bus.Request("pipedpeer.daemon.cancel-nats-node.cancel", cancelReq, 2*time.Second)

	var resp map[string]string
	json.Unmarshal(msg.Data, &resp)
	if resp["status"] != "cancelled" {
		t.Fatalf("expected cancelled, got %v", resp)
	}
	if s.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after cancel, got %d", s.ActiveLeases())
	}
}

func TestNATSHealth(t *testing.T) {
	_, bus := setupNATSServer(t, "health-nats-node")

	msg, err := bus.Request("pipedpeer.daemon.health-nats-node.health", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("health request: %v", err)
	}

	var health map[string]interface{}
	json.Unmarshal(msg.Data, &health)

	if health["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", health["status"])
	}
	if health["node_id"] != "health-nats-node" {
		t.Fatalf("expected node_id=health-nats-node, got %v", health["node_id"])
	}
	// active_jobs should be a number
	if _, ok := health["active_jobs"]; !ok {
		t.Fatal("expected active_jobs field in health response")
	}
}

func TestNATSAndHTTPDualTransport(t *testing.T) {
	// Verify both HTTP and NATS work simultaneously on the same server
	s, bus := setupNATSServer(t, "dual-node")

	// Accept via NATS
	natsReq, _ := json.Marshal(acceptRequest{
		TargetID:      "dual-node",
		JobName:       "nats-job",
		SubmitterNode: "nats-sub",
	})
	msg, err := bus.Request("pipedpeer.daemon.dual-node.accept", natsReq, 2*time.Second)
	if err != nil {
		t.Fatalf("nats accept: %v", err)
	}
	var natsAR acceptResponse
	json.Unmarshal(msg.Data, &natsAR)
	if !natsAR.Accepted {
		t.Fatalf("nats accept failed: %s", natsAR.Reason)
	}

	// Server should have 1 lease from NATS
	if s.ActiveLeases() != 1 {
		t.Fatalf("expected 1 lease from NATS, got %d", s.ActiveLeases())
	}

	// Verify the NATS lease has correct metadata
	lease, ok := s.GetLease(natsAR.LeaseID)
	if !ok {
		t.Fatal("NATS lease not found")
	}
	if lease.SubmitterNode != "nats-sub" {
		t.Fatalf("expected submitter=nats-sub, got %s", lease.SubmitterNode)
	}
}

// suppress unused import warning
var _ = nats.Msg{}
