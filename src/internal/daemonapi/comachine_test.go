package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
)

// TestAPeerSharingThisMachineIsSubtracted.
//
// Free memory belongs to the machine, not to the daemon. Two daemons on one
// host each read the host's free memory and each subtract only their own
// reservations, so both conclude they have all of it and both admit work that
// between them the machine cannot hold. Measured before this: a forced spill
// drove a 14 GB machine to 194 MB free and the kernel killed its desktop
// shell.
func TestAPeerSharingThisMachineIsSubtracted(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id, so co-location " +
			"cannot be detected here at all")
	}
	s := New("self")

	// The mechanism itself, which holds whatever else bounds this daemon.
	// Asserting on AvailableForJob alone passed here and failed on the test
	// machine: there the daemon's own budget is the binding constraint, so
	// subtracting a roommate's reservation from a larger free figure changed
	// nothing visible - a real weakness in the test, not in the code.
	forgetRoommates(s)
	if got := s.reservedOnThisMachine(); got != 0 {
		t.Fatalf("a daemon with no peers thinks %d bytes are spoken for", got)
	}

	s.peersMu.Lock()
	if s.peerHealths == nil {
		s.peerHealths = map[string]*PeerHealth{}
	}
	s.peerHealths["127.0.0.1:38091"] = &PeerHealth{
		Host: "127.0.0.1", Port: 38091, NodeID: "roommate",
		Status: "healthy", Machine: me, ReservedMem: 4 << 30,
	}
	s.peersMu.Unlock()
	forgetRoommates(s)

	if got := s.reservedOnThisMachine(); got != 4<<30 {
		t.Errorf("a roommate reserving 4 GiB shows as %d bytes here; both "+
			"daemons would admit work the machine cannot hold", got)
	}

}

// TestARoommatesReservationReachesAdmission is the other half, split out
// because it can only be exercised where free memory is the tighter of the
// two bounds. Kept apart so the mechanism above always runs: as one test it
// reported as skipped on the machine where the budget binds, even though its
// main assertion had passed.
func TestARoommatesReservationReachesAdmission(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id")
	}
	load := heartbeat.CollectLoad(0, 0)
	budget := cgroups.SelfBudget()
	if budget.Total > 0 && budget.Remaining() < load.AvailableMemBytes-(4<<30) {
		t.Skip("NOT VERIFIED: this daemon's own budget is the tighter bound " +
			"here, so a roommate's reservation cannot move the figure " +
			"admission uses. TestAPeerSharingThisMachineIsSubtracted covers " +
			"the mechanism itself and runs everywhere.")
	}

	s := New("self")
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{
		"127.0.0.1:38091": {
			Host: "127.0.0.1", Port: 38091, NodeID: "roommate",
			Status: "healthy", Machine: me, ReservedMem: 4 << 30,
		},
	}
	s.peersMu.Unlock()
	forgetRoommates(s)

	if s.AvailableForJob() >= load.AvailableMemBytes {
		t.Errorf("the roommate's 4 GiB never reached what this daemon offers")
	}
}

// TestAPeerOnAnotherMachineIsNotSubtracted. Its memory is its own; charging
// this node for it would shrink every cluster's usable memory.
func TestAPeerOnAnotherMachineIsNotSubtracted(t *testing.T) {
	s := New("self")
	before := s.AvailableForJob()

	s.peersMu.Lock()
	if s.peerHealths == nil {
		// The daemon creates this when it starts polling; these tests do not
		// start a poller, which would go looking for real peers on the LAN.
		s.peerHealths = map[string]*PeerHealth{}
	}
	s.peerHealths["10.0.0.5:38080"] = &PeerHealth{
		Host: "10.0.0.5", Port: 38080, NodeID: "elsewhere",
		Status: "healthy", Machine: "a-different-boot-id", ReservedMem: 8 << 30,
	}
	s.peersMu.Unlock()
	forgetRoommates(s)

	after := s.AvailableForJob()
	// Free memory moves under a running test, so compare with a tolerance
	// well below the 8 GiB the peer reserved.
	if diff := before - after; diff > 1<<30 {
		t.Errorf("a peer on another machine cost this one %d bytes of headroom", diff)
	}
}

// TestUnknownMachineIsNotAssumedToBeThisOne. A peer that advertises nothing
// must not be guessed to be co-located: that would shrink every node's usable
// memory for no reason, on every cluster running a mixed set of versions.
func TestUnknownMachineIsNotAssumedToBeThisOne(t *testing.T) {
	s := New("self")
	before := s.AvailableForJob()

	s.peersMu.Lock()
	if s.peerHealths == nil {
		// The daemon creates this when it starts polling; these tests do not
		// start a poller, which would go looking for real peers on the LAN.
		s.peerHealths = map[string]*PeerHealth{}
	}
	s.peerHealths["10.0.0.6:38080"] = &PeerHealth{
		Host: "10.0.0.6", Port: 38080, NodeID: "older-build",
		Status: "healthy", Machine: "", ReservedMem: 8 << 30,
	}
	s.peersMu.Unlock()

	forgetRoommates(s)
	if diff := before - s.AvailableForJob(); diff > 1<<30 {
		t.Errorf("a peer that said nothing about its machine cost this one %d "+
			"bytes; silence is not agreement", diff)
	}
}

// TestAnUnreachablePeerIsNotHoldingMemory. A daemon that has stopped answering
// is not running anything, and continuing to charge for it would leave the
// machine unable to admit work after a peer died.
func TestAnUnreachablePeerIsNotHoldingMemory(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id")
	}
	s := New("self")
	before := s.AvailableForJob()

	s.peersMu.Lock()
	if s.peerHealths == nil {
		// The daemon creates this when it starts polling; these tests do not
		// start a poller, which would go looking for real peers on the LAN.
		s.peerHealths = map[string]*PeerHealth{}
	}
	s.peerHealths["127.0.0.1:38092"] = &PeerHealth{
		Host: "127.0.0.1", Port: 38092, NodeID: "dead",
		Status: "unreachable", Machine: me, ReservedMem: 8 << 30,
	}
	s.peersMu.Unlock()

	forgetRoommates(s)
	if diff := before - s.AvailableForJob(); diff > 1<<30 {
		t.Errorf("a peer that stopped answering still held %d bytes", diff)
	}
}

// TestSameMachineNeedsBothSidesKnown. Two unknowns are not a match, or every
// node that cannot read a boot id would look co-located with every other.
func TestSameMachineNeedsBothSidesKnown(t *testing.T) {
	if heartbeat.SameMachine("", "") {
		t.Error("two unknown machines were called the same machine")
	}
	if heartbeat.SameMachine("abc", "") || heartbeat.SameMachine("", "abc") {
		t.Error("an unknown machine matched a known one")
	}
	if !heartbeat.SameMachine("abc", "abc") {
		t.Error("a machine did not match itself")
	}
}

// TestTheLiveFigureBeatsTheCachedOne.
//
// The health poller runs every ten seconds or so. A chunk arriving inside that
// window was admitted against a reservation figure from before the reservation
// it needed to see, so both daemons admitted in the same window and the
// machine went over anyway - measured, with the kernel taking a pipedpeer
// worker. A roommate is on the loopback, so it can simply be asked.
func TestTheLiveFigureBeatsTheCachedOne(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id")
	}

	var reserved int64 = 6 << 30
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(healthResponse{
			Status:      "ok",
			ReservedMem: atomic.LoadInt64(&reserved),
		})
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())

	s := New("self")
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{
		"roommate": {
			Host: u.Hostname(), Port: port, NodeID: "roommate",
			Status: "healthy", Machine: me,
			// Stale: what the poller last saw, before the peer reserved.
			ReservedMem: 0,
		},
	}
	s.peersMu.Unlock()

	forgetRoommates(s)
	if got := s.reservedOnThisMachine(); got != 6<<30 {
		t.Errorf("read %d bytes reserved on this machine, want the live 6 GiB "+
			"rather than the poller's stale 0", got)
	}
}

// TestARoommateThatStopsAnsweringKeepsItsLastFigure. Treating silence as
// "reserved nothing" would hand out memory that is still spoken for, which is
// the failure this whole path exists to prevent.
func TestARoommateThatStopsAnsweringKeepsItsLastFigure(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id")
	}
	s := New("self")
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{
		// Port 1 is not listening, so the live question fails.
		"gone": {
			Host: "127.0.0.1", Port: 1, NodeID: "gone",
			Status: "healthy", Machine: me, ReservedMem: 3 << 30,
		},
	}
	s.peersMu.Unlock()

	forgetRoommates(s)
	if got := s.reservedOnThisMachine(); got != 3<<30 {
		t.Errorf("read %d bytes; a roommate that did not answer should keep "+
			"its last known 3 GiB, not drop to nothing", got)
	}
}

// forgetRoommates drops the short-lived answer cache, standing in for the
// poller cycle that would separate two of these checks in a running daemon.
func forgetRoommates(s *Server) {
	s.coResMu.Lock()
	s.coResAt = time.Time{}
	s.coResMu.Unlock()
}

// TestPoolBodyLimitFitsTheDaemonsBudget.
//
// A fixed 2 GiB ceiling is most of a small worker's entire allowance - the
// lab's container beside its host is capped at exactly 2 GiB - so one request
// sized to the constant would consume everything the daemon is allowed before
// a single item had been looked at.
func TestPoolBodyLimitFitsTheDaemonsBudget(t *testing.T) {
	s := New("self")
	limit := s.poolBodyLimit()

	if limit > maxPoolBody {
		t.Errorf("limit %d exceeds the absolute ceiling %d", limit, maxPoolBody)
	}
	if limit <= 0 {
		t.Fatalf("limit is %d, which accepts nothing at all", limit)
	}
	if b := cgroups.SelfBudget(); b.Total > 0 && limit >= b.Total {
		t.Errorf("one request may fill %d bytes of a %d byte budget, leaving "+
			"nothing to process it with", limit, b.Total)
	}
}

// TestReservationsCountAgainstTheBudgetToo.
//
// The free-memory reading has this daemon's reservations subtracted; the
// budget reading did not, so the two used different accounting. A daemon
// whose budget was the tighter bound reported the same headroom however much
// it had already promised, and admitted the same work twice over.
//
// Found by a stacked-reservation test that passed on one machine and failed
// on the other — the difference being which of the two bounds was binding.
func TestReservationsCountAgainstTheBudgetToo(t *testing.T) {
	s := New("self")
	before := s.AvailableForJob()
	if before <= 0 {
		t.Skip("NOT VERIFIED: this daemon can offer nothing to begin with")
	}

	// Promise most of what it says it has.
	chunk := before * 60 / 100
	s.mu.Lock()
	s.leases = map[string]*Lease{
		"a": {LeaseID: "a", State: LeaseRunning, MemBytes: chunk},
	}
	s.mu.Unlock()

	after := s.AvailableForJob()
	if after > before-chunk {
		t.Errorf("after promising %d bytes the daemon still offers %d of its "+
			"original %d; a promise it has already made must come off both "+
			"readings, or it makes the same promise twice", chunk, after, before)
	}
}

// TestLeaseAdmissionHonoursTheSameBoundsAsEverythingElse.
//
// /v1/accept had its own memory arithmetic and used neither the roommate
// subtraction nor the daemon's own budget — so every gap the budget work
// closed was still open on the path every job is actually admitted through. A
// daemon would take leases past its share of the machine and past what a
// co-located daemon had already promised.
//
// Caught by a stacked-reservation test failing on the machine where the
// budget binds and passing on the one where free memory does.
func TestLeaseAdmissionHonoursTheSameBoundsAsEverythingElse(t *testing.T) {
	s := NewWithConfig("bounds-node", 5*time.Second, 2*time.Second, time.Second)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	offered := s.AvailableForJob()
	if offered <= 0 {
		t.Skip("NOT VERIFIED: this daemon offers nothing to begin with")
	}

	// Ask for a shade more than the daemon says it has. Admission must refuse
	// it, whichever bound is the tighter one here.
	body := fmt.Sprintf(`{"target_id":"bounds-node","job_name":"too-big",`+
		`"required_mem_bytes":%d}`, offered+(1<<30))
	resp, err := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var out acceptResponse
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()

	if out.Accepted {
		t.Errorf("a lease for %d bytes was accepted by a daemon offering %d; "+
			"admission is not using the bounds the rest of the daemon reports",
			offered+(1<<30), offered)
	}
}
