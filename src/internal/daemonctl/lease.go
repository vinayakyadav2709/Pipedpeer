package daemonctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/say"
)

// LeaseRenewInterval is well under the node's 5-min running-lease TTL, so a
// few dropped renewals on a saturated link do not cost a run its node.
const LeaseRenewInterval = 30 * time.Second

// errLeaseGone means the node no longer holds the lease.
var errLeaseGone = fmt.Errorf("node no longer holds the lease")

// RenewLease tells the node holding leaseID that its submitter is still at
// work (POST /v1/renew). Until the job starts and binds the lease, nothing
// else keeps it: an env build plus closure send longer than the TTL lost it.
func RenewLease(host string, port int, leaseID string) error {
	body, err := json.Marshal(map[string]string{"lease_id": leaseID})
	if err != nil {
		return err
	}
	// Generous: renewals share the link with the closure upload.
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(fmt.Sprintf("http://%s:%d/v1/renew", host, port), "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return errLeaseGone
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("node answered %s", resp.Status)
	}
	return nil
}

// KeepLeaseAlive renews leaseID every LeaseRenewInterval until stop is
// called. Failed renewals show with -v; the 4th in a row (2 min of the
// node's 5-min TTL gone) is printed whatever the verbosity. A lease the node no longer holds ends the renewals: the job ended and
// released it (normal while results download), or it expired before the job
// started and the node takes it again when the job binds it (logged there).
func KeepLeaseAlive(host string, port int, leaseID string) (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(LeaseRenewInterval)
		defer t.Stop()
		failed := 0
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			err := RenewLease(host, port, leaseID)
			switch {
			case err == nil:
				failed = 0
			case err == errLeaseGone:
				say.Printf("lease %s: %s:%d no longer holds it (job ended, or it expired before the job started); renewals stop\n", leaseID, host, port)
				return
			default:
				failed++
				if failed == 4 {
					say.Warnf("could not renew lease %s on %s:%d for 2 min (%v); the node frees this run's slot after 5 min without one\n", leaseID, host, port, err)
				} else {
					say.Printf("lease %s: renewal %d failed: %v\n", leaseID, failed, err)
				}
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

// ReleaseLease asks a node to free leaseID now (POST /v1/complete). A run
// cancelled before its job started has nothing on the node to end the lease,
// and a GPU node then refused every other run until the TTL ran out.
// Idempotent: the node ignores a lease it no longer holds.
func ReleaseLease(host string, port int, leaseID, status string) error {
	body, err := json.Marshal(map[string]string{"lease_id": leaseID, "status": status})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(fmt.Sprintf("http://%s:%d/v1/complete", host, port), "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("node answered %s", resp.Status)
	}
	return nil
}
