package daemonapi

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestPersistUnderConcurrentUploads.
//
// persist used to hand the live jobs and leases maps to the marshaller, so
// JSON iterated one while another request wrote to it. Go does not report
// that as a race to be found later - it kills the process immediately with
// "concurrent map iteration and map write", and it killed the daemon
// mid-training-run.
//
// Two uploads arriving at one daemon at the same moment is all it takes.
// That never happened while each node hosted a single rank; giving a node one
// rank per accelerator made it ordinary.
//
// Run with -race for the strongest version, but this fails without it too:
// the fatal map error is not suppressible.
func TestPersistUnderConcurrentUploads(t *testing.T) {
	s := New("self")
	s.state = &state{path: filepath.Join(t.TempDir(), "state.json")}
	s.jobs = map[string]*JobRecord{}
	s.leases = map[string]*Lease{}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: new jobs and leases arriving, as uploads and reservations do.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := string(rune('a'+w)) + string(rune('0'+i%10))
				s.jobsMu.Lock()
				s.jobs[id] = &JobRecord{
					JobID: id, Status: "uploaded", CreatedAt: time.Now(),
					Uploaded: map[string]FileStamp{"f": {Size: 1}},
				}
				s.jobsMu.Unlock()

				s.mu.Lock()
				s.leases[id] = &Lease{LeaseID: id, State: LeaseReserved}
				s.mu.Unlock()
			}
		}(w)
	}

	// Persisters: what every upload does once it has recorded its job.
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s.persist()
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestSweepExpiredPersistsWithoutDeadlocking. persist takes the lease lock to
// copy the map; a caller still holding it would wait for itself forever, and
// the daemon would stop answering with no error anywhere.
func TestSweepExpiredPersistsWithoutDeadlocking(t *testing.T) {
	s := New("self")
	s.state = &state{path: filepath.Join(t.TempDir(), "state.json")}
	s.leases = map[string]*Lease{
		"old": {LeaseID: "old", State: LeaseReserved, ExpiresAt: time.Now().Add(-time.Hour)},
	}
	s.jobs = map[string]*JobRecord{}

	done := make(chan struct{})
	go func() {
		s.sweepExpired()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sweepExpired did not return: it is holding the lease lock " +
			"that persist needs to copy the map")
	}
}
