package daemonapi

// Pool speed stats kept across runs (U29 implements; U18 calls from pool.go).
// Keyed by machine key and function fingerprint, never by address: an
// internet peer's address is a forwarder port that changes. Stubs keep
// today's behaviour (no prior, nothing stored) until U29 lands.

// poolPrior returns what earlier runs measured for fn on machine.
func (s *Server) poolPrior(machine, fn string) (itemsPerSec, overheadSec float64, ok bool) {
	return 0, 0, false
}

// poolObserve records one completed warm part (the first part per node and
// function is cold and must not be passed in).
func (s *Server) poolObserve(machine, fn string, items int, computeSec, elapsedSec float64) {}
