package jobhistory

import "fmt"

// Task lifecycle states per blueprint §7
const (
	StateQueued    = "queued"
	StateReserved  = "reserved"
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
	StateExpired   = "expired"
)

// AllStates lists every valid state.
var AllStates = []string{
	StateQueued, StateReserved, StateRunning,
	StateSucceeded, StateFailed, StateCancelled, StateExpired,
}

// validTransitions maps each state to the set of states it can move to.
var validTransitions = map[string]map[string]bool{
	StateQueued: {
		StateReserved:  true,
		StateCancelled: true,
	},
	StateReserved: {
		StateRunning:   true,
		StateExpired:   true,
		StateCancelled: true,
	},
	StateRunning: {
		StateSucceeded: true,
		StateFailed:    true,
		StateCancelled: true,
	},
	// Terminal states — no transitions out
	StateSucceeded: {},
	StateFailed:    {},
	StateCancelled: {},
	StateExpired:   {},
}

// IsTerminal returns true if the state is a final state (no further transitions).
func IsTerminal(state string) bool {
	return state == StateSucceeded || state == StateFailed ||
		state == StateCancelled || state == StateExpired
}

// ValidateTransition checks whether moving from `from` to `to` is allowed.
// Returns nil if valid, error if not.
func ValidateTransition(from, to string) error {
	targets, ok := validTransitions[from]
	if !ok {
		return fmt.Errorf("unknown state: %q", from)
	}
	if !targets[to] {
		return fmt.Errorf("invalid transition: %s → %s", from, to)
	}
	return nil
}

// Transition attempts to move a Record from its current state to `to`.
// Returns an error if the transition is invalid.
func (r *Record) Transition(to string) error {
	if err := ValidateTransition(r.Status, to); err != nil {
		return err
	}
	r.Status = to
	return nil
}
