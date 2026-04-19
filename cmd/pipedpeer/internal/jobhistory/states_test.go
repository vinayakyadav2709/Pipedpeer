package jobhistory

import (
	"testing"
)

func TestValidTransitions(t *testing.T) {
	tests := []struct {
		from, to string
		valid    bool
	}{
		// queued transitions
		{StateQueued, StateReserved, true},
		{StateQueued, StateCancelled, true},
		{StateQueued, StateRunning, false},
		{StateQueued, StateSucceeded, false},
		// reserved transitions
		{StateReserved, StateRunning, true},
		{StateReserved, StateExpired, true},
		{StateReserved, StateCancelled, true},
		{StateReserved, StateSucceeded, false},
		{StateReserved, StateQueued, false},
		// running transitions
		{StateRunning, StateSucceeded, true},
		{StateRunning, StateFailed, true},
		{StateRunning, StateCancelled, true},
		{StateRunning, StateQueued, false},
		{StateRunning, StateReserved, false},
		// terminal states — no transitions out
		{StateSucceeded, StateRunning, false},
		{StateFailed, StateRunning, false},
		{StateCancelled, StateRunning, false},
		{StateExpired, StateRunning, false},
	}

	for _, tt := range tests {
		err := ValidateTransition(tt.from, tt.to)
		if tt.valid && err != nil {
			t.Errorf("%s → %s should be valid, got error: %v", tt.from, tt.to, err)
		}
		if !tt.valid && err == nil {
			t.Errorf("%s → %s should be invalid, but no error", tt.from, tt.to)
		}
	}
}

func TestRecordTransition(t *testing.T) {
	r := Record{Status: StateQueued}

	// queued → reserved
	if err := r.Transition(StateReserved); err != nil {
		t.Fatalf("queued→reserved failed: %v", err)
	}
	if r.Status != StateReserved {
		t.Fatalf("expected reserved, got %s", r.Status)
	}

	// reserved → running
	if err := r.Transition(StateRunning); err != nil {
		t.Fatalf("reserved→running failed: %v", err)
	}
	if r.Status != StateRunning {
		t.Fatalf("expected running, got %s", r.Status)
	}

	// running → succeeded
	if err := r.Transition(StateSucceeded); err != nil {
		t.Fatalf("running→succeeded failed: %v", err)
	}
	if r.Status != StateSucceeded {
		t.Fatalf("expected succeeded, got %s", r.Status)
	}

	// succeeded → anything should fail (terminal state)
	if err := r.Transition(StateRunning); err == nil {
		t.Fatal("succeeded→running should fail")
	}
}

func TestIsTerminal(t *testing.T) {
	terminals := []string{StateSucceeded, StateFailed, StateCancelled, StateExpired}
	nonTerminals := []string{StateQueued, StateReserved, StateRunning}

	for _, s := range terminals {
		if !IsTerminal(s) {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range nonTerminals {
		if IsTerminal(s) {
			t.Errorf("%s should NOT be terminal", s)
		}
	}
}

func TestInvalidFromState(t *testing.T) {
	err := ValidateTransition("bogus", StateRunning)
	if err == nil {
		t.Fatal("unknown from-state should fail")
	}
}
