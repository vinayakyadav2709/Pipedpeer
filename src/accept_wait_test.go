package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// A memory refusal clears once the previous job has handed its memory back,
// so admission is asked again; any other refusal is final at once.
func TestAcceptWaitsOutMemoryButNothingElse(t *testing.T) {
	calls := 0
	resp, err := acceptWithin(context.Background(), 5*time.Second, func() (*daemonctl.AcceptResponse, error) {
		calls++
		if calls < 2 {
			return nil, errors.New("job rejected by remote daemon: insufficient memory: need 2, available 1")
		}
		return &daemonctl.AcceptResponse{Accepted: true, LeaseID: "l"}, nil
	})
	if err != nil || resp == nil || calls != 2 {
		t.Fatalf("resp=%v err=%v calls=%d, want accepted on the second ask", resp, err, calls)
	}

	calls = 0
	_, err = acceptWithin(context.Background(), 5*time.Second, func() (*daemonctl.AcceptResponse, error) {
		calls++
		return nil, errors.New("job rejected by remote daemon: no GPU available")
	})
	if err == nil || calls != 1 {
		t.Fatalf("err=%v calls=%d, want one ask and the refusal", err, calls)
	}

	calls = 0
	_, err = acceptWithin(context.Background(), 0, func() (*daemonctl.AcceptResponse, error) {
		calls++
		return nil, errors.New("insufficient memory")
	})
	if err == nil || calls != 1 {
		t.Fatalf("a zero window asked %d times", calls)
	}
}
