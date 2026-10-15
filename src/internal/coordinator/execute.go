package coordinator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// ExecutorFunc is a function that executes a task on a placed node.
// It receives the daemon host, port, and target node ID.
// Returns nil on success, error on failure (triggering reschedule).
type ExecutorFunc func(host string, port int, targetNodeID string) error

// StatusFunc is called with human-readable status updates.
type StatusFunc func(msg string)

// ExecuteWithRetry is the full task lifecycle:
//
//	place → commit → execute → succeed or fail → reschedule if failed.
//
// A failed execution completes the lease as "failed" and retries placement on
// a node not yet tried where there is one, at most MaxExecAttempts times in
// all; then it returns the last error. A failure that would follow the task
// anywhere (Permanent) returns at once. A node that refused the closure is
// left out from then on without counting as an attempt; the run fails only
// when no node is left. A failed commit (resources changed) retries placement
// immediately. ctx cancellation (user ^C) stops the loop.
func (c *Coordinator) ExecuteWithRetry(ctx context.Context, executor ExecutorFunc, statusFn StatusFunc) (*LeaseResult, error) {
	cfg := c.configSnapshot()
	failures := 0
	var failedOn []string

	for attempt := 0; ; attempt++ {
		// Check for cancellation before each attempt
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
		default:
		}

		if attempt > 0 && statusFn != nil {
			statusFn(fmt.Sprintf("Rescheduling (attempt %d)...", attempt+1))
		}

		// 1. Place: find a node and get a lease (queues if none available)
		result, err := c.PlaceWithRetry(ctx, cfg, statusFn)
		if err != nil {
			return nil, err // context cancelled
		}

		daemonURL := fmt.Sprintf("http://%s:%d", extractHost(result.Endpoint), result.DaemonPort)

		if statusFn != nil {
			statusFn(fmt.Sprintf("Placed on node %s (lease=%s)", result.NodeID[:min(8, len(result.NodeID))], result.LeaseID[:min(8, len(result.LeaseID))]))
		}
		c.notifyPlacement(result)

		// 2. Commit the lease
		if err := c.CommitLease(daemonURL, result.LeaseID, result.NodeID); err != nil {
			if statusFn != nil {
				statusFn(fmt.Sprintf("Commit failed on %s: %v — retrying placement", result.NodeID[:min(8, len(result.NodeID))], err))
			}
			// Commit failed (resources changed since accept) — retry placement
			continue
		}

		if statusFn != nil {
			statusFn(fmt.Sprintf("Lease committed — executing on %s", result.NodeID[:min(8, len(result.NodeID))]))
		}

		// 3. Execute, renewing the lease throughout so the node can tell this
		//    submitter is still alive.
		stopRenew := daemonctl.KeepLeaseAlive(extractHost(result.Endpoint), result.DaemonPort, result.LeaseID)
		execErr := executor(extractHost(result.Endpoint), result.DaemonPort, result.NodeID)
		stopRenew()

		// 4. Complete the lease
		if execErr == nil {
			_ = c.CompleteLease(daemonURL, result.LeaseID, "succeeded", result.NodeID)
			return result, nil // success!
		}

		// Detached: the job keeps running on the node, which releases the
		// lease when the job ends. No retry.
		if errors.Is(execErr, daemonctl.ErrDetached) {
			return result, execErr
		}

		// Cancelled by the caller: no "failed" lease, no backoff, no retry.
		if ctx.Err() != nil {
			_ = c.CompleteLease(daemonURL, result.LeaseID, "cancelled", result.NodeID)
			return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
		}

		// Execution failed — complete as failed and reschedule
		_ = c.CompleteLease(daemonURL, result.LeaseID, "failed", result.NodeID)
		name := placedName(result)

		// A node that refused the closure refuses it on every retry, but
		// another node may take it: leave this one out and place again.
		if ClosureRefused(execErr) {
			fix := fmt.Sprintf("refused this job's closure (%v); fix: run `pipedpeer trust` on %s", execErr, name)
			c.mu.Lock()
			if c.refused == nil {
				c.refused = map[string]string{}
			}
			c.refused[result.NodeID] = fix
			c.mu.Unlock()
			if statusFn != nil {
				statusFn(fmt.Sprintf("%s %s — placing the job on another node", name, fix))
			}
			continue
		}

		// Unless the failure would follow the task to any node.
		var pe *permanentError
		if errors.As(execErr, &pe) {
			return nil, pe.err
		}

		c.mu.Lock()
		if c.tried == nil {
			c.tried = map[string]int{}
		}
		c.tried[result.NodeID]++
		c.mu.Unlock()
		failedOn = append(failedOn, name)
		failures++
		if failures >= MaxExecAttempts {
			return nil, fmt.Errorf("gave up after %d failed attempts (on %s); last error: %w",
				failures, strings.Join(failedOn, ", "), execErr)
		}

		// Back off before trying again. Some failures are not the node's fault
		// (a full disk, a script that always crashes) and retrying flat out
		// makes them worse: each attempt re-does the closure build and export.
		delay := execRetryBackoff(c.execBackoffBase, failures)

		if statusFn != nil {
			statusFn(fmt.Sprintf("Execution failed on %s: %v — retrying in %s",
				result.NodeID[:min(8, len(result.NodeID))], execErr, delay))
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
		case <-time.After(delay):
		}

		// Loop back to placement
	}
}

// MaxExecAttempts is how many failed executions ExecuteWithRetry takes before
// it gives up with the last error.
const MaxExecAttempts = 3

// ClosureRefused reports whether err is a node refusing the job's closure -
// a key it does not trust, or an archive it cannot read. The string covers an
// older node, whose job import says the same in its own words.
func ClosureRefused(err error) bool {
	var ce *daemonctl.ClosureRefusedError
	return errors.As(err, &ce) || (err != nil && strings.Contains(err.Error(), "lacks a signature by a trusted key"))
}

func (c *Coordinator) isRefused(nodeID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.refused[nodeID]
	return ok
}

// placedName names the node a placement chose, the way its owner knows it.
func placedName(r *LeaseResult) string {
	for _, cand := range r.Decision.Candidates {
		if cand.Node.NodeID == r.NodeID {
			return nodeName(cand.Node)
		}
	}
	if r.Decision.ChosenNode.NodeID == r.NodeID {
		return nodeName(r.Decision.ChosenNode)
	}
	return r.NodeID[:min(8, len(r.NodeID))]
}

// permanentError marks an execution failure that rescheduling cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as one that would recur on any node, so ExecuteWithRetry
// returns it instead of placing the task again. A nil err stays nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// DefaultExecBackoffBase is the first wait after a failed execution.
const DefaultExecBackoffBase = 2 * time.Second

// execRetryBackoff grows the wait after each consecutive execution failure, up
// to a 2-minute ceiling, so a stuck task keeps retrying forever without
// hammering the cluster.
func execRetryBackoff(base time.Duration, consecutiveFailures int) time.Duration {
	const max = 2 * time.Minute
	if base <= 0 {
		base = DefaultExecBackoffBase
	}
	d := base
	for i := 1; i < consecutiveFailures && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}
