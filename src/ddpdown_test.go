package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// exited is the error a rank's app.RunTask returns when its script exited
// with code.
func exited(code int) error {
	return fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: code})
}

// TestARankDyingMidRunDoesNotFailTheRing: rank 1 fails while the others are
// still training; the run carries on, says which rank and where, tells the
// lead daemon once, and succeeds when the survivors finish.
func TestARankDyingMidRunDoesNotFailTheRing(t *testing.T) {
	results := make(chan ddpRankResult, 3)
	var downs []int
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- ddpAwaitRanks(context.Background(), &out, results,
			[]string{"laptop-a", "pc-2", "laptop-b"}, nil, nil,
			func(rank int, err error) { downs = append(downs, rank) })
	}()

	results <- ddpRankResult{rank: 1, err: errors.New("connection reset")}
	time.Sleep(20 * time.Millisecond) // survivors still running
	results <- ddpRankResult{rank: 0}
	results <- ddpRankResult{rank: 2}

	if err := <-done; err != nil {
		t.Fatalf("the ring failed with %v; two of three ranks finished", err)
	}
	if !strings.Contains(out.String(), "rank 1 on pc-2 died; continuing on 2") {
		t.Errorf("no message naming the dead rank and its host:\n%s", out.String())
	}
	if len(downs) != 1 || downs[0] != 1 {
		t.Errorf("lead daemon told about ranks %v, want [1]", downs)
	}
}

// TestEveryRankFailingFailsTheRun, naming the right rank rather than the
// order results arrived in.
func TestEveryRankFailingFailsTheRun(t *testing.T) {
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 1, err: errors.New("ImportError")}
	results <- ddpRankResult{rank: 0, err: errors.New("ImportError")}
	var out bytes.Buffer
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, nil, func(int, error) {})
	if err == nil || !strings.Contains(err.Error(), "rank 1 on b") {
		t.Fatalf("got %v, want the first failure, rank 1 on b", err)
	}
}

// TestCancelIsNotADeath: Ctrl+C stops every rank, and none of them should be
// reported down or the run called a success.
func TestCancelIsNotADeath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 0, err: context.Canceled}
	results <- ddpRankResult{rank: 1}
	called := false
	err := ddpAwaitRanks(ctx, &bytes.Buffer{}, results, []string{"a", "b"}, nil, nil, func(int, error) { called = true })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want the cancel", err)
	}
	if called {
		t.Error("a cancelled rank was reported down")
	}
}

// TestAScriptFailureOnOneRankFailsTheRun: rank 0 crashing at checkpoint save
// is the script failing, not a lost machine. Every other rank is stopped at
// once (training on is wasted work) and the run fails with the script's error.
func TestAScriptFailureOnOneRankFailsTheRun(t *testing.T) {
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 0, err: exited(1)}
	var out bytes.Buffer
	var downs []int
	stopped := false
	stop := []context.CancelFunc{nil, func() { stopped = true; results <- ddpRankResult{rank: 1, err: context.Canceled} }}
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, stop, nil,
		func(rank int, err error) { downs = append(downs, rank) })
	if err == nil || !strings.Contains(err.Error(), "rank 0 on a: the script failed") {
		t.Fatalf("got %v, want a failure naming rank 0 on a", err)
	}
	if ddpExitCode(err) != 1 {
		t.Errorf("exit status %d travels with the error, want the script's 1", ddpExitCode(err))
	}
	if !stopped {
		t.Error("rank 1 was left training after rank 0's script failed")
	}
	if len(downs) != 0 {
		t.Errorf("lead daemon told about %v; the ring is being stopped, not continued", downs)
	}
	// A rank killed by a signal (machine stopping), or a node that could not
	// start the script (126/127), is a loss, not a failure.
	for _, code := range []int{137, 126, 127} {
		results <- ddpRankResult{rank: 1, err: exited(code)}
		results <- ddpRankResult{rank: 0}
		if err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, nil, func(int, error) {}); err != nil {
			t.Errorf("a rank that exited %d failed the run: %v", code, err)
		}
	}
}

// TestATrainingStopStopsEveryRank: exit 76 is the lead aborting the ring (the
// shim printed why). Every rank is stopped and the run fails without taking
// 76 as its own exit status.
func TestATrainingStopStopsEveryRank(t *testing.T) {
	results := make(chan ddpRankResult, 3)
	results <- ddpRankResult{rank: 1, err: exited(ddpStoppedExit)}
	var stopped []int
	stopFor := func(r int) context.CancelFunc {
		return func() { stopped = append(stopped, r); results <- ddpRankResult{rank: r, err: context.Canceled} }
	}
	stop := []context.CancelFunc{stopFor(0), nil, stopFor(2)}
	err := ddpAwaitRanks(context.Background(), &bytes.Buffer{}, results, []string{"a", "b", "c"}, stop, nil, func(int, error) {})
	if err == nil || !strings.Contains(err.Error(), "training stopped") {
		t.Fatalf("got %v, want the training-stopped failure", err)
	}
	if ddpExitCode(err) == ddpStoppedExit {
		t.Error("76 leaked into the run's exit status")
	}
	if len(stopped) != 2 {
		t.Errorf("stopped ranks %v, want 0 and 2", stopped)
	}
}

// TestAReleasedRankIsNotAFailure: exit 77 is the ring shrinking to its
// fastest rank; the run succeeds when that rank finishes, and the release
// does not start the "ring finished" grace on the rank still training.
func TestAReleasedRankIsNotAFailure(t *testing.T) {
	old := ddpJoinGrace
	ddpJoinGrace = 10 * time.Millisecond
	t.Cleanup(func() { ddpJoinGrace = old })
	results := make(chan ddpRankResult)
	stop := []context.CancelFunc{func() { t.Error("the training rank was stopped after the other was released") }, nil}
	var downs []int
	done := make(chan error, 1)
	go func() {
		done <- ddpAwaitRanks(context.Background(), &bytes.Buffer{}, results, []string{"a", "b"}, stop, nil,
			func(rank int, err error) { downs = append(downs, rank) })
	}()
	results <- ddpRankResult{rank: 1, err: exited(ddpReleasedExit)}
	time.Sleep(50 * time.Millisecond) // past the grace, were it running
	results <- ddpRankResult{rank: 0}
	if err := <-done; err != nil {
		t.Fatalf("a released rank failed the run: %v", err)
	}
	if len(downs) != 0 {
		t.Errorf("a released rank was reported down: %v", downs)
	}
}

// TestAJoinersCrashDoesNotFailTheRun: a rank added mid-run that exits 1 while
// joining (the lead refused it, a shape mismatch) never touched the training;
// the ranks that trained finish and the run succeeds.
func TestAJoinersCrashDoesNotFailTheRun(t *testing.T) {
	results := make(chan ddpRankResult)
	added := make(chan ddpAdded)
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, added, func(int, error) {})
	}()
	added <- ddpAdded{rank: 2, host: "pc-2", cancel: func() {}}
	results <- ddpRankResult{rank: 2, err: exited(1)}
	results <- ddpRankResult{rank: 0}
	results <- ddpRankResult{rank: 1}
	if err := <-done; err != nil {
		t.Fatalf("a joiner's crash failed a run that trained fine: %v", err)
	}
	if !strings.Contains(out.String(), "rank 2 on pc-2") {
		t.Errorf("the joiner's failure was not reported:\n%s", out.String())
	}
}

// TestNoRankJoinsAfterTheScriptFailed: once a rank's script has failed the
// run is lost, so a rank the grower starts afterwards is stopped at once
// rather than run on (possibly the same failing node).
func TestNoRankJoinsAfterTheScriptFailed(t *testing.T) {
	results := make(chan ddpRankResult)
	added := make(chan ddpAdded)
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, added, func(int, error) {})
	}()
	results <- ddpRankResult{rank: 0, err: exited(1)}
	cancelled := false
	added <- ddpAdded{rank: 2, host: "a", cancel: func() { cancelled = true }}
	results <- ddpRankResult{rank: 2, err: context.Canceled}
	results <- ddpRankResult{rank: 1}
	err := <-done
	if !cancelled {
		t.Error("a rank started after the script failed was left running")
	}
	if err == nil || !strings.Contains(err.Error(), "rank 0 on a: the script failed") {
		t.Fatalf("got %v, want rank 0's script failure", err)
	}
}

// TestARerunUnderTheSameNameGetsAFreshGroup: the lead daemon remembers a
// group's dropped ranks for ten minutes, so a re-run must not reuse it.
func TestARerunUnderTheSameNameGetsAFreshGroup(t *testing.T) {
	now := time.Now()
	a, b := ddpGroupName("train", now), ddpGroupName("train", now.Add(time.Second))
	if a == b || !strings.HasPrefix(a, "ddp-train-") {
		t.Errorf("groups %q and %q: want distinct, prefixed ddp-train-", a, b)
	}
}

// TestTheLeadDyingEndsTheRun: rank 0's machine hosts the sync, so the ring
// cannot carry on without it. Say so instead of "continuing on 1", stop the
// other ranks now, don't post to the dead lead, and fail the run.
func TestTheLeadDyingEndsTheRun(t *testing.T) {
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 0, err: errors.New("connection lost")}
	var out bytes.Buffer
	called, stopped := false, false
	stop := []context.CancelFunc{nil, func() { stopped = true; results <- ddpRankResult{rank: 1, err: context.Canceled} }}
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"gpu1", "gpu2"}, stop, nil,
		func(int, error) { called = true })
	if err == nil || !strings.Contains(err.Error(), "leads the ring") {
		t.Fatalf("got %v, want a failure saying the lead's machine died", err)
	}
	if strings.Contains(out.String(), "continuing") || !strings.Contains(out.String(), "rank 0 on gpu1 died") {
		t.Errorf("wrong message for a dead lead:\n%s", out.String())
	}
	if !stopped {
		t.Error("rank 1 was left running after the lead died")
	}
	if called {
		t.Error("the dead lead daemon was told a rank is down")
	}
}

// TestALostLeadMachineFailsFast is Finding 5: a Wi-Fi blip killed the lead's
// output stream without a FIN, the submitter's read never returned, and the
// run sat on its last line while no node had jobs. The rank's machine going
// silent now cancels that rank within ddpNodeLostAfter, which stops the ring
// with a message naming the machine.
func TestALostLeadMachineFailsFast(t *testing.T) {
	oldEvery, oldAfter, oldUp := ddpNodeProbeEvery, ddpNodeLostAfter, ddpNetworkUp
	ddpNodeProbeEvery, ddpNodeLostAfter = 10*time.Millisecond, 60*time.Millisecond
	ddpNetworkUp = func(int, string) (string, bool) { return "peer gpu2 answers", true }
	t.Cleanup(func() { ddpNodeProbeEvery, ddpNodeLostAfter, ddpNetworkUp = oldEvery, oldAfter, oldUp })

	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	host, portStr, _ := net.SplitHostPort(strings.TrimPrefix(node.URL, "http://"))
	port, _ := strconv.Atoi(portStr)

	results := make(chan ddpRankResult)
	// A rank's stream that died silently: it returns only when cancelled.
	deadStream := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("execution failed: %w", ctx.Err())
	}
	rank1Ctx, stopRank1 := context.WithCancel(context.Background())
	go func() {
		results <- ddpRankResult{rank: 0, err: ddpRunWatched(context.Background(), host, port, 0, deadStream)}
	}()
	go func() { results <- ddpRankResult{rank: 1, err: deadStream(rank1Ctx)} }()

	time.Sleep(30 * time.Millisecond)
	node.Close() // the lead's machine drops off the network

	start := time.Now()
	err := ddpAwaitRanks(context.Background(), &bytes.Buffer{}, results, []string{"gpu1", "gpu2"},
		[]context.CancelFunc{nil, stopRank1}, nil, func(int, error) {})
	if time.Since(start) > 5*time.Second {
		t.Errorf("took %s to notice the lost lead", time.Since(start))
	}
	if err == nil || !strings.Contains(err.Error(), "lost the connection to "+host) || !strings.Contains(err.Error(), "leads the ring") {
		t.Fatalf("got %v, want a lost-connection failure naming the lead", err)
	}
	if rank1Ctx.Err() == nil {
		t.Error("rank 1 was left running after its lead was lost")
	}
}

// TestARankThatMissedTheStartIsALossNotAFailure: the lead turned rank 1 away
// (exit 75) while rank 0 trained; the run succeeds and says why. A rank still
// running a grace after another finished (its closure still arriving) is
// stopped instead of holding the run open.
func TestARankThatMissedTheStartIsALossNotAFailure(t *testing.T) {
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 1, err: exited(ddpMissedStartExit)}
	results <- ddpRankResult{rank: 0}
	var out bytes.Buffer
	if err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, nil, func(int, error) {}); err != nil {
		t.Fatalf("a rank that missed the start failed the run: %v", err)
	}
	if !strings.Contains(out.String(), "rank 1 on b was not ready when training started") {
		t.Errorf("no plain message for the late rank:\n%s", out.String())
	}

	old := ddpJoinGrace
	ddpJoinGrace = 50 * time.Millisecond
	t.Cleanup(func() { ddpJoinGrace = old })
	results = make(chan ddpRankResult, 2)
	stopped := make(chan struct{})
	stop := []context.CancelFunc{nil, func() { close(stopped); results <- ddpRankResult{rank: 1, err: context.Canceled} }}
	results <- ddpRankResult{rank: 0}
	out.Reset()
	if err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, stop, nil, func(int, error) {}); err != nil {
		t.Fatalf("stopping a straggler failed the run: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Error("the straggler was never stopped")
	}
	if !strings.Contains(out.String(), "rank 1 on b: the ring finished without it") {
		t.Errorf("no message for the stopped straggler:\n%s", out.String())
	}
}
