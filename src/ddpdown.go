package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/say"
)

// ddpRankResult is how one rank's job ended.
type ddpRankResult struct {
	rank int
	err  error
}

// Exit statuses a rank's shim uses to tell the CLI how the ring treated it.
const (
	// ddpMissedStartExit: the lead daemon turned the rank away because it
	// missed the start barrier (EX_TEMPFAIL; the shim's _daemon_reduce). The
	// ring is training without it: a loss, not a script failure, and the
	// machine may take a joining rank later.
	ddpMissedStartExit = 75
	// ddpStoppedExit: the lead daemon aborted the ring (mismatched models,
	// say) or the lead stopped answering. The shim has already printed
	// "pipedpeer: training stopped: <reason>" on the rank's stderr.
	ddpStoppedExit = 76
	// ddpReleasedExit: the ring shrank because fewer ranks are faster, and
	// this rank was let go. Not a failure.
	ddpReleasedExit = 77
)

// ddpExitCode is the exit status of a rank's script, or -1 when the rank
// ended some other way.
func ddpExitCode(err error) int {
	var xe *daemonctl.RemoteExitError
	if errors.As(err, &xe) {
		return xe.Code
	}
	return -1
}

// ddpMissedStart reports whether a rank's job ended for missing the start.
func ddpMissedStart(err error) bool { return ddpExitCode(err) == ddpMissedStartExit }

// ddpHostName names a rank's machine the way a person knows it.
func ddpHostName(n registry.NodeRecord) string {
	if h := n.Capabilities["hostname"]; h != "" {
		return h
	}
	if h := ddpExtractHost(n.SSHEndpoint); h != "" {
		return h
	}
	return shortID(n.NodeID)
}

// ddpHostList is the ring's machine names, by rank, comma-separated.
func ddpHostList(ring []registry.NodeRecord) string {
	names := make([]string, len(ring))
	for i, n := range ring {
		names[i] = strings.ReplaceAll(ddpHostName(n), ",", " ")
	}
	return strings.Join(names, ",")
}

// ddpAwaitRanks waits for every rank, including ranks started mid-run
// (announced on added). A rank whose machine is lost while others still run
// is reported through onDown and the ring carries on without it.
//
// Three things end the run at once, stopping every rank still going (stop[]
// and the joiners): a rank's script exiting non-zero (a crash at checkpoint
// save is a failed run, not a lost machine, and the ranks left would only
// train on to a result nobody keeps), a rank exiting ddpStoppedExit (the lead
// aborted the ring), and rank 0's machine being lost (the sync lives there).
// The run also fails when no rank finishes, and on cancel. A rank exiting
// ddpReleasedExit was let go by a shrinking ring and is neither.
//
// A rank added mid-run never fails the run: until it joins it has trained
// nothing, and a crash while joining (refused, shape mismatch) is the join
// failing. Once the run has failed, a rank announced on added is stopped at
// once: the grower may have picked the very node that failed.
//
// A rank still waiting to join when only joiners are left is stopped after
// ddpJoinGrace: the ring it was joining has finished. So is any rank still
// running ddpJoinGrace after one rank finished (stop[rank] cancels a starting
// rank): ranks train in lockstep, so one finishing means the ring is done, and
// a rank still going is one that missed the start barrier and is still
// receiving its closure (V3 53: 3.8 GB over a relay).
func ddpAwaitRanks(ctx context.Context, w io.Writer, results <-chan ddpRankResult, hosts []string, stop []context.CancelFunc, added <-chan ddpAdded, onDown func(rank int, err error)) error {
	hosts = append([]string(nil), hosts...)
	running := len(hosts)
	ended := map[int]bool{}
	finished := false
	joiners := map[int]context.CancelFunc{}
	var grace <-chan time.Time
	stopped := false
	var firstErr error
	var died []int
	// fatal is why the run failed while ranks were still going; set once,
	// by abort, which stops them.
	var fatal error
	fatalFirst := false // fatal is the run's first failure
	var others []string // ranks that failed after firstErr's
	refused := map[int]bool{}
	ok := 0
	abort := func(why error) {
		if fatal != nil {
			return
		}
		fatal, stopped, grace = why, true, nil
		fatalFirst = len(others) == 0
		if running > 0 {
			fmt.Fprintf(w, "[ddp] the run has failed; stopping the other ranks\n")
		}
		for _, cancel := range joiners {
			cancel()
		}
		for rank, cancel := range stop {
			if cancel != nil && !ended[rank] {
				cancel()
			}
		}
	}
	for running > 0 {
		if grace == nil && !stopped && (finished || len(joiners) > 0 && running == len(joiners)) {
			grace = time.After(ddpJoinGrace)
		}
		select {
		case a := <-added:
			for len(hosts) <= a.rank {
				hosts = append(hosts, "?")
			}
			hosts[a.rank] = a.host
			running++
			if fatal != nil {
				fmt.Fprintf(w, "[ddp] rank %d on %s: not joining, the run has already failed\n", a.rank, a.host)
				refused[a.rank] = true
				a.cancel()
				continue
			}
			joiners[a.rank] = a.cancel
			continue
		case <-grace:
			grace, stopped = nil, true
			for rank, cancel := range joiners {
				fmt.Fprintf(w, "[ddp] rank %d on %s: the ring finished before it could join; stopping it\n", rank, hosts[rank])
				cancel()
			}
			for rank, cancel := range stop {
				if cancel != nil && !ended[rank] {
					fmt.Fprintf(w, "[ddp] rank %d on %s: the ring finished without it; stopping it\n", rank, hosts[rank])
					cancel()
				}
			}
			continue
		case r := <-results:
			running--
			ended[r.rank] = true
			_, joiner := joiners[r.rank]
			delete(joiners, r.rank)
			host := "?"
			if r.rank >= 0 && r.rank < len(hosts) {
				host = hosts[r.rank]
			}
			if r.err == nil {
				ok++
				finished = true
				continue
			}
			code := ddpExitCode(r.err)
			if code == ddpReleasedExit && ctx.Err() == nil {
				// Rank 0's output already says the ring shrank and why.
				say.Printf("[ddp] rank %d on %s released: the ring is faster without it\n", r.rank, host)
				continue
			}
			if fatal != nil || (refused[r.rank] || stopped) && errors.Is(r.err, context.Canceled) && ctx.Err() == nil {
				continue // being stopped; how it went no longer matters
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("rank %d on %s failed: %w", r.rank, host, r.err)
			} else {
				others = append(others, fmt.Sprintf("rank %d on %s", r.rank, host))
			}
			if ctx.Err() != nil {
				continue // a cancel stops every rank; nobody died
			}
			died = append(died, r.rank)
			missed := !joiner && code == ddpMissedStartExit
			if missed {
				// The lead already dropped it and said so in rank 0's output;
				// this is that rank's own process getting there late.
				fmt.Fprintf(w, "[ddp] rank %d on %s was not ready when training started, so the ring went on without it\n", r.rank, host)
				if running > 0 {
					onDown(r.rank, r.err)
				}
				continue
			}
			// A joiner failing is its join failing: the ring trains on.
			if !joiner {
				switch {
				case code == ddpStoppedExit:
					// Not wrapped: 76 is the shim's verdict, not the
					// script's exit status, and must not become this
					// command's.
					abort(fmt.Errorf("rank %d on %s: training stopped (the reason is printed above)", r.rank, host))
					continue
				case app.IsPermanent(r.err):
					abort(fmt.Errorf("rank %d on %s: the script failed: %w", r.rank, host, r.err))
					continue
				case r.rank == 0:
					// Rank 0's machine is the ring's lead: the daemon-backed
					// sync lives there, so the ranks left have nobody to
					// sync with, and telling the lead daemon a rank is down
					// cannot reach a dead lead.
					fmt.Fprintf(w, "[ddp] rank 0 on %s died: its machine leads the ring, so the run can't continue without it\n", host)
					fmt.Fprintf(w, "[ddp]   cause: %v\n", r.err)
					abort(fmt.Errorf("rank 0 on %s died: its machine leads the ring, and the run can't continue without it: %w", host, r.err))
					continue
				}
			}
			if running > 0 {
				fmt.Fprintf(w, "[ddp] rank %d on %s died; continuing on %d\n", r.rank, host, running)
				fmt.Fprintf(w, "[ddp]   cause: %v\n", r.err)
				onDown(r.rank, r.err)
			}
		}
	}
	if ctx.Err() != nil {
		return firstErr
	}
	// The run's own verdict (fatal) names the cause, unless an earlier
	// failure left no rank finishing: then the earliest failure is the
	// cause, and the later ones are named after it.
	if fatal != nil && (fatalFirst || ok > 0) {
		return fatal
	}
	if ok == 0 {
		if firstErr == nil {
			return errors.New("no rank finished training")
		}
		if len(others) > 0 {
			return fmt.Errorf("%w (also failed: %s)", firstErr, strings.Join(others, ", "))
		}
		return firstErr
	}
	if len(died) > 0 {
		fmt.Fprintf(w, "[ddp] finished on %d of %d ranks (lost: %v)\n", ok, len(hosts), died)
	}
	return nil
}

// ddpGroupName names a run's sync group. Unique per run even under a fixed
// --job-name: the lead daemon keeps a group's dropped ranks and shares for
// ten minutes, and a re-run sharing the name would start with a dead rank.
func ddpGroupName(jobName string, now time.Time) string {
	nonce := strconv.FormatInt(now.UnixNano(), 36)
	if jobName == "" {
		return "ddp-" + nonce
	}
	return "ddp-" + jobName + "-" + nonce
}

// ddpReportDown tells the lead rank's daemon that a rank is gone, so the
// ring stops waiting for it and its data moves to the ranks left.
func ddpReportDown(host string, port int, group string, rank, world int, cause error) error {
	why := "its job ended"
	if cause != nil {
		why = cause.Error()
		if len(why) > 120 {
			why = why[:120] + "…"
		}
	}
	body, _ := json.Marshal(map[string]any{"group": group, "rank": rank, "world": world, "why": why})
	u := "http://" + net.JoinHostPort(host, strconv.Itoa(port)) + "/v1/ddp/down"
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok := authtoken.Current(); tok != "" {
		req.Header.Set(authtoken.Header, tok)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// A rank's machine that has not answered /health for ddpNodeLostAfter is
// lost, and its rank is stopped. Variables so tests run fast.
//
// Without this a DDP run hung on a Wi-Fi blip (Finding 5): a rank passes no
// detach channel, so app.Run never judges a broken connection, and the
// output stream is a websocket read with no deadline and no heartbeat. A
// connection that dies without a FIN or RST (the far side's Wi-Fi drops) is
// only noticed when TCP keepalive gives up, minutes later, while
// ddpAwaitRanks waits for that rank's result and nothing more is printed.
var (
	ddpNodeProbeEvery = 5 * time.Second
	ddpNodeLostAfter  = 30 * time.Second
	// ddpNetworkUp judges whether this machine still has a network.
	ddpNetworkUp = app.NetworkUp
)

// ddpNodeLost is a rank stopped because its machine stopped answering.
type ddpNodeLost struct {
	host   string
	silent time.Duration
	last   error
	how    string // what still answers from here; "" = this machine looks offline
}

func (e *ddpNodeLost) Error() string {
	if e.how == "" {
		return fmt.Sprintf("lost the connection to %s: no answer for %s (%v), and this machine looks offline (%s)",
			e.host, e.silent.Round(time.Second), e.last, app.NetworkDownWhy)
	}
	return fmt.Sprintf("lost the connection to %s: no answer for %s (%v) while %s",
		e.host, e.silent.Round(time.Second), e.last, e.how)
}

// ddpRunWatched runs one rank's job while watching its machine. When the
// machine goes silent the run is cancelled and the error is a *ddpNodeLost
// saying so, rather than the cancel it caused.
func ddpRunWatched(ctx context.Context, host string, port, selfPort int, run func(context.Context) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go ddpWatchNode(ctx, host, port, selfPort, cancel)
	err := run(ctx)
	var lost *ddpNodeLost
	if err != nil && errors.As(context.Cause(ctx), &lost) {
		return lost
	}
	return err
}

// ddpWatchNode asks host:port for /health every ddpNodeProbeEvery until ctx
// ends, and calls lost once nothing has answered for ddpNodeLostAfter. Any
// HTTP answer counts: the machine is there.
func ddpWatchNode(ctx context.Context, host string, port, selfPort int, lost context.CancelCauseFunc) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	client := &http.Client{Timeout: ddpNodeProbeEvery}
	t := time.NewTicker(ddpNodeProbeEvery)
	defer t.Stop()
	lastOK := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ddp] cannot watch %s (%v); a lost connection to it is noticed only when TCP gives up\n", addr, err)
			return
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			lastOK = time.Now()
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if silent := time.Since(lastOK); silent >= ddpNodeLostAfter {
			how, _ := ddpNetworkUp(selfPort, addr)
			lost(&ddpNodeLost{host: host, silent: silent, last: err, how: how})
			return
		}
	}
}
