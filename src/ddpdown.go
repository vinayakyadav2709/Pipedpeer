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
	"strconv"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// ddpRankResult is how one rank's job ended.
type ddpRankResult struct {
	rank int
	err  error
}

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

// ddpAwaitRanks waits for every rank, including ranks started mid-run
// (announced on added). A rank that fails while others still run is reported
// through onDown and the ring carries on without it. The run fails when no
// rank finishes, on cancel, or when a rank's script itself exited non-zero
// (a crash at checkpoint save is a failed run, not a lost machine); the ring
// still carries on so the other ranks do not stall on it. A rank added
// mid-run never fails the run: until it joins it has trained nothing, and a
// crash while joining (refused, shape mismatch) is the join failing. Once the
// script has failed, a rank announced on added is stopped at once: the run is
// lost, and the grower may have picked the very node that failed.
//
// A rank still waiting to join when only joiners are left is stopped after
// ddpJoinGrace: the ring it was joining has finished.
func ddpAwaitRanks(ctx context.Context, w io.Writer, results <-chan ddpRankResult, hosts []string, added <-chan ddpAdded, onDown func(rank int, err error)) error {
	hosts = append([]string(nil), hosts...)
	running := len(hosts)
	joiners := map[int]context.CancelFunc{}
	var grace <-chan time.Time
	stopped := false
	var firstErr error
	var died []int
	var scriptErr error
	refused := map[int]bool{}
	ok := 0
	for running > 0 {
		if grace == nil && !stopped && len(joiners) > 0 && running == len(joiners) {
			grace = time.After(ddpJoinGrace)
		}
		select {
		case a := <-added:
			for len(hosts) <= a.rank {
				hosts = append(hosts, "?")
			}
			hosts[a.rank] = a.host
			running++
			if scriptErr != nil {
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
			continue
		case r := <-results:
			running--
			_, joiner := joiners[r.rank]
			delete(joiners, r.rank)
			if r.err == nil {
				ok++
				continue
			}
			if (refused[r.rank] || joiner && stopped) && errors.Is(r.err, context.Canceled) {
				continue
			}
			host := "?"
			if r.rank >= 0 && r.rank < len(hosts) {
				host = hosts[r.rank]
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("rank %d on %s failed: %w", r.rank, host, r.err)
			}
			if ctx.Err() != nil {
				continue // a cancel stops every rank; nobody died
			}
			died = append(died, r.rank)
			script := !joiner && app.IsPermanent(r.err)
			if script && scriptErr == nil {
				scriptErr = fmt.Errorf("rank %d on %s: the script failed: %w", r.rank, host, r.err)
			}
			if running > 0 {
				if script {
					fmt.Fprintf(w, "[ddp] rank %d on %s: its script failed; continuing on %d, but the run will fail\n", r.rank, host, running)
				} else {
					fmt.Fprintf(w, "[ddp] rank %d on %s died; continuing on %d\n", r.rank, host, running)
				}
				fmt.Fprintf(w, "[ddp]   cause: %v\n", r.err)
				onDown(r.rank, r.err)
			}
		}
	}
	if ctx.Err() != nil || ok == 0 {
		return firstErr
	}
	if scriptErr != nil {
		return scriptErr
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
