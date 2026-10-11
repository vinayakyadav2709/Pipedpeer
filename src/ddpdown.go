package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

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

// ddpAwaitRanks waits for every rank. A rank that fails while others still
// run is reported through onDown and the ring carries on without it. The run
// fails only when no rank finishes, or on cancel; the first rank error is
// returned then.
func ddpAwaitRanks(ctx context.Context, w io.Writer, results <-chan ddpRankResult, hosts []string, onDown func(rank int, err error)) error {
	running := len(hosts)
	var firstErr error
	var died []int
	ok := 0
	for range hosts {
		r := <-results
		running--
		if r.err == nil {
			ok++
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
		if running > 0 {
			fmt.Fprintf(w, "[ddp] rank %d on %s died; continuing on %d\n", r.rank, host, running)
			fmt.Fprintf(w, "[ddp]   cause: %v\n", r.err)
			onDown(r.rank, r.err)
		}
	}
	if ctx.Err() != nil || ok == 0 {
		return firstErr
	}
	if len(died) > 0 {
		fmt.Fprintf(w, "[ddp] finished on %d of %d ranks (lost: %v)\n", ok, len(hosts), died)
	}
	return nil
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
