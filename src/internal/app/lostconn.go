package app

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// lostVerdict is what a run does after its connection to the node broke.
type lostVerdict int

const (
	lostRerun  lostVerdict = iota // the node is gone: run the job elsewhere
	lostDetach                    // the job runs on; this machine collects it later
	lostEnded                     // the job already ended on the node: collect it now
)

// The node is asked lostProbeTries times, lostProbeGap apart (~10 s), before
// it counts as unreachable. Variables so tests run fast.
var (
	lostProbeTries = 5
	lostProbeGap   = 2 * time.Second
	healthTimeout  = 2 * time.Second
)

// defaultDaemonPort is the local daemon's port when the caller gave none.
const defaultDaemonPort = 38080

// judgeLostConnection decides, when the connection to a started job breaks,
// whether the node died (rerun it elsewhere: the failover) or this machine
// lost its network (the job runs on: detach). The node answering settles
// it; a silent node is judged by whether any other peer the local daemon
// knows still answers /health. selfPort is the local daemon's port.
func judgeLostConnection(host string, port int, jobID string, selfPort int) (lostVerdict, string) {
	var lastErr error
	for i := 0; i < lostProbeTries; i++ {
		if i > 0 {
			time.Sleep(lostProbeGap)
		}
		st, err := daemonctl.JobStatus(host, port, jobID)
		if err == nil {
			if jobEnded(st.Status) {
				return lostEnded, "the job ended on the node (" + st.Status + ")"
			}
			return lostDetach, "the node still runs the job"
		}
		if !nodeUnreachable(err) {
			return lostRerun, fmt.Sprintf("the node no longer has the job (%v)", err)
		}
		lastErr = err
	}
	node := net.JoinHostPort(host, strconv.Itoa(port))
	if peer, ok := anyPeerAnswers(selfPort, node); ok {
		return lostRerun, fmt.Sprintf("the node does not answer (%v) while peer %s does", lastErr, peer)
	}
	return lostDetach, fmt.Sprintf("neither the node (%v) nor any other peer answers, so this machine is the one offline", lastErr)
}

// nodeUnreachable: err is a transport failure (no answer at all), not an
// answer from the node.
func nodeUnreachable(err error) bool {
	var ue *url.Error
	return errors.As(err, &ue)
}

// anyPeerAnswers reports whether some peer the local daemon lists, other than
// this machine and except, answers /health; it returns that peer's address.
func anyPeerAnswers(selfPort int, except string) (string, bool) {
	if selfPort <= 0 {
		selfPort = defaultDaemonPort
	}
	var nodes []struct {
		SSHEndpoint string `json:"ssh_endpoint"`
		DaemonPort  int    `json:"daemon_port"`
		Source      string `json:"source"`
	}
	if err := getJSON(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", selfPort), &nodes); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not list peers from the local daemon to check this machine's network: %v\n", err)
		return "", false
	}
	found := make(chan string, len(nodes))
	asked := 0
	client := &http.Client{Timeout: healthTimeout}
	for _, n := range nodes {
		if strings.Contains(n.Source, "self") {
			continue
		}
		h := n.SSHEndpoint
		if i := strings.LastIndex(h, "@"); i >= 0 {
			h = h[i+1:]
		}
		if hh, _, err := net.SplitHostPort(h); err == nil {
			h = hh
		}
		addr := net.JoinHostPort(h, strconv.Itoa(n.DaemonPort))
		if h == "" || addr == except {
			continue
		}
		asked++
		go func() {
			resp, err := client.Get("http://" + addr + "/health")
			if err != nil {
				found <- ""
				return
			}
			resp.Body.Close()
			found <- addr
		}()
	}
	for ; asked > 0; asked-- {
		if addr := <-found; addr != "" {
			return addr, true
		}
	}
	return "", false
}
