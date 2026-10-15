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
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/portmap"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
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

// judgeLostConnection decides, when the connection to a started job breaks,
// whether the node died (rerun it elsewhere: the failover) or this machine
// lost its network (the job runs on: detach). The node answering settles
// it; a silent node is judged by whether this machine still has a network
// (networkUp). selfPort is the local daemon's port.
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
	if how, ok := networkUp(selfPort, node); ok {
		return lostRerun, fmt.Sprintf("the node does not answer (%v) while %s", lastErr, how)
	}
	return lostDetach, fmt.Sprintf("the node does not answer (%v) and this machine looks offline: %s", lastErr, networkDownWhy)
}

// networkDownWhy names what networkUp tried when it found nothing answering.
const networkDownWhy = "no other peer, no introducer and no default gateway answers"

// Probe targets for networkUp; variables so tests can stand them in.
var (
	gatewayAddr    = portmap.DefaultGateway
	gatewayPorts   = []string{"53", "80", "443"}
	introducerAddr = lastIntroducer
)

// lastIntroducer is the most recently remembered introducer, or "".
func lastIntroducer() string {
	c, err := clustercfg.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not read the cluster config to ask the introducer whether this machine is online: %v\n", err)
		return ""
	}
	if len(c.Introducers) == 0 {
		return ""
	}
	return c.Introducers[len(c.Introducers)-1]
}

// NetworkUp is networkUp for the DDP watchdog in package main: whether this
// machine still has a network while node does not answer.
func NetworkUp(selfPort int, node string) (how string, ok bool) { return networkUp(selfPort, node) }

// NetworkDownWhy names what NetworkUp tried when nothing answered.
const NetworkDownWhy = networkDownWhy

// networkUp reports whether this machine still has a network, so that a
// silent node means the node is gone rather than us offline. With two
// machines there is no other peer to ask, so it also asks the introducer
// (if one is set) and the default gateway; how names what answered. All
// probes run at once and take at most healthTimeout; none needs root.
//
// The gateway does not count for an internet node (reached through this
// machine's loopback forwarder) when an introducer is set: the LAN being up
// says nothing about the uplink, and the introducer does.
func networkUp(selfPort int, node string) (how string, ok bool) {
	h, _, _ := net.SplitHostPort(node)
	ip := net.ParseIP(h)
	internetNode := ip != nil && ip.IsLoopback()
	intro := introducerAddr()

	probes := []func() (string, bool){
		func() (string, bool) {
			if peer, ok := anyPeerAnswers(selfPort, node); ok {
				return "peer " + peer + " answers", true
			}
			return "", false
		},
	}
	if intro != "" {
		probes = append(probes, func() (string, bool) {
			if introducerAnswers(intro) {
				return "the introducer " + intro + " answers", true
			}
			return "", false
		})
	}
	if !(internetNode && intro != "") {
		probes = append(probes, gatewayAnswers)
	}
	found := make(chan string, len(probes))
	for _, p := range probes {
		go func() {
			how, _ := p()
			found <- how
		}()
	}
	for range probes {
		if how := <-found; how != "" {
			return how, true
		}
	}
	return "", false
}

// introducerAnswers registers under a throwaway probe cluster (the
// introducer only answers registrations) and reports whether a reply came.
func introducerAnswers(server string) bool {
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not open a socket to ask the introducer whether this machine is online: %v\n", err)
		return false
	}
	defer conn.Close()
	_, _, err = rendezvous.Register(conn, server, rendezvous.ClusterID("pipedpeer-network-probe"), "probe", healthTimeout)
	return err == nil
}

// gatewayAnswers tries a TCP connect to the default gateway's usual ports. A
// refused connection is an answer too: the router is there.
func gatewayAnswers() (string, bool) {
	gw, err := gatewayAddr()
	if err != nil {
		return "", false // no default route: no network
	}
	found := make(chan bool, len(gatewayPorts))
	for _, port := range gatewayPorts {
		go func() {
			c, err := net.DialTimeout("tcp", net.JoinHostPort(gw.String(), port), healthTimeout)
			if err == nil {
				c.Close()
			}
			found <- err == nil || errors.Is(err, syscall.ECONNREFUSED)
		}()
	}
	for range gatewayPorts {
		if <-found {
			return "the default gateway " + gw.String() + " answers", true
		}
	}
	return "", false
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
		selfPort = daemonctl.DefaultPort
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
