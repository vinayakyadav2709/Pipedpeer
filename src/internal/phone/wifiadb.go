package phone

// Wireless debugging over this phone's own adb: ports from mDNS (adbmdns),
// pairing only when the connection is refused, and Wireless debugging left
// the way it was found. Termux cannot read the phantom-process settings
// itself (docs/phone.md), so everything that reads or writes them goes
// through here.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/adbmdns"
	"github.com/pipedpeer/pipedpeer/internal/term"
)

// Seams and timings; variables so tests need no phone.
var (
	discoverPorts = adbmdns.Discover
	dialPort      = func(port int) bool {
		c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 300*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}
	probeFor  = 4 * time.Second  // is Wireless debugging on already?
	waitFor   = 60 * time.Second // the user switches it on / opens the pairing dialog
	settleFor = 3 * time.Second  // the connect port stops dialing after "off"
	pollEvery = 100 * time.Millisecond
)

var (
	errNoWireless   = errors.New("Wireless debugging is off")
	errUnauthorized = errors.New("adb refused the connection (not paired, or the pairing is gone)")
)

const wifiOffNote = "turn off Wireless debugging in Developer options"

// wifiSession is one Wireless-debugging use; done restores what it found.
type wifiSession struct {
	w     io.Writer
	wasOn bool // the connect SRV dialed (or could not be checked) before we asked anything
	port  int  // connect port; 0 = none found
	dev   string
	ended bool
}

// wifiADB connects this phone's adb to itself. needPair: when the connection
// is refused, pair (pairing port from mDNS, else asked; the code is always
// asked). ask: the terminal may be questioned; without it a Wireless
// debugging that is off is errNoWireless. done is never nil and always safe
// to call, also after an error: it puts Wireless debugging back (off, if it
// was off at the start) and disconnects.
func wifiADB(ctx context.Context, w io.Writer, in *bufio.Reader, needPair, ask bool) (dev string, done func(), err error) {
	s := &wifiSession{w: w}
	pctx, cancel := context.WithTimeout(ctx, probeFor)
	p, derr := discoverPorts(pctx, false)
	cancel()
	noMcast := errors.Is(derr, adbmdns.ErrNoMulticast)
	switch {
	case p.Connect != 0:
		s.wasOn, s.port = true, p.Connect
	case noMcast:
		// Cannot tell whether it was on: leave its state alone.
		s.wasOn = true
		log.Warn().Err(derr).Msg("phone: no mDNS; cannot tell whether Wireless debugging was on, leaving it as it is")
	}
	if s.port == 0 && !noMcast {
		if !ask {
			return "", s.done, errNoWireless
		}
		fmt.Fprintln(w, "Settings > Developer options > Wireless debugging: switch it on for a moment (pipedpeer switches it off again afterwards)")
		ask1(w, in, "press Enter when it is on: ")
		wctx, cancel := context.WithTimeout(ctx, waitFor)
		p, derr = discoverPorts(wctx, false)
		cancel()
		if p.Connect != 0 {
			s.port = p.Connect
		} else {
			log.Warn().Err(derr).Msg("phone: no Wireless debugging connect port found")
			term.Warn(w, "no Wireless debugging port found within %s", waitFor)
			return "", s.done, errNoWireless
		}
	}
	if s.port == 0 { // no mDNS: ask
		if !ask {
			return "", s.done, errNoWireless
		}
		if s.port = askPort(w, in, "port under 'IP address & Port' on the Wireless debugging screen: "); s.port == 0 {
			return "", s.done, errNoWireless
		}
	}
	s.dev = "127.0.0.1:" + strconv.Itoa(s.port)
	err = s.connect()
	if err != nil && errors.Is(err, errUnauthorized) && needPair {
		if !ask {
			return "", s.done, err
		}
		if err = s.pair(ctx, in); err == nil {
			err = s.connect()
		}
	}
	if err != nil {
		return "", s.done, err
	}
	return s.dev, s.done, nil
}

func (s *wifiSession) connect() error {
	var b bytes.Buffer
	err := adb(&b, "connect", s.dev)
	out := strings.TrimSpace(b.String())
	low := strings.ToLower(out)
	bad := err != nil || strings.Contains(low, "failed") || strings.Contains(low, "unable") ||
		strings.Contains(low, "cannot") || strings.Contains(low, "refused") || strings.Contains(low, "unauthorized")
	if !bad {
		return nil
	}
	log.Warn().Err(err).Str("output", out).Str("device", s.dev).Msg("phone: adb connect failed")
	if strings.Contains(low, "authenticate") || strings.Contains(low, "unauthorized") {
		return errUnauthorized
	}
	if err != nil {
		return fmt.Errorf("adb connect: %w: %s", err, out)
	}
	return fmt.Errorf("adb connect: %s", out)
}

func (s *wifiSession) pair(ctx context.Context, in *bufio.Reader) error {
	fmt.Fprintln(s.w, "Wireless debugging > \"Pair device with pairing code\" (Termux and Settings in split screen)")
	wctx, cancel := context.WithTimeout(ctx, waitFor)
	p, err := discoverPorts(wctx, true)
	cancel()
	pp := p.Pair
	if pp == 0 {
		log.Warn().Err(err).Msg("phone: no pairing port found by mDNS; asking")
		if pp = askPort(s.w, in, "pairing port: "); pp == 0 {
			return errors.New("no pairing port")
		}
	}
	var perr error
	for i := 0; i < 3; i++ {
		code := ask(s.w, in, "pairing code: ")
		if perr = adb(s.w, "pair", "127.0.0.1:"+strconv.Itoa(pp), code); perr == nil {
			return nil
		}
		log.Warn().Err(perr).Int("attempt", i+1).Msg("phone: adb pair failed")
		term.Warn(s.w, "pairing failed (attempt %d of 3); the code changes if the dialog was closed", i+1)
	}
	return fmt.Errorf("adb pair: %w", perr)
}

// done restores Wireless debugging and disconnects (see wifiADB).
func (s *wifiSession) done() {
	if s.ended {
		return
	}
	s.ended = true
	if !s.wasOn {
		if s.dev != "" {
			// May end with the transport closing: the dial check decides.
			if err := adb(io.Discard, "-s", s.dev, "shell", "settings", "put", "global", "adb_wifi_enabled", "0"); err != nil {
				log.Debug().Err(err).Msg("phone: adb_wifi_enabled 0 returned an error (the transport may have dropped)")
			}
		}
		if s.port == 0 || !s.offSoon() {
			term.Warn(s.w, "%s", wifiOffNote)
		}
	}
	if s.dev != "" {
		if err := adb(io.Discard, "disconnect", s.dev); err != nil {
			log.Warn().Err(err).Str("device", s.dev).Msg("phone: adb disconnect failed")
		}
	}
}

// offSoon reports whether the connect port stops dialing within settleFor.
func (s *wifiSession) offSoon() bool {
	for end := time.Now().Add(settleFor); ; {
		if !dialPort(s.port) {
			return true
		}
		if time.Now().After(end) {
			return false
		}
		time.Sleep(pollEvery)
	}
}

func askPort(w io.Writer, in *bufio.Reader, q string) int {
	s := ask(w, in, q)
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 || n > 65535 {
		log.Warn().Str("answer", s).Msg("phone: not a port number")
		return 0
	}
	return n
}

// ask1 reads one line and drops it (press Enter).
func ask1(w io.Writer, in *bufio.Reader, q string) { ask(w, in, q) }
