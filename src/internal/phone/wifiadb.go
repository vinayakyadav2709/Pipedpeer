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
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/adbmdns"
	"github.com/pipedpeer/pipedpeer/internal/term"
)

// Seams and timings; variables so tests need no phone.
var (
	discoverPorts = adbmdns.Discover
	discoverPair  = adbmdns.DiscoverPair
	// interrupt re-raises the signal that ended a session, once Wireless
	// debugging is restored. If the process ignores it (nohup, a
	// non-interactive shell), it exits with the shell's 128+n.
	interrupt = func(sig syscall.Signal) {
		syscall.Kill(os.Getpid(), sig)
		time.Sleep(200 * time.Millisecond)
		os.Exit(128 + int(sig))
	}
	dialPort = func(port int) bool {
		c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 300*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}
	probeFor  = 5 * time.Second  // is Wireless debugging on already?
	graceFor  = 5 * time.Second  // after Enter: how long to wait for the announcement
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

	ctx    context.Context // ends on Ctrl-C / SIGTERM
	parent context.Context
	stop   func()

	mu  sync.Mutex
	sig syscall.Signal // the signal that ended ctx; 0 = none
}

// interrupted is non-nil once a signal ended the session.
func (s *wifiSession) interrupted() error {
	if s.ctx.Err() != nil {
		return errors.New("interrupted")
	}
	return nil
}

// wifiADB connects this phone's adb to itself. needPair: when the connection
// is refused, pair (pairing port from mDNS, else asked; the code is always
// asked). ask: the terminal may be questioned; without it a Wireless
// debugging that is off is errNoWireless. done is never nil and always safe
// to call, also after an error: it puts Wireless debugging back (off, if it
// was off at the start) and disconnects.
func wifiADB(ctx context.Context, w io.Writer, in *bufio.Reader, needPair, ask bool) (dev string, sess *wifiSession, err error) {
	sctx, cancelS := context.WithCancel(ctx)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	s := &wifiSession{w: w, ctx: sctx, parent: ctx}
	quit := make(chan struct{})
	var once sync.Once
	s.stop = func() { once.Do(func() { signal.Stop(ch); close(quit); cancelS() }) }
	go func() {
		select {
		case sg := <-ch:
			if n, ok := sg.(syscall.Signal); ok {
				s.mu.Lock()
				s.sig = n
				s.mu.Unlock()
			}
			// Handled once: a second Ctrl-C finds the default action again
			// and ends the process, however long adb hangs.
			signal.Stop(ch)
			cancelS()
		case <-quit:
		}
	}()
	pctx, cancel := context.WithTimeout(sctx, probeFor)
	p, derr := discoverPorts(pctx, false)
	cancel()
	if err := s.interrupted(); err != nil {
		return "", s, err
	}
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
			return "", s, errNoWireless
		}
		// The phone does not answer its own queries while Wireless debugging
		// is already on, but a toggle announces it. Listen from now on, so
		// the goodbye of "off" is heard before the announcement of "on".
		type found struct {
			p   adbmdns.Ports
			err error
		}
		res := make(chan found, 1)
		wctx, cancel := context.WithTimeout(sctx, waitFor)
		defer cancel()
		go func() {
			p, err := discoverPorts(wctx, false)
			res <- found{p, err}
		}()
		fmt.Fprintln(w, "Settings > Developer options > Wireless debugging: switch it off and on again (on for a moment if it is off; pipedpeer switches it off afterwards if it was off)")
		askCtx(sctx, w, in, "press Enter when it is on: ")
		var f found
		got := false
		select {
		case f = <-res:
			got = true
		case <-time.After(graceFor):
			// Enter was pressed and nothing was heard: stop waiting.
			cancel()
			f, got = <-res, true
		case <-sctx.Done():
		}
		if err := s.interrupted(); err != nil {
			// Ctrl-C while the toggle was being switched: what the listener
			// heard so far is needed to switch it off again.
			cancel()
			if !got {
				f = <-res
			}
			if f.p.Connect != 0 {
				s.port, s.wasOn = f.p.Connect, f.p.Goodbye
			}
			return "", s, err
		}
		switch {
		case f.p.Connect != 0:
			s.port = f.p.Connect
			s.wasOn = f.p.Goodbye // a goodbye came first: it was on
		default:
			log.Warn().Err(f.err).Msg("phone: no Wireless debugging connect port found")
			term.Warn(w, "no Wireless debugging port found within %s", waitFor)
			// State unknown: ask for the port below and leave it as it is.
			s.wasOn = true
		}
	}
	if s.port == 0 { // no mDNS: ask
		if !ask {
			return "", s, errNoWireless
		}
		if s.port = askPort(sctx, w, in, "port under 'IP address & Port' on the Wireless debugging screen: "); s.port == 0 {
			return "", s, errNoWireless
		}
	}
	if err := s.interrupted(); err != nil {
		return "", s, err
	}
	s.dev = "127.0.0.1:" + strconv.Itoa(s.port)
	err = s.connect()
	if err != nil && errors.Is(err, errUnauthorized) && needPair {
		if !ask {
			return "", s, err
		}
		if err = s.pair(in); err == nil {
			err = s.connect()
		}
	}
	if err != nil {
		return "", s, err
	}
	return s.dev, s, nil
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

func (s *wifiSession) pair(in *bufio.Reader) error {
	fmt.Fprintln(s.w, "Wireless debugging > \"Pair device with pairing code\" (Termux and Settings in split screen)")
	// Only the pairing service is announced while the dialog is open.
	wctx, cancel := context.WithTimeout(s.ctx, waitFor)
	p, err := discoverPair(wctx)
	cancel()
	pp := p.Pair
	if pp == 0 {
		log.Warn().Err(err).Msg("phone: no pairing port found by mDNS; asking")
		if pp = askPort(s.ctx, s.w, in, "pairing port: "); pp == 0 {
			return errors.New("no pairing port")
		}
	}
	var perr error
	for i := 0; i < 3; i++ {
		code := askCtx(s.ctx, s.w, in, "pairing code: ")
		if err := s.interrupted(); err != nil {
			return err
		}
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
	s.mu.Lock()
	sig := s.sig
	s.mu.Unlock()
	defer func() {
		s.stop()
		if sig != 0 && s.parent.Err() == nil {
			interrupt(sig)
		}
	}()
	if !s.wasOn && s.dev == "" && s.port != 0 {
		// Interrupted after Wireless debugging came on but before adb was
		// connected: connect now, so it can be switched off again.
		dev := "127.0.0.1:" + strconv.Itoa(s.port)
		s.dev = dev
		if err := adb(io.Discard, "connect", dev); err != nil {
			log.Warn().Err(err).Str("device", dev).Msg("phone: adb connect for switching Wireless debugging off failed")
		}
	}
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

func askPort(ctx context.Context, w io.Writer, in *bufio.Reader, q string) int {
	s := askCtx(ctx, w, in, q)
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 || n > 65535 {
		log.Warn().Str("answer", s).Msg("phone: not a port number")
		return 0
	}
	return n
}
