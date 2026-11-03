package phone

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/adbmdns"
)

// rig is a fake phone: adb, mDNS, the connect port and getprop, no real one.
type rig struct {
	t         *testing.T
	calls     []string
	wifiOn    bool // Wireless debugging; the user switches it on at the first prompt
	userOn    bool // the user does switch it on when asked
	paired    bool
	code      string
	maxV      string
	monV      string
	putErr    bool // adb_wifi_enabled 0 returns an error
	stuck     bool // the connect port keeps dialing after "off"
	noMcast   bool
	noPair    bool // mDNS never finds the pairing port
	silent    bool // Wireless debugging is on but mDNS stays silent until it is toggled (goodbye + announcement)
	deaf      bool // mDNS never hears anything
	discover  int
	pairCalls int
	hang      bool // mDNS blocks until its context ends
	props     map[string]string
	sdk       int
	pkgs      []string
}

func newRig(t *testing.T, sdk int) *rig {
	fakePhone(t)
	r := &rig{t: t, sdk: sdk, userOn: true, code: "123456", maxV: "null", monV: "true", props: map[string]string{}}
	oldAdb, oldLook, oldDisc, oldDial, oldGet, oldSdk, oldOpen, oldPair, oldInt := adb, lookAdb, discoverPorts, dialPort, getprop, sdkLevel, openTTY, discoverPair, interrupt
	oldS, oldP, oldPrep := settleFor, pollEvery, prepped
	t.Cleanup(func() {
		adb, lookAdb, discoverPorts, dialPort, getprop, sdkLevel, openTTY, discoverPair, interrupt = oldAdb, oldLook, oldDisc, oldDial, oldGet, oldSdk, oldOpen, oldPair, oldInt
		settleFor, pollEvery, prepped = oldS, oldP, oldPrep
	})
	settleFor, pollEvery, prepped = 40*time.Millisecond, 5*time.Millisecond, false
	sdkLevel = func() int { return r.sdk }
	getprop = func(name string) string { return r.props[name] }
	lookAdb = func() (string, error) { return "/bin/adb", nil }
	openTTY = func() (io.ReadCloser, error) { return nil, errors.New("no tty") }
	dialPort = func(p int) bool { return p == 42965 && (r.wifiOn || r.stuck) }
	discoverPair = func(ctx context.Context) (adbmdns.Ports, error) {
		r.pairCalls++
		if r.noPair {
			return adbmdns.Ports{}, context.DeadlineExceeded
		}
		return adbmdns.Ports{Pair: 41913}, nil // the dialog announces only the pairing service
	}
	discoverPorts = func(ctx context.Context, wantPair bool) (adbmdns.Ports, error) {
		r.discover++
		if r.noMcast {
			return adbmdns.Ports{}, adbmdns.ErrNoMulticast
		}
		if r.hang && r.discover >= 2 {
			<-ctx.Done()
			return adbmdns.Ports{}, ctx.Err()
		}
		if r.deaf || r.silent && r.discover == 1 {
			return adbmdns.Ports{}, context.DeadlineExceeded
		}
		if !r.wifiOn && r.discover >= 2 && r.userOn {
			r.wifiOn = true
		}
		if !r.wifiOn {
			return adbmdns.Ports{}, context.DeadlineExceeded
		}
		p := adbmdns.Ports{Connect: 42965, Goodbye: r.silent}
		if wantPair && !r.noPair {
			p.Pair = 41913
		}
		return p, nil
	}
	adb = func(w io.Writer, args ...string) error {
		c := strings.Join(args, " ")
		r.calls = append(r.calls, c)
		switch {
		case args[0] == "connect":
			if !r.paired {
				io.WriteString(w, "failed to authenticate to "+args[1])
			}
		case args[0] == "pair":
			if args[2] != r.code {
				io.WriteString(w, "Failed: Wrong password")
				return errors.New("exit 1")
			}
			r.paired = true
		case strings.HasSuffix(c, "adb_wifi_enabled 0"):
			r.wifiOn = false
			if r.putErr {
				return errors.New("closed")
			}
		case strings.HasSuffix(c, "persistent"):
		case strings.HasSuffix(c, "max_phantom_processes 2147483647"):
			r.maxV = "2147483647"
		case strings.HasSuffix(c, "settings_enable_monitor_phantom_procs false"):
			r.monV = "false"
		case strings.HasSuffix(c, "get global settings_enable_monitor_phantom_procs"):
			io.WriteString(w, r.monV+"\n")
		case strings.HasSuffix(c, "get activity_manager max_phantom_processes"):
			io.WriteString(w, r.maxV+"\n")
		}
		return nil
	}
	runCmd = func(name string, a ...string) ([]byte, error) {
		r.calls = append(r.calls, "RUN "+name+" "+strings.Join(a, " "))
		if name == "pkg" {
			r.pkgs = append(r.pkgs, a...)
		}
		return nil, nil
	}
	return r
}

func (r *rig) had(sub string) bool {
	for _, c := range r.calls {
		if strings.Contains(c, sub) {
			return true
		}
	}
	return false
}

func (r *rig) idx(sub string) int {
	for i, c := range r.calls {
		if strings.Contains(c, sub) {
			return i
		}
	}
	return -1
}

func (r *rig) adbCalls() []string {
	var o []string
	for _, c := range r.calls {
		if !strings.HasPrefix(c, "RUN ") {
			o = append(o, c)
		}
	}
	return o
}

const enter = "\n"

func prep(r *rig, in string, o PrepOpts) (string, error) {
	var out bytes.Buffer
	if o.In == nil && in != "-" {
		o.In = strings.NewReader(in)
	}
	err := TermuxPrep(&out, o)
	return out.String(), err
}

func wantSets(t *testing.T, r *rig) {
	t.Helper()
	a, b, c := r.idx("set_sync_disabled_for_tests persistent"), r.idx("max_phantom_processes 2147483647"), r.idx("settings_enable_monitor_phantom_procs false")
	if a < 0 || a > b || b > c {
		t.Errorf("sets missing or out of order: %q", r.calls)
	}
}

func TestPrepSdk30(t *testing.T) {
	r := newRig(t, 30)
	out, err := prep(r, "", PrepOpts{})
	if err != nil || !strings.Contains(out, "not needed below Android 12") || len(r.adbCalls()) != 0 || !r.had("RUN termux-wake-lock") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
}

func TestPrepNoTerminal(t *testing.T) {
	r := newRig(t, 33)
	lookAdb = func() (string, error) { return "", errors.New("missing") }
	out, err := prep(r, "-", PrepOpts{}) // no In, no tty; stdin in tests is not a terminal
	if err != nil || !r.had("RUN pkg install -y android-tools") || !strings.Contains(out, "pipedpeer setup") || r.had("shell") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
}

func TestPrepFresh(t *testing.T) { // 3
	r := newRig(t, 33)
	out, err := prep(r, "\n123456\n", PrepOpts{})
	if err != nil {
		t.Fatalf("err %v out %q", err, out)
	}
	wantSets(t, r)
	calls := r.adbCalls()
	if n := len(calls); calls[n-1] != "disconnect 127.0.0.1:42965" || calls[n-2] != "-s 127.0.0.1:42965 shell settings put global adb_wifi_enabled 0" {
		t.Errorf("tail %q", calls)
	}
	if !r.had("pair 127.0.0.1:41913 123456") || !strings.Contains(out, "phantom-process killer off") || strings.Contains(out, wifiOffNote) || strings.Contains(out, "child process") {
		t.Errorf("calls %q out %q", calls, out)
	}
}

func TestPrepWasOn(t *testing.T) { // 3b
	r := newRig(t, 33)
	r.wifiOn = true
	out, err := prep(r, "123456\n", PrepOpts{})
	wantSets(t, r)
	if err != nil || r.had("adb_wifi_enabled") || !r.had("disconnect") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
}

func TestPrepPutErrorButOff(t *testing.T) { // 3c
	r := newRig(t, 33)
	r.putErr = true
	out, _ := prep(r, "\n123456\n", PrepOpts{})
	if strings.Contains(out, wifiOffNote) || !strings.Contains(out, "phantom-process killer off") {
		t.Errorf("out %q", out)
	}
}

func TestPrepStillDials(t *testing.T) { // 3d
	r := newRig(t, 33)
	r.stuck = true
	out, err := prep(r, "\n123456\n", PrepOpts{})
	if err != nil || !strings.Contains(out, wifiOffNote) || !strings.Contains(out, "phantom-process killer off") {
		t.Errorf("err %v out %q", err, out)
	}
}

func TestPrepMdnsFails(t *testing.T) { // 4
	r := newRig(t, 33)
	r.noMcast, r.noPair = true, true
	out, err := prep(r, "42965\n41913\n123456\n", PrepOpts{})
	if err != nil || !r.had("connect 127.0.0.1:42965") || !r.had("pair 127.0.0.1:41913 123456") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
	if !strings.Contains(out, "pairing port: ") || !strings.Contains(out, "IP address & Port") {
		t.Errorf("out %q", out)
	}
	if r.had("adb_wifi_enabled") {
		t.Errorf("no mDNS: Wireless debugging state unknown, must be left alone: %q", r.calls)
	}
}

func TestPrepPairRetry(t *testing.T) { // 5, 6
	r := newRig(t, 33)
	out, err := prep(r, "\n000000\n111111\n123456\n", PrepOpts{})
	if err != nil || strings.Count(out, "pairing code: ") != 3 {
		t.Errorf("5: err %v out %q", err, out)
	}
	r = newRig(t, 33)
	out, err = prep(r, "\n1\n2\n3\n", PrepOpts{})
	if err == nil || r.had("max_phantom") || strings.Count(out, "pairing code: ") != 3 {
		t.Errorf("6: err %v out %q calls %q", err, out, r.calls)
	}
	if !r.had("adb_wifi_enabled 0") {
		t.Errorf("6: Wireless debugging not restored: %q", r.calls)
	}
}

func TestPrepRerunOn(t *testing.T) { // 7
	r := newRig(t, 33)
	r.wifiOn, r.paired, r.monV, r.maxV = true, true, "false", "2147483647"
	out, err := prep(r, "", PrepOpts{})
	if err != nil || !strings.Contains(out, "already off") || r.had("pair ") || r.had("put") || r.had("persistent") || r.had("adb_wifi_enabled") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
}

func TestPrepRerunOffThenRestore(t *testing.T) { // 7b
	r := newRig(t, 33)
	r.paired, r.monV, r.maxV = true, "false", "2147483647"
	out, err := prep(r, "\n", PrepOpts{})
	if err != nil || !strings.Contains(out, "already off") || r.had("pair ") || r.had("persistent") || !r.had("adb_wifi_enabled 0") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
}

func TestPrepSdk34(t *testing.T) { // 8, 9, 9b
	r := newRig(t, 34)
	r.props[overrideProp] = "false"
	out, err := prep(r, "-", PrepOpts{})
	if err != nil || !strings.Contains(out, "already off") || len(r.adbCalls()) != 0 || strings.Contains(out, "Enter") {
		t.Errorf("8: err %v out %q", err, out)
	}
	r = newRig(t, 34)
	in := &flipReader{r: r, v: "false"}
	out, err = prep(r, "", PrepOpts{In: in})
	if err != nil || strings.Count(out, "child process restrictions") != 1 || !strings.Contains(out, "phantom-process killer off") || len(r.adbCalls()) != 0 || r.had("android-tools") {
		t.Errorf("9: err %v out %q calls %q", err, out, r.calls)
	}
	r = newRig(t, 34)
	// H2: prop unset or unreadable, the user says yes: recorded, doctor reports it.
	out, err = prep(r, "\ny\n", PrepOpts{})
	if err != nil || !strings.Contains(out, "your answer") || len(r.adbCalls()) != 0 || r.had("android-tools") {
		t.Errorf("9b: err %v out %q", err, out)
	}
	if st := PhantomState(context.Background(), io.Discard, nil, true); st != PhantomOff || len(r.adbCalls()) != 0 { // 15f
		t.Errorf("15f: %v", st)
	}
	// H2 opposite: Enter without the toggle, the user says no (or nothing): not done.
	for _, in := range []string{"\nn\n", "\n\n", "\n"} {
		r = newRig(t, 34)
		out, err = prep(r, in, PrepOpts{})
		if err == nil || strings.Contains(out, "your answer") || strings.Contains(out, "phantom-process killer off") {
			t.Errorf("9b opposite %q: err %v out %q", in, err, out)
		}
		if st := PhantomState(context.Background(), io.Discard, nil, true); st != PhantomUnknown {
			t.Errorf("15f opposite %q: %v", in, st)
		}
	}
}

// flipReader sets the prop when the user "presses Enter".
type flipReader struct {
	r *rig
	v string
}

func (f *flipReader) Read(p []byte) (int, error) {
	f.r.props[overrideProp] = f.v
	return copy(p, "\n"), nil
}

func TestPrepSdk33NoToggle(t *testing.T) { // 9c is covered in TestPrepFresh; 10 here
	r := newRig(t, 33)
	r.wifiOn, r.paired = true, true
	out, err := prep(r, "", PrepOpts{NoInstall: true})
	if err != nil || r.had("put") || r.had("persistent") || r.had("pkg install") {
		t.Errorf("err %v calls %q", err, r.calls)
	}
	_ = out
	r = newRig(t, 33)
	lookAdb = func() (string, error) { return "", errors.New("missing") }
	prep(r, "", PrepOpts{NoInstall: true})
	if r.had("pkg install") {
		t.Errorf("pkg install ran with --no-install")
	}
}

func TestPrepOnce(t *testing.T) { // 11
	r := newRig(t, 30)
	prep(r, "", PrepOpts{})
	n := len(r.calls)
	prep(r, "", PrepOpts{})
	if len(r.calls) != n {
		t.Errorf("second call ran: %q", r.calls[n:])
	}
}

func TestPhantomState(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 33)
	if st := PhantomState(ctx, io.Discard, nil, false); st != PhantomUnknown || len(r.adbCalls()) != 0 { // 15
		t.Errorf("15: %v %q", st, r.calls)
	}
	r = newRig(t, 33) // 15b
	r.paired, r.monV, r.maxV = true, "false", "2147483647"
	if st := PhantomState(ctx, io.Discard, strings.NewReader("\n"), true); st != PhantomOff || !r.had("adb_wifi_enabled 0") {
		t.Errorf("15b: %v %q", st, r.calls)
	}
	r = newRig(t, 33) // 15c
	r.userOn = false
	var out bytes.Buffer
	if st := PhantomState(ctx, &out, strings.NewReader("\n"), true); st != PhantomUnknown || r.had("adb_wifi_enabled") || !strings.Contains(out.String(), "no Wireless debugging port") {
		t.Errorf("15c: %v %q %q", st, r.calls, out.String())
	}
	r = newRig(t, 33) // 15d
	r.wifiOn, r.paired = true, true
	if st := PhantomState(ctx, io.Discard, strings.NewReader(""), true); st != PhantomOn || r.had("adb_wifi_enabled") || !r.had("disconnect") { // also 16
		t.Errorf("15d/16: %v %q", st, r.calls)
	}
	r = newRig(t, 33) // 15e
	r.wifiOn = true
	out.Reset()
	if st := PhantomState(ctx, &out, strings.NewReader(""), true); st != PhantomUnknown || !strings.Contains(out.String(), "pipedpeer setup") || !r.had("disconnect") {
		t.Errorf("15e: %v %q %q", st, r.calls, out.String())
	}
	r = newRig(t, 30)
	if st := PhantomState(ctx, io.Discard, nil, true); st != PhantomNotNeeded {
		t.Errorf("sdk 30: %v", st)
	}
}

func TestPhantomFromEnv(t *testing.T) {
	t.Setenv(phantomEnv, "")
	if _, ok := PhantomFromEnv(); ok {
		t.Error("unset: ok")
	}
	t.Setenv(phantomEnv, "bogus")
	if _, ok := PhantomFromEnv(); ok {
		t.Error("bogus: ok")
	}
	t.Setenv(phantomEnv, "off")
	if p, ok := PhantomFromEnv(); !ok || p != PhantomOff {
		t.Errorf("off: %v %v", p, ok)
	}
}

func TestLaunchHooks(t *testing.T) { // 12, 13, 14
	marker := fakePhone(t)
	mustWrite(t, marker)
	oldP, oldS, oldF, oldE, oldOpen := prepFn, phantomStateFn, forwardFn, exitFn, openTTY
	t.Cleanup(func() { prepFn, phantomStateFn, forwardFn, exitFn, openTTY = oldP, oldS, oldF, oldE, oldOpen })
	t.Setenv(phantomEnv, "")
	var log []string
	var gotOpts PrepOpts
	prepFn = func(_ io.Writer, o PrepOpts) error { log = append(log, "prep"); gotOpts = o; return nil }
	phantomStateFn = func(_ context.Context, _ io.Writer, _ io.Reader, ask bool) Phantom {
		log = append(log, "state")
		if ask {
			log = append(log, "ask")
		}
		return PhantomOn
	}
	var envAtForward string
	forwardFn = func(a []string) int { log = append(log, "forward"); envAtForward = getenv(phantomEnv); return 0 }
	exitFn = func(int) {}
	openTTY = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("")), nil }

	Launch([]string{"setup", "-y", "--no-install"})
	if strings.Join(log, ",") != "prep,forward" || !gotOpts.AutoYes || !gotOpts.NoInstall {
		t.Errorf("12: %q %+v", log, gotOpts)
	}
	log = nil
	Launch([]string{"doctor"})
	if strings.Join(log, ",") != "state,ask,forward" || envAtForward != "on" {
		t.Errorf("13: %q env %q", log, envAtForward)
	}
	log = nil
	Launch([]string{"status"})
	if strings.Join(log, ",") != "forward" {
		t.Errorf("14: %q", log)
	}
}

func getenv(k string) string { return os.Getenv(k) }

func mustWrite(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPrepSilentOn(t *testing.T) { // P5b: on already, mDNS silent until toggled; the goodbye says it was on
	r := newRig(t, 33)
	r.wifiOn, r.silent = true, true
	out, err := prep(r, "\n123456\n", PrepOpts{})
	if err != nil || !strings.Contains(out, "off and on again") || !r.had("pair 127.0.0.1:41913 123456") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
	if r.had("adb_wifi_enabled") || !r.had("disconnect") {
		t.Errorf("it was on: must be left on: %q", r.calls)
	}
}

func TestPrepToggledFromOff(t *testing.T) { // P5b opposite: no goodbye, so it was off and goes off again
	r := newRig(t, 33)
	out, err := prep(r, "\n123456\n", PrepOpts{})
	if err != nil || !strings.Contains(out, "off and on again") || !r.had("adb_wifi_enabled 0") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
}

func TestPrepDeafAsksPort(t *testing.T) { // P5b: neither goodbye nor announcement: ask the port, leave the state alone
	r := newRig(t, 33)
	r.wifiOn, r.deaf, r.paired = true, true, true
	out, err := prep(r, "\n42965\n", PrepOpts{})
	if err != nil || !r.had("connect 127.0.0.1:42965") || !strings.Contains(out, "IP address & Port") || r.had("adb_wifi_enabled") {
		t.Errorf("err %v out %q calls %q", err, out, r.calls)
	}
}

func TestPrepPlayBuildNoBootNote(t *testing.T) { // P5a: TERMUX_VERSION carries googleplay.
	for _, c := range []struct {
		ver  string
		note bool
	}{{"googleplay.2026.10.09", false}, {"0.118.3", true}} {
		r := newRig(t, 30)
		t.Setenv("TERMUX_VERSION", c.ver)
		t.Setenv("TERMUX_APP__APP_VERSION_NAME", "")
		out, err := prep(r, "", PrepOpts{})
		if got := strings.Contains(out, "Termux:Boot"); err != nil || got != c.note {
			t.Errorf("%s: Termux:Boot note %v, want %v (err %v, out %q)", c.ver, got, c.note, err, out)
		}
	}
}

func TestPrepRerunSkipsAfterVerified(t *testing.T) { // H3
	r := newRig(t, 33)
	if _, err := prep(r, "\n123456\n", PrepOpts{}); err != nil {
		t.Fatal(err)
	}
	r.calls, prepped = nil, false
	out, err := prep(r, "-", PrepOpts{})
	if err != nil || !strings.Contains(out, "recorded by an earlier run") || len(r.adbCalls()) != 0 {
		t.Errorf("rerun: err %v out %q calls %q", err, out, r.calls)
	}
	if st := PhantomState(context.Background(), io.Discard, nil, false); st != PhantomOff || len(r.adbCalls()) != 0 {
		t.Errorf("doctor with the record: %v %q", st, r.calls)
	}
	// On request the phone is asked again.
	t.Setenv(verifyEnv, "1")
	r.calls, prepped = nil, false
	r.wifiOn, r.paired = true, true
	if st := PhantomState(context.Background(), io.Discard, strings.NewReader(""), true); st != PhantomOff || !r.had("get global settings_enable_monitor_phantom_procs") {
		t.Errorf("verify: %v %q", st, r.calls)
	}
	// A new Android version invalidates the record.
	t.Setenv(verifyEnv, "")
	r.sdk, r.wifiOn, r.userOn = 32, false, false
	if st := PhantomState(context.Background(), io.Discard, nil, false); st != PhantomUnknown {
		t.Errorf("other sdk: %v", st)
	}
}

func TestPrepNotRecordedWhenNotOff(t *testing.T) { // H3 opposite
	r := newRig(t, 33)
	prep(r, "\n1\n2\n3\n", PrepOpts{}) // pairing fails
	r.calls, prepped = nil, false
	if st := PhantomState(context.Background(), io.Discard, nil, false); st != PhantomUnknown {
		t.Errorf("a failed run left a record: %v", st)
	}
}

func TestPairUsesPairingServiceOnly(t *testing.T) { // H1
	r := newRig(t, 33)
	out, err := prep(r, "\n123456\n", PrepOpts{})
	if err != nil || r.pairCalls != 1 || !r.had("pair 127.0.0.1:41913 123456") {
		t.Errorf("err %v pairCalls %d out %q", err, r.pairCalls, out)
	}
	r = newRig(t, 33) // opposite: no pairing port announced: the port is asked
	r.noPair = true
	out, err = prep(r, "\n41913\n123456\n", PrepOpts{})
	if err != nil || !strings.Contains(out, "pairing port: ") {
		t.Errorf("err %v out %q", err, out)
	}
}

func TestAskTimeout(t *testing.T) { // H3
	old := askTimeout
	askTimeout = 50 * time.Millisecond
	t.Cleanup(func() { askTimeout = old })
	pr, pw := io.Pipe()
	t.Cleanup(func() { pw.Close() })
	in := bufioReader(pr)
	var out bytes.Buffer
	start := time.Now()
	if a := ask(&out, in, "q: "); a != "" || time.Since(start) > 2*time.Second || !strings.Contains(out.String(), "no answer within") {
		t.Errorf("timeout: %q after %v out %q", a, time.Since(start), out.String())
	}
	// The abandoned read still owns the reader: the late answer goes to the next question.
	go pw.Write([]byte("Yes\n"))
	if a := ask(&out, in, "q: "); a != "yes" {
		t.Errorf("late answer: %q", a)
	}
	// Opposite: an answer in time is returned.
	askTimeout = time.Minute
	go pw.Write([]byte("ok\n"))
	if a := ask(&out, in, "q: "); a != "ok" {
		t.Errorf("answer: %q", a)
	}
}

func TestInterruptRestores(t *testing.T) { // H4
	r := newRig(t, 33)
	var raised int
	var out syncBuf
	interrupt = func() { raised++ }
	pr, pw := io.Pipe()
	t.Cleanup(func() { pw.Close() })
	go func() {
		pw.Write([]byte("\n")) // Enter at "switch it on"; then the pairing code is awaited
		for i := 0; i < 500 && !strings.Contains(out.String(), "pairing code: "); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	done := make(chan error, 1)
	go func() { done <- TermuxPrep(&out, PrepOpts{In: pr}) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Ctrl-C at the pairing code did not end the wait")
	}
	if !r.had("adb_wifi_enabled 0") || !r.had("disconnect 127.0.0.1:42965") || raised != 1 {
		t.Errorf("not restored: raised %d calls %q", raised, r.calls)
	}
	if r.had("persistent") {
		t.Errorf("settings changed after the interrupt: %q", r.calls)
	}
}

func TestNoInterruptWhenNormal(t *testing.T) { // H4 opposite
	r := newRig(t, 33)
	var raised int
	interrupt = func() { raised++ }
	if _, err := prep(r, "\n123456\n", PrepOpts{}); err != nil || raised != 0 {
		t.Errorf("err %v raised %d", err, raised)
	}
}

func TestEnterGrace(t *testing.T) { // M1
	oW, oG := waitFor, graceFor
	waitFor, graceFor = 30*time.Second, 50*time.Millisecond
	t.Cleanup(func() { waitFor, graceFor = oW, oG })
	r := newRig(t, 33)
	r.hang = true
	r.paired = true
	start := time.Now()
	out, _ := prep(r, "\n42965\n", PrepOpts{})
	if time.Since(start) > 5*time.Second || !strings.Contains(out, "no Wireless debugging port found") || !r.had("connect 127.0.0.1:42965") {
		t.Errorf("after %v out %q calls %q", time.Since(start), out, r.calls)
	}
}

func TestEnterGraceNotNeeded(t *testing.T) { // M1 opposite: the announcement came: no grace wait
	oG := graceFor
	graceFor = 30 * time.Second
	t.Cleanup(func() { graceFor = oG })
	r := newRig(t, 33)
	start := time.Now()
	if _, err := prep(r, "\n123456\n", PrepOpts{}); err != nil || time.Since(start) > 5*time.Second {
		t.Errorf("err %v after %v", err, time.Since(start))
	}
}

func TestAdbTimeout(t *testing.T) { // M10
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "adb"), []byte("#!/bin/sh\nsleep 5\n"), 0o755)
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	old := adbTimeout
	adbTimeout = 100 * time.Millisecond
	t.Cleanup(func() { adbTimeout = old })
	start := time.Now()
	err := realAdb(io.Discard, "connect", "127.0.0.1:1")
	if err == nil || !strings.Contains(err.Error(), "no answer within") || time.Since(start) > 3*time.Second {
		t.Errorf("err %v after %v", err, time.Since(start))
	}
	os.WriteFile(filepath.Join(dir, "adb"), []byte("#!/bin/sh\necho ok\n"), 0o755) // opposite
	adbTimeout = 5 * time.Second
	var b bytes.Buffer
	if err := realAdb(&b, "version"); err != nil || !strings.Contains(b.String(), "ok") {
		t.Errorf("fast adb: %v %q", err, b.String())
	}
}

func TestFlagSyncDisclosed(t *testing.T) { // M11
	r := newRig(t, 33)
	out, _ := prep(r, "\n123456\n", PrepOpts{})
	if !strings.Contains(out, "server-side flags") {
		t.Errorf("no disclosure: %q", out)
	}
	r = newRig(t, 33) // opposite: already off, nothing is changed, nothing is announced
	r.wifiOn, r.paired, r.monV, r.maxV = true, true, "false", "2147483647"
	if out, _ = prep(r, "", PrepOpts{}); strings.Contains(out, "server-side flags") {
		t.Errorf("disclosure without a change: %q", out)
	}
}

func TestReadPhantomValueLine(t *testing.T) { // L4
	r := newRig(t, 33)
	adb = func(w io.Writer, args ...string) error {
		if strings.Contains(strings.Join(args, " "), "settings get") {
			io.WriteString(w, "false\nadb server version (41) doesn't match this client (40); killing...\n")
		} else {
			io.WriteString(w, "2147483647\n* daemon started successfully\n")
		}
		return nil
	}
	if st, err := readPhantom("x"); err != nil || st != PhantomOff {
		t.Errorf("%v %v", st, err)
	}
	adb = func(w io.Writer, args ...string) error { io.WriteString(w, "error: closed\n"); return nil } // opposite
	if st, err := readPhantom("x"); err == nil || st == PhantomOff {
		t.Errorf("garbage read as a value: %v %v", st, err)
	}
	_ = r
}

func bufioReader(r io.Reader) *bufio.Reader { return bufio.NewReader(r) }

var realAdb = adb

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }
