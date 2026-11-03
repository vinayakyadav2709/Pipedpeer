package phone

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/adbmdns"
)

// rig is a fake phone: adb, mDNS, the connect port and getprop, no real one.
type rig struct {
	t        *testing.T
	calls    []string
	wifiOn   bool // Wireless debugging; the user switches it on at the first prompt
	userOn   bool // the user does switch it on when asked
	paired   bool
	code     string
	maxV     string
	monV     string
	putErr   bool // adb_wifi_enabled 0 returns an error
	stuck    bool // the connect port keeps dialing after "off"
	noMcast  bool
	noPair   bool // mDNS never finds the pairing port
	discover int
	props    map[string]string
	sdk      int
	pkgs     []string
}

func newRig(t *testing.T, sdk int) *rig {
	fakePhone(t)
	r := &rig{t: t, sdk: sdk, userOn: true, code: "123456", maxV: "null", monV: "true", props: map[string]string{}}
	oldAdb, oldLook, oldDisc, oldDial, oldGet, oldSdk, oldOpen := adb, lookAdb, discoverPorts, dialPort, getprop, sdkLevel, openTTY
	oldS, oldP, oldPrep := settleFor, pollEvery, prepped
	t.Cleanup(func() {
		adb, lookAdb, discoverPorts, dialPort, getprop, sdkLevel, openTTY = oldAdb, oldLook, oldDisc, oldDial, oldGet, oldSdk, oldOpen
		settleFor, pollEvery, prepped = oldS, oldP, oldPrep
	})
	settleFor, pollEvery, prepped = 40*time.Millisecond, 5*time.Millisecond, false
	sdkLevel = func() int { return r.sdk }
	getprop = func(name string) string { return r.props[name] }
	lookAdb = func() (string, error) { return "/bin/adb", nil }
	openTTY = func() (io.ReadCloser, error) { return nil, errors.New("no tty") }
	dialPort = func(p int) bool { return p == 42965 && (r.wifiOn || r.stuck) }
	discoverPorts = func(ctx context.Context, wantPair bool) (adbmdns.Ports, error) {
		r.discover++
		if r.noMcast {
			return adbmdns.Ports{}, adbmdns.ErrNoMulticast
		}
		if !r.wifiOn && r.discover >= 2 && r.userOn {
			r.wifiOn = true
		}
		if !r.wifiOn {
			return adbmdns.Ports{}, context.DeadlineExceeded
		}
		p := adbmdns.Ports{Connect: 42965}
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
	out, err = prep(r, "\n", PrepOpts{})
	if err != nil || !strings.Contains(out, "taken as done") || !strings.Contains(out, "cannot confirm") || len(r.adbCalls()) != 0 || r.had("android-tools") {
		t.Errorf("9b: err %v out %q", err, out)
	}
	if st := PhantomState(context.Background(), io.Discard, nil, true); st != PhantomUnknown || len(r.adbCalls()) != 0 { // 15f
		t.Errorf("15f: %v", st)
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
