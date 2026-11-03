package phone

// The phantom-process killer: Android 12+ kills app child processes past 32,
// which kills pool workers. SDK 31-33: off over Wireless debugging and adb;
// SDK 34+: the Developer options toggle "Disable child process restrictions";
// below 31 there is no killer.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/term"
)

// Phantom is the killer's state as doctor reports it.
type Phantom string

const (
	PhantomOff       Phantom = "off"
	PhantomOn        Phantom = "on"
	PhantomUnknown   Phantom = "unknown"
	PhantomNotNeeded Phantom = "not-needed"
)

// phantomEnv carries Launch's answer into the Debian.
const phantomEnv = "PIPEDPEER_PHONE_PHANTOM"

// PhantomFromEnv reads what Launch passed; false when unset or not a state.
func PhantomFromEnv() (Phantom, bool) {
	p := Phantom(os.Getenv(phantomEnv))
	switch p {
	case PhantomOff, PhantomOn, PhantomUnknown, PhantomNotNeeded:
		return p, true
	}
	return "", false
}

const overrideProp = "persist.sys.fflag.override.settings_enable_monitor_phantom_procs"

// getprop reads an Android property ("" when unset or unreadable); a variable
// for tests.
var getprop = func(name string) string {
	out, err := runCmd("getprop", name)
	if err != nil {
		log.Warn().Err(err).Str("prop", name).Msg("phone: getprop failed")
		return ""
	}
	return strings.TrimSpace(string(out))
}

// sdkLevel is Android's SDK level; 0 when unreadable.
var sdkLevel = func() int {
	n, err := strconv.Atoi(getprop("ro.build.version.sdk"))
	if err != nil {
		return 0
	}
	return n
}

// phantomOffCmds are scripts/phone-phantom-off.sh's three commands, in order.
var phantomOffCmds = [][]string{
	{"shell", "device_config", "set_sync_disabled_for_tests", "persistent"},
	{"shell", "device_config", "put", "activity_manager", "max_phantom_processes", "2147483647"},
	{"shell", "settings", "put", "global", "settings_enable_monitor_phantom_procs", "false"},
}

// verifyEnv set (non-empty) makes setup and doctor check the phone again even
// when an earlier run recorded the killer as off.
const verifyEnv = "PIPEDPEER_PHONE_VERIFY"

// phantomMarkPath records, Termux-side, that the killer was verified off (or,
// on SDK 34+ where the property cannot always be read, that the user said so).
// The settings persist across reboots, so setup and doctor need not ask the
// phone again; the Android version is stored so an OS upgrade invalidates it.
func phantomMarkPath() string {
	return filepath.Join(prefix(), "var", "lib", "pipedpeer", "phone-phantom-off")
}

func markPhantom(sdk int) {
	p := phantomMarkPath()
	err := os.MkdirAll(filepath.Dir(p), 0o700)
	if err == nil {
		err = os.WriteFile(p, []byte(strconv.Itoa(sdk)+"\n"), 0o600)
	}
	if err != nil {
		log.Warn().Err(err).Str("path", p).Msg("phone: cannot record the phantom-process step; it will be checked again next time")
	}
}

// phantomMarked reports a mark for this Android version, unless a re-check is
// requested.
func phantomMarked(sdk int) bool {
	if os.Getenv(verifyEnv) != "" {
		return false
	}
	b, err := os.ReadFile(phantomMarkPath())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Warn().Err(err).Msg("phone: cannot read the phantom-process record")
		}
		return false
	}
	return strings.TrimSpace(string(b)) == strconv.Itoa(sdk)
}

func clearPhantomMark() {
	if err := os.Remove(phantomMarkPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn().Err(err).Msg("phone: cannot remove the phantom-process record")
	}
}

// propState is the SDK 34+ state: the override prop, no adb.
func propState() Phantom {
	switch getprop(overrideProp) {
	case "false":
		return PhantomOff
	case "true":
		return PhantomOn
	}
	return PhantomUnknown
}

// PhantomState is the killer's state, computed Termux-side. SDK 31-33 reads
// it over adb, which may need Wireless debugging switched on for a moment
// (only when ask and a terminal opens; the pairing persists, so no code).
// Wireless debugging is left as it was found.
func PhantomState(ctx context.Context, w io.Writer, in io.Reader, ask bool) Phantom {
	switch sdk := sdkLevel(); {
	case sdk == 0:
		log.Warn().Msg("phone: cannot read the Android version")
		return PhantomUnknown
	case sdk < 31:
		return PhantomNotNeeded
	case sdk >= 34:
		// The property is unset when the toggle was never touched and also
		// unreadable by this app: only a recorded answer tells them apart.
		if st := propState(); st != PhantomUnknown || !phantomMarked(sdk) {
			return st
		}
		return PhantomOff
	case phantomMarked(sdk):
		return PhantomOff
	}
	if _, err := lookAdb(); err != nil {
		log.Warn().Err(err).Msg("phone: adb missing; cannot read the phantom-process state")
		return PhantomUnknown
	}
	rd, ok, closeIn := inputOf(in, ask)
	defer closeIn()
	st, _, err := adbPhantom(ctx, w, rd, false, ask && ok, false)
	if err != nil {
		log.Warn().Err(err).Msg("phone: cannot read the phantom-process state")
		if errors.Is(err, errUnauthorized) {
			term.Action(w, "run `pipedpeer setup` to pair this phone's adb again")
		}
		return PhantomUnknown
	}
	return st
}

// adbPhantom reads the state over Wireless debugging and, when set and it is
// on, runs the three off commands. already: it was off before.
func adbPhantom(ctx context.Context, w io.Writer, in *bufio.Reader, pair, ask, set bool) (st Phantom, already bool, err error) {
	dev, done, err := wifiADB(ctx, w, in, pair, ask)
	defer done()
	if err != nil {
		return PhantomUnknown, false, err
	}
	st, err = readPhantom(dev)
	if err != nil || st == PhantomOff {
		if st == PhantomOff {
			markPhantom(sdkLevel())
		}
		return st, st == PhantomOff, err
	}
	if !set {
		return st, false, nil
	}
	term.Action(w, "this also stops the phone syncing server-side flags (device_config set_sync_disabled_for_tests) until `pipedpeer uninstall` puts it back")
	var errs []error
	for _, a := range phantomOffCmds {
		if err := adb(w, append([]string{"-s", dev}, a...)...); err != nil {
			errs = append(errs, fmt.Errorf("adb %s: %w", strings.Join(a, " "), err))
		}
	}
	st, rerr := readPhantom(dev)
	if rerr == nil && st == PhantomOff {
		markPhantom(sdkLevel())
	}
	return st, false, errors.Join(append(errs, rerr)...)
}

var (
	monRe = regexp.MustCompile(`^(true|false|null)$`)
	maxRe = regexp.MustCompile(`^(null|-?\d+)$`)
)

// readPhantom reads both settings over adb.
func readPhantom(dev string) (Phantom, error) {
	get := func(re *regexp.Regexp, args ...string) (string, error) {
		var b bytes.Buffer
		if err := adb(&b, append([]string{"-s", dev, "shell"}, args...)...); err != nil {
			return "", fmt.Errorf("adb %s: %w", strings.Join(args, " "), err)
		}
		// Only a line that is a value counts: adb may add warnings around it.
		for _, l := range strings.Split(b.String(), "\n") {
			if l = strings.TrimSpace(l); re.MatchString(l) {
				return l, nil
			}
		}
		return "", fmt.Errorf("adb %s: no value in %q", strings.Join(args, " "), strings.TrimSpace(b.String()))
	}
	mon, err := get(monRe, "settings", "get", "global", "settings_enable_monitor_phantom_procs")
	if err != nil {
		return PhantomUnknown, err
	}
	max, err := get(maxRe, "device_config", "get", "activity_manager", "max_phantom_processes")
	if err != nil {
		return PhantomUnknown, err
	}
	if mon == "false" && max == "2147483647" {
		return PhantomOff, nil
	}
	return PhantomOn, nil
}
