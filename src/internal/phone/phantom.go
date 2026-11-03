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
		return propState()
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
		return st, st == PhantomOff, err
	}
	if !set {
		return st, false, nil
	}
	var errs []error
	for _, a := range phantomOffCmds {
		if err := adb(w, append([]string{"-s", dev}, a...)...); err != nil {
			errs = append(errs, fmt.Errorf("adb %s: %w", strings.Join(a, " "), err))
		}
	}
	st, rerr := readPhantom(dev)
	return st, false, errors.Join(append(errs, rerr)...)
}

// readPhantom reads both settings over adb.
func readPhantom(dev string) (Phantom, error) {
	get := func(args ...string) (string, error) {
		var b bytes.Buffer
		if err := adb(&b, append([]string{"-s", dev, "shell"}, args...)...); err != nil {
			return "", fmt.Errorf("adb %s: %w", strings.Join(args, " "), err)
		}
		lines := strings.Fields(b.String())
		if len(lines) == 0 {
			return "", fmt.Errorf("adb %s: empty answer", strings.Join(args, " "))
		}
		return lines[len(lines)-1], nil
	}
	mon, err := get("settings", "get", "global", "settings_enable_monitor_phantom_procs")
	if err != nil {
		return PhantomUnknown, err
	}
	max, err := get("device_config", "get", "activity_manager", "max_phantom_processes")
	if err != nil {
		return PhantomUnknown, err
	}
	if mon == "false" && max == "2147483647" {
		return PhantomOff, nil
	}
	return PhantomOn, nil
}
