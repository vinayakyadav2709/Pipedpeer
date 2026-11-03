package phone

// TermuxPrep: what setup does Termux-side before (and instead of) the
// Debian: the wake lock, adb, the phantom-process killer, and the boot script.
// Every step checks first and says so when there is nothing to do.

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/term"
)

// PrepOpts are setup's flags.
type PrepOpts struct {
	AutoYes, NoInstall bool
	In                 io.Reader // nil: /dev/tty if it opens, else os.Stdin
}

// prepped stops TermuxPrep running twice in one process (a first run, then
// the same setup forwarded into the Debian).
var prepped bool

// openTTY opens the controlling terminal; a variable for tests.
var openTTY = func() (io.ReadCloser, error) { return os.Open("/dev/tty") }

// inputOf is the reader questions use. ok is false with no terminal at all
// (no /dev/tty and stdin is not a terminal): then nothing may be asked.
func inputOf(in io.Reader, want bool) (rd *bufio.Reader, ok bool, closeIn func()) {
	noop := func() {}
	if !want {
		return bufio.NewReader(strings.NewReader("")), false, noop
	}
	if in != nil {
		return bufio.NewReader(in), true, noop
	}
	if f, err := openTTY(); err == nil {
		return bufio.NewReader(f), true, func() { f.Close() }
	}
	if stdinIsTerm() {
		return bufio.NewReader(os.Stdin), true, noop
	}
	return bufio.NewReader(strings.NewReader("")), false, noop
}

const toggleName = `Settings > Developer options > "Disable child process restrictions"`

// TermuxPrep runs the steps; the returned error joins the failed ones
// (setup warns and goes on).
func TermuxPrep(w io.Writer, o PrepOpts) error {
	if prepped {
		return nil
	}
	prepped = true
	var errs []error
	if out, err := runCmd("termux-wake-lock"); err != nil {
		log.Warn().Err(err).Str("output", tail(out)).Msg("phone: termux-wake-lock failed")
		term.Warn(w, "termux-wake-lock failed: %v", err)
		errs = append(errs, err)
	} else {
		term.OK(w, "wake lock held")
	}
	rd, hasTerm, closeIn := inputOf(o.In, true)
	defer closeIn()
	var err error
	switch sdk := sdkLevel(); {
	case sdk == 0:
		log.Warn().Msg("phone: cannot read the Android version")
		term.Warn(w, "cannot read the Android version; the phantom-process step is skipped")
	case sdk < 31:
		term.OK(w, "phantom-process killer: not needed below Android 12")
	case sdk >= 34:
		err = prepToggle(w, rd, hasTerm, o)
	default:
		err = prepAdb(w, rd, hasTerm, o)
	}
	if err != nil {
		errs = append(errs, err)
	}
	if err := writeBoot(w, os.Getenv("TERMUX_APP__APP_VERSION_NAME")); err != nil {
		log.Warn().Err(err).Msg("phone: start-on-boot script not written")
		term.Warn(w, "start on boot: %v", err)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// prepToggle is the SDK 34+ route: the user flips a Developer options toggle.
func prepToggle(w io.Writer, in *bufio.Reader, hasTerm bool, o PrepOpts) error {
	if propState() == PhantomOff {
		term.OK(w, "phantom-process killer already off")
		return nil
	}
	if o.NoInstall {
		term.Warn(w, "phantom-process killer is not off; turn on %s", toggleName)
		return nil
	}
	if !hasTerm {
		term.Action(w, "turn on %s, or run `pipedpeer setup` in a terminal", toggleName)
		return nil
	}
	term.Action(w, "turn on %s", toggleName)
	ask(w, in, "press Enter when it is on: ")
	switch propState() {
	case PhantomOff:
		term.OK(w, "phantom-process killer off")
	case PhantomOn:
		term.Fail(w, "the toggle still reads off; turn on %s and run `pipedpeer setup` again", toggleName)
		return errors.New("phantom-process killer still on")
	default:
		term.OK(w, "phantom-process killer: taken as done")
		term.Warn(w, "this phone's property cannot be read, so `pipedpeer doctor` cannot confirm the toggle")
	}
	return nil
}

// prepAdb is the SDK 31-33 route: Wireless debugging and the pairing code.
func prepAdb(w io.Writer, in *bufio.Reader, hasTerm bool, o PrepOpts) error {
	if _, err := lookAdb(); err != nil {
		if o.NoInstall {
			term.Warn(w, "adb (android-tools) is missing; the phantom-process killer is not checked")
			return nil
		}
		pkgs := []string{"android-tools"}
		var out []byte
		if err := tracked(pkgs, func() (err error) {
			out, err = runCmd("pkg", "install", "-y", "android-tools")
			return err
		}); err != nil {
			log.Warn().Err(err).Str("output", tail(out)).Msg("phone: pkg install android-tools failed")
			term.Warn(w, "pkg install android-tools failed: %v", err)
			return err
		}
	}
	st, already, err := adbPhantom(context.Background(), w, in, true, hasTerm, !o.NoInstall)
	switch {
	case err != nil && !hasTerm && (errors.Is(err, errNoWireless) || errors.Is(err, errUnauthorized)):
		term.Action(w, "turn the phantom-process killer off: run `pipedpeer setup` in a terminal (Wireless debugging and its pairing code)")
		return nil
	case err != nil:
		log.Warn().Err(err).Msg("phone: phantom-process step failed")
		term.Warn(w, "phantom-process killer not changed: %v", err)
		return err
	case already:
		term.OK(w, "phantom-process killer already off")
	case st == PhantomOff:
		term.OK(w, "phantom-process killer off")
	case o.NoInstall:
		term.Warn(w, "phantom-process killer is on (--no-install: left as it is)")
	default:
		term.Warn(w, "phantom-process killer still reads on after the commands")
		return errors.New("phantom-process killer still on")
	}
	return nil
}

// stdinIsTerm: stdin is a character device other than /dev/null (a terminal;
// avoids a cgo/x-term dependency for one check).
func stdinIsTerm() bool {
	st, err := os.Stdin.Stat()
	if err != nil || st.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	null, err := os.Stat(os.DevNull)
	return err != nil || !os.SameFile(st, null)
}
