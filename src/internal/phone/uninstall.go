package phone

// What setup adds to Termux, recorded as it goes, and the uninstall steps
// that remove exactly that: a package the user had before setup stays.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/doctor"
	"github.com/pipedpeer/pipedpeer/internal/term"
	"github.com/pipedpeer/pipedpeer/internal/termuxexec"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// legacyDistro is the container name setup used before it had its own; it
// may be the user's, so uninstall only names it.
const legacyDistro = "debian"

// installed is the record of what setup created, kept across setup reruns
// and updates: it only grows, so a package setup installed in any run stays
// ours even when a later run finds it present. Termux packages include the
// dependencies they pulled in; Containers are proot-distro names, Paths the
// directories setup made (an update may move them, the record keeps the old).
type installed struct {
	Pkgs       []string `json:"pkgs,omitempty"`
	Pip        []string `json:"pip,omitempty"`
	Containers []string `json:"containers,omitempty"`
	Paths      []string `json:"paths,omitempty"`
}

// recordPath never moves, so any later build finds it: under Termux's
// prefix, not userdir (whose paths an update may change).
func recordPath() string {
	return filepath.Join(prefix(), "var", "lib", "pipedpeer", "phone-installed.json")
}

// loadRecord reads the record; found is false when there is no file.
func loadRecord() (r installed, found bool) {
	b, err := os.ReadFile(recordPath())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Warn().Err(err).Str("path", recordPath()).Msg("phone: cannot read what setup installed; uninstall keeps every Termux package")
		}
		return r, false
	}
	if err := json.Unmarshal(b, &r); err != nil {
		log.Warn().Err(err).Str("path", recordPath()).Msg("phone: bad install record; uninstall keeps every Termux package")
	}
	return r, true
}

// record adds to the record (a union; nothing is ever dropped). Setup calls
// it before each install step, so an interrupted setup leaves a record of
// what it was installing.
func record(add installed) {
	r, _ := loadRecord()
	union := func(have *[]string, more []string) {
		for _, p := range more {
			if p != "" && !slices.Contains(*have, p) {
				*have = append(*have, p)
			}
		}
	}
	union(&r.Pkgs, add.Pkgs)
	union(&r.Pip, add.Pip)
	union(&r.Containers, add.Containers)
	union(&r.Paths, add.Paths)
	b, _ := json.MarshalIndent(r, "", "  ") // string slices always marshal
	err := os.MkdirAll(filepath.Dir(recordPath()), 0o700)
	if err == nil {
		err = os.WriteFile(recordPath(), b, 0o600)
	}
	if err != nil {
		log.Warn().Err(err).Interface("adding", add).Msg("phone: cannot record what setup installs; uninstall will keep these")
	}
}

// recordDirs records the Termux-side pipedpeer dirs and, when given, a
// container about to be created.
func recordDirs(container string) {
	add := installed{Paths: []string{userdir.Data(), userdir.State(), userdir.Cache()}}
	if container != "" {
		add.Containers = []string{container}
		add.Paths = append(add.Paths, filepath.Dir(rootfsOf(container)))
	}
	record(add)
}

// installedPkgs is every Termux package installed now; nil when dpkg cannot
// say.
func installedPkgs() map[string]bool {
	out, err := runCmd("dpkg-query", "-W", "-f=${Package} ${db:Status-Status}\n")
	if err != nil {
		log.Warn().Err(err).Str("output", tail(out)).Msg("phone: cannot list Termux packages; what setup installs now is not recorded for uninstall")
		return nil
	}
	have := map[string]bool{}
	for _, l := range strings.Split(string(out), "\n") {
		if name, status, ok := strings.Cut(strings.TrimSpace(l), " "); ok && status == "installed" {
			have[name] = true
		}
	}
	return have
}

// tracked runs install (a pkg install of pkgs) and records what it adds:
// before it runs, what apt says it will install (new packages only, not
// upgrades) plus the requested ones missing now; after, what actually
// appeared.
func tracked(pkgs []string, install func() error) error {
	before := installedPkgs()
	var plan []string
	if out, err := runCmd("apt-get", append([]string{"-s", "install"}, pkgs...)...); err != nil {
		log.Warn().Err(err).Str("output", tail(out)).Strs("packages", pkgs).Msg("phone: apt cannot say what it will install; recording the requested packages only")
	} else {
		for _, l := range strings.Split(string(out), "\n") {
			// "Inst name (ver ...)" is new; "Inst name [old] (ver ...)" an upgrade.
			if f := strings.Fields(l); len(f) > 2 && f[0] == "Inst" && !strings.HasPrefix(f[2], "[") {
				plan = append(plan, f[1])
			}
		}
	}
	if before != nil {
		for _, p := range pkgs {
			if !before[p] {
				plan = append(plan, p)
			}
		}
	}
	record(installed{Pkgs: plan})
	err := install()
	if before != nil {
		record(installed{Pkgs: newSince(before)})
	}
	return err
}

// newSince is the packages installed now that were not in before.
func newSince(before map[string]bool) []string {
	var added []string
	for p := range installedPkgs() {
		if !before[p] {
			added = append(added, p)
		}
	}
	slices.Sort(added)
	return added
}

// UninstallSteps are the phone's own parts of `pipedpeer uninstall`, run
// from Termux before its data dirs and the binary go.
func UninstallSteps() []doctor.Step {
	var steps []doctor.Step
	if ProotReady() {
		steps = append(steps, doctor.Step{What: "stop the daemon in the " + Distro + " container", Do: func() error {
			if code := forward([]string{"stop"}); code != 0 {
				return fmt.Errorf("pipedpeer stop in the container exited %d", code)
			}
			return nil
		}})
	}
	rec, found := loadRecord()
	failed := false
	add := func(what string, do func() error) {
		steps = append(steps, doctor.Step{What: what, Do: func() error {
			err := do()
			failed = failed || err != nil
			return err
		}})
	}
	if len(rec.Pip) > 0 {
		add("pip uninstall "+strings.Join(rec.Pip, " ")+" (installed by setup)", func() error {
			py, _, err := hostPython()
			if err != nil {
				log.Warn().Err(err).Strs("pip", rec.Pip).Msg("phone: no python to pip uninstall with; nothing of pip's is left either")
				return nil
			}
			if out, err := runCmd(py, append([]string{"-m", "pip", "uninstall", "-y"}, rec.Pip...)...); err != nil {
				return fmt.Errorf("%w: %s", err, tail(out))
			}
			return nil
		})
	}
	// Every container setup created, under any name it ever had, and ours
	// by name even without a record.
	names := append(slices.Clone(rec.Containers), Distro)
	slices.Sort(names)
	for _, name := range slices.Compact(names) {
		if !exists(rootfsOf(name)) {
			continue
		}
		add("delete the proot-distro container "+name+" (Debian, Nix and the daemon's state inside)", func() error {
			if out, err := runCmd("proot-distro", "remove", name); err != nil {
				return fmt.Errorf("proot-distro remove %s: %w: %s", name, err, tail(out))
			}
			return nil
		})
	}
	if !slices.Contains(rec.Containers, legacyDistro) && exists(filepath.Join(rootfsOf(legacyDistro), marker)) {
		add("keep the proot-distro container "+legacyDistro+": an older setup put Nix in it and it may be yours (remove it with: proot-distro remove "+legacyDistro+")", func() error { return nil })
	}
	// Dirs setup made at paths this build no longer uses (the current
	// userdir ones are the caller's).
	current := []string{userdir.Data(), userdir.State(), userdir.Cache()}
	for _, p := range rec.Paths {
		if exists(p) && !slices.Contains(current, p) {
			add("delete "+p+" (made by an earlier setup)", func() error { return os.RemoveAll(p) })
		}
	}
	if own := filepath.Dir(filepath.Dir(ownProot())); exists(own) {
		add("delete "+own+" (the proot built for proot-distro)", func() error { return os.RemoveAll(own) })
	}
	if len(rec.Pkgs) > 0 {
		add("remove the Termux packages setup installed: "+strings.Join(rec.Pkgs, " "), func() error { return removePkgs(rec.Pkgs) })
	}
	if !found {
		log.Warn().Str("path", recordPath()).Msg("phone: no install record; uninstall keeps every Termux package")
		var cands []string
		have := installedPkgs()
		for _, p := range append(append([]string{"proot-distro", "proot"}, requiredPkgs...), mlPkgs...) {
			if have[p] {
				cands = append(cands, p)
			}
		}
		what := "keep Termux packages: no install record at " + recordPath() + " (an older setup kept none), so setup's packages cannot be told from yours"
		if len(cands) > 0 {
			what += "; present: " + strings.Join(cands, " ") + " (to remove those you do not use: pkg uninstall <name>)"
		} else {
			what += "; none of them is present"
		}
		add(what, func() error { return nil })
	}
	// Package removal leaves the programs' own data: proot-distro's image
	// cache and locks, proot's temp dirs. Ours when setup installed them.
	pdData := filepath.Join(prefix(), "var", "lib", "proot-distro")
	if slices.Contains(rec.Pkgs, "proot-distro") && exists(pdData) {
		add("delete "+pdData+" (proot-distro's image cache)", func() error {
			if left, _ := os.ReadDir(filepath.Join(pdData, "containers")); len(left) > 0 {
				log.Warn().Int("containers", len(left)).Msg("phone: other proot-distro containers exist; keeping proot-distro's data")
				return nil
			}
			return os.RemoveAll(pdData)
		})
	}
	if slices.Contains(rec.Pkgs, "proot") {
		tmp, _ := filepath.Glob(filepath.Join(prefix(), "tmp", "proot-*")) // the pattern is valid
		if len(tmp) > 0 {
			add(fmt.Sprintf("delete proot's %d temp dirs in %s", len(tmp), filepath.Join(prefix(), "tmp")), func() error {
				var errs []error
				for _, d := range tmp {
					errs = append(errs, os.RemoveAll(d))
				}
				return errors.Join(errs...)
			})
		}
	}
	if found {
		// Last, and only after every step above worked: a rerun needs it.
		dir := filepath.Dir(recordPath())
		steps = append(steps, doctor.Step{What: "delete " + dir + " (the install record)", Do: func() error {
			if failed {
				return fmt.Errorf("kept, so a rerun of uninstall still knows what setup installed")
			}
			return os.RemoveAll(dir)
		}})
	}
	return steps
}

// ownProot is the proot scripts/phone-proot-setup.sh builds for
// proot-distro (PD_PROOT_BIN) on kernels Termux's proot cannot handle.
func ownProot() string { return filepath.Join(prefix(), "opt", "pipedpeer-proot", "bin", "proot") }

// removePkgs purges the recorded packages still installed, but none at all
// when that would take a package of the user's with them (one that needs
// ours): apt's simulation says which it would remove.
func removePkgs(rec []string) error {
	have := installedPkgs()
	if have == nil {
		return errors.New("cannot list Termux packages, so none removed")
	}
	var pkgs []string
	for _, p := range rec {
		if have[p] {
			pkgs = append(pkgs, p)
		}
	}
	if len(pkgs) == 0 {
		return nil
	}
	args := append([]string{"purge", "-y"}, pkgs...)
	out, err := runCmd("apt-get", append([]string{"-s"}, args...)...)
	if err != nil {
		return fmt.Errorf("apt-get -s purge: %w: %s", err, tail(out))
	}
	var theirs []string
	for _, l := range strings.Split(string(out), "\n") {
		if f := strings.Fields(l); len(f) > 1 && (f[0] == "Remv" || f[0] == "Purg") && !slices.Contains(pkgs, f[1]) {
			theirs = append(theirs, f[1])
		}
	}
	if len(theirs) > 0 {
		return fmt.Errorf("kept them all: removing them would also remove %s, which you installed (to remove anyway: pkg uninstall %s)",
			strings.Join(theirs, " "), strings.Join(pkgs, " "))
	}
	if out, err := runCmd("apt-get", args...); err != nil {
		return fmt.Errorf("apt-get purge: %w: %s", err, tail(out))
	}
	return nil
}

// phantomRevert puts Android's phantom-process killer back to its defaults:
// the lines undo docs/phone.md's and scripts/phone-phantom-off.sh's.
var phantomRevert = [][]string{
	{"shell", "device_config", "delete", "activity_manager", "max_phantom_processes"},
	{"shell", "settings", "put", "global", "settings_enable_monitor_phantom_procs", "true"},
	{"shell", "device_config", "set_sync_disabled_for_tests", "none"},
}

// PhantomNote prints the adb lines that undo the phantom-process settings
// and, when ask, offers to run them from this phone over Wireless debugging
// (the way scripts/phone-phantom-off.sh set them). dry only lists them.
func PhantomNote(w io.Writer, in *bufio.Reader, dry, ask bool) {
	verb := "Turn"
	if dry {
		verb = "would print how to turn"
	}
	fmt.Fprintf(w, "%s Android's phantom-process killer back on (if you turned it off for pipedpeer), from a computer with adb:\n", verb)
	for _, a := range phantomRevert {
		fmt.Fprintf(w, "  adb %s\n", strings.Join(a, " "))
	}
	if !dry {
		// Whatever the Android version: the record of "killer off" is stale
		// once the user reverts it.
		clearPhantomMark()
	}
	if dry || !ask {
		fmt.Fprintln(w, "or from this phone: Wireless debugging on, then adb pair and adb connect to 127.0.0.1 (see scripts/phone-phantom-off.sh)")
		return
	}
	if !yes(w, in, "Do it now from this phone over Wireless debugging? [y/N] ") {
		return
	}
	if err := phantomOverWifi(w, in); err != nil {
		log.Warn().Err(err).Msg("phone: reverting the phantom-process settings failed")
		term.Fail(w, "could not revert them: %v; run the lines above from a computer", err)
		return
	}
	term.OK(w, "phantom-process killer back to Android's defaults; Wireless debugging can be turned off")
}

func yes(w io.Writer, in *bufio.Reader, q string) bool {
	a := ask(w, in, q)
	return a == "y" || a == "yes"
}

// askTimeout is how long a question waits for an answer; a variable for tests.
var askTimeout = 5 * time.Minute

type askRes struct {
	s   string
	err error
}

// pendingAsk holds a line read still in flight per reader, so a question that
// timed out does not leave a second reader on the same terminal.
var pendingAsk = struct {
	sync.Mutex
	m map[*bufio.Reader]chan askRes
}{m: map[*bufio.Reader]chan askRes{}}

func ask(w io.Writer, in *bufio.Reader, q string) string {
	return askCtx(context.Background(), w, in, q)
}

// askCtx asks one question and returns the lowercased answer; "" on EOF, on
// ctx ending (Ctrl-C) and after askTimeout without an answer.
func askCtx(ctx context.Context, w io.Writer, in *bufio.Reader, q string) string {
	fmt.Fprint(w, q)
	pendingAsk.Lock()
	ch := pendingAsk.m[in]
	if ch == nil {
		ch = make(chan askRes, 1)
		pendingAsk.m[in] = ch
		go func() {
			s, err := in.ReadString('\n')
			ch <- askRes{s, err}
		}()
	}
	pendingAsk.Unlock()
	t := time.NewTimer(askTimeout)
	defer t.Stop()
	select {
	case r := <-ch:
		pendingAsk.Lock()
		delete(pendingAsk.m, in)
		pendingAsk.Unlock()
		if r.err != nil && r.s == "" {
			log.Warn().Err(r.err).Str("question", q).Msg("phone: no answer read")
		}
		return strings.ToLower(strings.TrimSpace(r.s))
	case <-ctx.Done():
		fmt.Fprintln(w)
		return ""
	case <-t.C:
		log.Warn().Str("question", q).Dur("after", askTimeout).Msg("phone: no answer in time")
		term.Warn(w, "no answer within %s; going on without one", askTimeout)
		return ""
	}
}

// adbTimeout bounds every adb call: a connect to a port that accepts but
// stalls, or a shell on a half-closed transport, must not hang setup.
var adbTimeout = 30 * time.Second

// adb runs adb with its output to w; a variable for tests.
var adb = func(w io.Writer, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), adbTimeout)
	defer cancel()
	cmd := termuxexec.CommandContext(ctx, "adb", args...)
	cmd.Stdout, cmd.Stderr = w, w
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("adb %s: no answer within %s: %w", args[0], adbTimeout, err)
		}
		return err
	}
	return nil
}

// phantomOverWifi connects this phone's own adb (wifiADB: ports from mDNS,
// pairing when refused), runs phantomRevert and leaves Wireless debugging as
// it was found. android-tools comes in for it when missing and goes again.
func phantomOverWifi(w io.Writer, in *bufio.Reader) error {
	if _, err := lookAdb(); err != nil {
		fmt.Fprintln(w, "installing adb (pkg install android-tools), removed again after...")
		before := installedPkgs()
		out, err := runCmd("pkg", "install", "-y", "android-tools")
		if before != nil {
			defer func() {
				added := newSince(before)
				if err := removePkgs(added); err != nil {
					log.Warn().Err(err).Strs("packages", added).Msg("phone: could not remove android-tools again")
					term.Warn(w, "android-tools stays: %v", err)
				}
			}()
		}
		if err != nil {
			return fmt.Errorf("pkg install android-tools: %w: %s", err, tail(out))
		}
	}
	dev, sess, err := wifiADB(context.Background(), w, in, true, true)
	defer sess.done()
	if err != nil {
		return err
	}
	// The mark goes first: a revert that stops half way must not leave
	// "already off" behind.
	clearPhantomMark()
	var errs []error
	for _, a := range phantomRevert {
		if err := sess.interrupted(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := adb(w, append([]string{"-s", dev}, a...)...); err != nil {
			errs = append(errs, fmt.Errorf("adb %s: %w", strings.Join(a, " "), err))
		}
	}
	return errors.Join(errs...)
}

// lookAdb finds adb; a variable for tests.
var lookAdb = func() (string, error) { return exec.LookPath("adb") }
