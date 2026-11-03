package phone

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/term"
)

// bootScript is ~/.termux/boot/pipedpeer: Termux runs every script in that
// directory on BOOT_COMPLETED. The Play Store Termux has the boot receiver
// built in; F-Droid and GitHub builds need the Termux:Boot app (opened once).
const bootScript = "#!/data/data/com.termux/files/usr/bin/sh\n" +
	"termux-wake-lock\n" +
	"exec pipedpeer start\n"

// playVersionPrefix is the start of TERMUX_VERSION on the Play
// Store build (for example "googleplay.2026.10.09").
const playVersionPrefix = "googleplay."

// bootPath is the boot script's path under the Termux home.
func bootPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/data/data/com.termux/files/home"
	}
	return filepath.Join(home, ".termux", "boot", "pipedpeer")
}

// writeBoot installs the boot script. It is rewritten only when the content
// or mode differs, so a second run prints "already". The path is recorded so
// uninstall removes it (its Paths loop). appVersion is TERMUX_VERSION;
// a non-Play build gets a note about Termux:Boot.
func writeBoot(w io.Writer, appVersion string) error {
	p := bootPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("boot: cannot create %s: %w", filepath.Dir(p), err)
	}
	fi, statErr := os.Stat(p)
	switch {
	case statErr == nil:
		have, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("boot: cannot read %s: %w", p, err)
		}
		ours := slices.Contains(recorded().Paths, p)
		same := bytes.Equal(have, []byte(bootScript))
		switch {
		case !same && !ours:
			// The user's own script: not ours to replace or to delete later.
			log.Warn().Str("path", p).Msg("phone: boot script exists with other content; left alone")
			term.Warn(w, "start on boot: %s exists with other content and is left as it is; add `exec pipedpeer start` to it if you want pipedpeer to start at boot", p)
			return nil
		case same && fi.Mode().Perm() == 0o755:
			term.OK(w, "start on boot: already set (%s)", p)
			bootNote(w, appVersion)
			return nil
		}
	case !os.IsNotExist(statErr):
		return fmt.Errorf("boot: cannot stat %s: %w", p, statErr)
	}

	if !errors.Is(statErr, os.ErrNotExist) && !slices.Contains(recorded().Paths, p) {
		// Same content, wrong mode, not recorded: fix the mode, own nothing.
		if err := os.Chmod(p, 0o755); err != nil {
			return fmt.Errorf("boot: cannot set mode on %s: %w", p, err)
		}
		term.OK(w, "start on boot: already set (%s)", p)
		bootNote(w, appVersion)
		return nil
	}
	record(installed{Paths: []string{p}})
	if err := os.WriteFile(p, []byte(bootScript), 0o755); err != nil {
		return fmt.Errorf("boot: cannot write %s: %w", p, err)
	}
	// WriteFile keeps the mode of an existing file, so set it explicitly.
	if err := os.Chmod(p, 0o755); err != nil {
		return fmt.Errorf("boot: cannot set mode on %s: %w", p, err)
	}
	term.OK(w, "start on boot: %s written", p)
	bootNote(w, appVersion)
	return nil
}

// bootNote tells a non-Play user to install Termux:Boot. It is not an error:
// the script is harmless until that app runs it.
func bootNote(w io.Writer, appVersion string) {
	if !strings.HasPrefix(appVersion, playVersionPrefix) {
		term.Action(w, "install Termux:Boot (F-Droid) and open it once, so the start-on-boot script runs")
	}
}

// recorded is what setup has recorded as its own.
func recorded() installed {
	r, _ := loadRecord()
	return r
}
