package phone

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/term"
)

// bootScript is ~/.termux/boot/pipedpeer: Termux runs every script in that
// directory on BOOT_COMPLETED. The Play Store Termux has the boot receiver
// built in; F-Droid and GitHub builds need the Termux:Boot app (opened once).
const bootScript = "#!/data/data/com.termux/files/usr/bin/sh\n" +
	"termux-wake-lock\n" +
	"exec pipedpeer start\n"

// playVersionPrefix is the start of TERMUX_APP__APP_VERSION_NAME on the Play
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
// uninstall removes it (its Paths loop). appVersion is TERMUX_APP__APP_VERSION_NAME;
// a non-Play build gets a note about Termux:Boot.
func writeBoot(w io.Writer, appVersion string) error {
	p := bootPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("boot: cannot create %s: %w", filepath.Dir(p), err)
	}
	record(installed{Paths: []string{p}})

	fi, statErr := os.Stat(p)
	switch {
	case statErr == nil:
		have, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("boot: cannot read %s: %w", p, err)
		}
		if bytes.Equal(have, []byte(bootScript)) && fi.Mode().Perm() == 0o755 {
			term.OK(w, "start on boot: already set (%s)", p)
			bootNote(w, appVersion)
			return nil
		}
	case !os.IsNotExist(statErr):
		return fmt.Errorf("boot: cannot stat %s: %w", p, statErr)
	}

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
