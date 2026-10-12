package daemonctl

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
)

// stdoutIsTerminal decides whether a live bar is drawn. Piped or logged
// output gets one summary line instead of hundreds of redraws.
func stdoutIsTerminal() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// withProgress wraps r in a byte progress bar when stdout is a terminal.
// The returned finish func clears the bar; call it once the read is done.
func withProgress(r io.Reader, size int64, desc string) (io.Reader, func()) {
	if !stdoutIsTerminal() || size <= 0 {
		return r, func() {}
	}
	bar := progressbar.NewOptions64(size,
		progressbar.OptionSetWriter(os.Stdout),
		progressbar.OptionSetDescription(desc),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowTotalBytes(true),
		progressbar.OptionSetWidth(24),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionSetRenderBlankState(true),
	)
	pr := progressbar.NewReader(r, bar)
	return &pr, func() {
		if err := bar.Finish(); err != nil {
			fmt.Fprintf(os.Stderr, "      warning: progress bar: %v\n", err)
		}
	}
}

// fileSize is the size of a file for progress and the run summary; 0 (with a
// warning) when it cannot be read, which only costs the bar its total.
func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "      warning: cannot size %s: %v\n", path, err)
		return 0
	}
	return fi.Size()
}

// rate renders bytes over a duration for the transfer lines.
func rate(n int64, d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	return humanize.Bytes(uint64(float64(n)/d.Seconds())) + "/s"
}

func secs(d time.Duration) string { return fmt.Sprintf("%.1f s", d.Seconds()) }
