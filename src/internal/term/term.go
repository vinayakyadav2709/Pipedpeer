// Package term prints status lines for setup, connect and doctor: a mark,
// a colour on a terminal, and an explicit "ACTION NEEDED" line for anything
// the user has to do. No colour when the writer is not a terminal or
// NO_COLOR is set (lipgloss/termenv decide).
package term

import (
	"fmt"
	"io"

	"github.com/charmbracelet/lipgloss"
)

func line(w io.Writer, color, mark string, bold bool, format string, a ...any) {
	r := lipgloss.NewRenderer(w)
	st := r.NewStyle().Foreground(lipgloss.Color(color)).Bold(bold)
	fmt.Fprintln(w, st.Render(mark+fmt.Sprintf(format, a...)))
}

// OK prints a done step: "✓ msg" in green.
func OK(w io.Writer, format string, a ...any) { line(w, "2", "✓ ", false, format, a...) }

// Warn prints something that works but is worse than it should be: "! msg" in yellow.
func Warn(w io.Writer, format string, a ...any) { line(w, "3", "! ", false, format, a...) }

// Fail prints a failed step: "✗ msg" in red.
func Fail(w io.Writer, format string, a ...any) { line(w, "1", "✗ ", false, format, a...) }

// Action prints a step the user must do: "ACTION NEEDED: msg" in bold yellow.
func Action(w io.Writer, format string, a ...any) {
	line(w, "3", "ACTION NEEDED: ", true, format, a...)
}
