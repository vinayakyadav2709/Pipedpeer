package main

import (
	"io"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// V1 rehearsal: Ctrl+C delivered as SIGINT (not a key press) made the
// dashboard exit with "Error: program was interrupted".
func TestDashboardInterruptIsCleanExit(t *testing.T) {
	m := dashModel{interval: time.Hour}
	p := tea.NewProgram(m, tea.WithInput(nil), tea.WithOutput(io.Discard), tea.WithoutSignalHandler())
	go p.Send(tea.Interrupt())
	if err := runDashboard(p); err != nil {
		t.Fatalf("interrupt: %v", err)
	}
}
