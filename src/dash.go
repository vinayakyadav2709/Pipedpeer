package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

type dashModel struct {
	daemonPort int
	nodeID     identity.NodeIdentity
	interval   time.Duration
	width      int
	height     int
	peers      []clusterNode
	events     []clusterEvent
	jobs       []jobhistory.JobSummary
	live       []clusterTask
	peerErr    string
	jobsErr    string
	eventsErr  string
	running    bool
}

var (
	headerStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	greenStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	redStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	yellowStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	dimStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	whiteStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("7"))
	titleStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6")).Padding(0, 1)
	sectionStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6")).MarginTop(1)
)

func (m dashModel) Init() tea.Cmd {
	return tea.Tick(m.interval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

type tickMsg time.Time

func (m dashModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}

	case tickMsg:
		m.refresh()
		return m, tea.Tick(m.interval, func(t time.Time) tea.Msg {
			return tickMsg(t)
		})
	}

	return m, nil
}

func (m *dashModel) refresh() {
	// Fetch peers from daemon
	peers, err := fetchClusterNodes(m.daemonPort)
	if err != nil {
		m.peerErr = "daemon unreachable"
	} else {
		m.peerErr = ""
		m.peers = peers
	}

	// Recent daemon events; a missing log just means nothing to show yet.
	if evs, err := readEvents(daemonctl.DefaultPaths().LogFile, 5, false); err != nil {
		m.eventsErr = err.Error()
	} else {
		m.eventsErr = ""
		names := nodeNames(m.daemonPort)
		m.events = nil
		for _, e := range evs {
			m.events = append(m.events, e.withNames(names))
		}
	}

	// Fetch job history (local, completed + in-flight)
	jobs, err := jobhistory.List(10)
	if err != nil {
		m.jobsErr = err.Error()
	} else {
		m.jobsErr = ""
		m.jobs = jobs
	}

	// Fetch live leases from every healthy node, so the table shows tasks this
	// machine submitted elsewhere as well as ones running here.
	if live, err := fetchClusterTasks(m.daemonPort); err == nil {
		m.live = live
	}
}

func (m dashModel) View() string {
	var s string

	// ── Header
	status := greenStyle.Render("● RUNNING")
	if !m.running {
		status = redStyle.Render("● STOPPED")
	}
	header := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder(), false, false, true, false).
		BorderForeground(lipgloss.Color("8")).
		Padding(0, 1).
		Render(
			titleStyle.Render("Pipedpeer") +
				dimStyle.Render(fmt.Sprintf(" port %d  node %s ", m.daemonPort, m.nodeID.ShortID())) +
				status,
		)
	s += header + "\n"

	// ── Workers table
	s += sectionStyle.Render("WORKERS") + "\n"

	tw := m.tableWidth()
	peerRows := [][]string{}
	for _, p := range m.peers {
		node := p.Capabilities["hostname"]
		if node == "" {
			node = shortID(p.NodeID)
		}
		if p.Source == "self" {
			node += "*"
		}

		statusCol := greenStyle
		if p.State != "healthy" {
			statusCol = redStyle
		}

		memStr := resourceest.FormatBytes(p.Load.AvailableMemBytes)
		if p.Load.AvailableMemBytes == 0 && p.State != "healthy" {
			memStr = "-"
		}

		jobsStr := fmt.Sprintf("%d", p.Load.ActiveJobs)
		if p.Load.ActiveJobs > 0 {
			jobsStr = yellowStyle.Render(jobsStr)
		}

		srcDisplay := p.Source
		if srcDisplay == "discovery" {
			// UDP broadcast, not mDNS: an operator debugging why a peer is
			// not found should look at whether the access point drops
			// broadcast, not at Avahi.
			srcDisplay = "broadcast"
		}

		peerRows = append(peerRows, []string{
			node,
			statusCol.Render(p.State),
			jobsStr,
			fmt.Sprintf("%.0f%%", p.Load.CPUPercent),
			memStr,
			dashGPUCell(p),
			srcDisplay,
		})
	}
	s += renderTable(
		[]string{"NODE", "STATUS", "JOBS", "CPU", "MEM FREE", "GPU", "SOURCE"},
		peerRows,
		[]float64{0.18, 0.11, 0.07, 0.07, 0.12, 0.27, 0.12},
		tw,
	)

	healthy, jobs, memAvail := 0, 0, int64(0)
	var cpuSum float64
	for _, p := range m.peers {
		if p.State == "healthy" {
			healthy++
		}
		jobs += p.Load.ActiveJobs
		memAvail += p.Load.AvailableMemBytes
		cpuSum += p.Load.CPUPercent
	}
	avgCPU := 0.0
	if len(m.peers) > 0 {
		avgCPU = cpuSum / float64(len(m.peers))
	}
	s += dimStyle.Render(fmt.Sprintf("  workers %d/%d  jobs %d  mem avail %s  cpu avg %.0f%%",
		healthy, len(m.peers), jobs, resourceest.FormatBytes(memAvail), avgCPU)) + "\n"

	// ── Tasks table: live cluster leases first, then local history.
	s += sectionStyle.Render("RECENT TASKS") + "\n"

	taskRows := [][]string{}
	seenLease := make(map[string]bool)
	for _, t := range m.live {
		seenLease[t.LeaseID] = true
		name := t.JobName
		if name == "" {
			name = "(unnamed)"
		}
		if len(name) > 18 {
			name = name[:15] + "..."
		}
		stage := string(t.State)
		stageCol := yellowStyle
		if t.State == "reserved" {
			stageCol = whiteStyle
		}
		node := t.Hostname
		if len(node) > 12 {
			node = node[:12]
		}
		taskRows = append(taskRows, []string{
			shortID(t.LeaseID),
			name,
			node,
			stageCol.Render(stage),
			t.Age.Truncate(time.Second).String(),
		})
	}

	for _, j := range m.jobs {
		shortID := j.ID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		scriptName := filepath.Base(j.ScriptPath)
		if len(scriptName) > 18 {
			scriptName = scriptName[:15] + "..."
		}
		shortTarget := j.TargetID
		if len(shortTarget) > 8 {
			shortTarget = shortTarget[:8]
		}
		if shortTarget == "" {
			shortTarget = "-"
		}

		stage := j.Stage
		var stageCol lipgloss.Style
		if j.Status == "running" {
			if stage == "" {
				stage = "running"
			}
			stageCol = yellowStyle
		} else if j.Status == "failed" {
			stage = "failed"
			stageCol = redStyle
		} else {
			stage = "done"
			stageCol = greenStyle
		}

		dur := "-"
		if j.DurationMs > 0 {
			dur = fmt.Sprintf("%.1fs", float64(j.DurationMs)/1000.0)
		}

		taskRows = append(taskRows, []string{
			shortID,
			scriptName,
			shortTarget,
			stageCol.Render(stage),
			dur,
		})
	}
	s += renderTable(
		[]string{"ID", "SCRIPT/JOB", "NODE", "STAGE", "TIME"},
		taskRows,
		[]float64{0.12, 0.28, 0.14, 0.13, 0.10},
		tw,
	)

	// ── Events: what the daemon has been doing, newest last.
	s += "\n" + sectionStyle.Render("EVENTS") + "\n"
	if m.eventsErr != "" {
		s += dimStyle.Render("  (no daemon log yet: "+m.eventsErr+")") + "\n"
	}
	for _, e := range m.events {
		s += "  " + e.String() + "\n"
	}
	if n := len(m.events); n > 0 {
		last := m.events[n-1]
		line := "last: " + last.Text
		if !last.At.IsZero() {
			age := time.Since(last.At).Truncate(time.Second)
			line += fmt.Sprintf(" · %s ago", age)
			if age < 15*time.Second {
				line = yellowStyle.Render(line)
			}
		}
		s += "  " + line + "\n"
	}

	s += "\n" + dimStyle.Render(" q / Ctrl+C to quit") + "\n"
	return s
}

func (m dashModel) tableWidth() int {
	if w := m.width - 2; w > 78 {
		return w
	}
	return 78
}

// dashGPUCell is "3060 12%" from the node's load, or "-" without a GPU.
func dashGPUCell(n clusterNode) string {
	name := n.Load.GPUModel
	if name == "" {
		return "-"
	}
	for _, w := range strings.Fields(name) {
		if len(w) >= 3 && w[0] >= '0' && w[0] <= '9' {
			name = w
			break
		}
	}
	return fmt.Sprintf("%s %.0f%%", name, n.Load.GPUUtilPercent)
}

func renderTable(headers []string, rows [][]string, widths []float64, totalWidth int) string {
	// Header
	headerCells := make([]string, len(headers))
	for i, h := range headers {
		w := int(float64(totalWidth) * widths[i])
		headerCells[i] = headerStyle.Width(w).Render(padRight(h, w))
	}
	headerLine := lipgloss.JoinHorizontal(lipgloss.Left, headerCells...)

	// Separator
	sep := dimStyle.Render("─" + lipgloss.NewStyle().Width(totalWidth-2).Render(strings.Repeat("─", totalWidth-2)) + "─")

	// Rows
	var rowLines []string
	for _, row := range rows {
		cells := make([]string, len(row))
		for i, cell := range row {
			w := int(float64(totalWidth) * widths[i])
			cells[i] = dimStyle.Width(w).Render(padRight(cell, w))
		}
		rowLines = append(rowLines, lipgloss.JoinHorizontal(lipgloss.Left, cells...))
	}

	var result string
	result += headerLine + "\n"
	result += sep + "\n"
	for _, r := range rowLines {
		result += r + "\n"
	}
	result += dimStyle.Render("─" + lipgloss.NewStyle().Width(totalWidth-2).Render(strings.Repeat("─", totalWidth-2)) + "─")

	return result
}

func padRight(s string, width int) string {
	if lipgloss.Width(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-lipgloss.Width(s))
}
