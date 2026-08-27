package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

type peerInfo struct {
	NodeID      string `json:"node_id"`
	SSHEndpoint string `json:"ssh_endpoint"`
	DaemonPort  int    `json:"daemon_port"`
	State       string `json:"state"`
	Load        struct {
		ActiveJobs        int     `json:"active_jobs"`
		AvailableMemBytes int64   `json:"available_mem_bytes"`
		CPUPercent        float64 `json:"cpu_percent"`
	} `json:"load"`
	Source string `json:"source"`
}

func (p peerInfo) ActiveJobs() int     { return p.Load.ActiveJobs }
func (p peerInfo) AvailableMem() int64 { return p.Load.AvailableMemBytes }

type dashModel struct {
	daemonPort int
	nodeID     identity.NodeIdentity
	interval   time.Duration
	width      int
	height     int
	peers      []peerInfo
	jobs       []jobhistory.JobSummary
	live       []clusterTask
	peerErr    string
	jobsErr    string
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
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", m.daemonPort))
	if err != nil {
		m.peerErr = "daemon unreachable"
	} else {
		defer resp.Body.Close()
		m.peerErr = ""
		if err := json.NewDecoder(resp.Body).Decode(&m.peers); err != nil {
			m.peerErr = err.Error()
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

	peerRows := [][]string{}
	for _, p := range m.peers {
		shortID := p.NodeID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		if p.NodeID == "" {
			shortID = "?"
		}

		host := p.SSHEndpoint
		if idx := strings.Index(host, "@"); idx != -1 {
			host = host[idx+1:]
		}
		if idx := strings.LastIndex(host, ":"); idx != -1 {
			host = host[:idx]
		}

		status := p.State
		statusCol := greenStyle
		if status != "healthy" {
			statusCol = redStyle
		}

		memStr := resourceest.FormatBytes(p.AvailableMem())
		if p.AvailableMem() == 0 && p.State != "healthy" {
			memStr = "-"
		}

		jobsStr := fmt.Sprintf("%d", p.ActiveJobs())
		if p.ActiveJobs() > 0 {
			jobsStr = yellowStyle.Render(jobsStr)
		}

		srcDisplay := p.Source
		if srcDisplay == "discovery" {
			// UDP broadcast, not mDNS. The label said mDNS since before the
			// mechanism was chosen, and an operator debugging why a peer is
			// not found goes looking at Avahi rather than at whether their
			// access point drops broadcast - which is the thing that actually
			// breaks it.
			srcDisplay = "broadcast"
		}

		peerRows = append(peerRows, []string{
			shortID,
			fmt.Sprintf("%s:%d", host, p.DaemonPort),
			statusCol.Render(status),
			jobsStr,
			memStr,
			srcDisplay,
		})
	}
	s += renderTable(
		[]string{"ID", "HOST:PORT", "STATUS", "JOBS", "MEM AVAIL", "SOURCE"},
		peerRows,
		[]float64{0.12, 0.24, 0.12, 0.07, 0.13, 0.10},
	)

	healthy, jobs, memAvail := 0, 0, int64(0)
	var cpuSum float64
	for _, p := range m.peers {
		if p.State == "healthy" {
			healthy++
		}
		jobs += p.ActiveJobs()
		memAvail += p.AvailableMem()
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
	)

	s += "\n" + dimStyle.Render(" q / Ctrl+C to quit") + "\n"
	return s
}

func renderTable(headers []string, rows [][]string, widths []float64) string {
	totalWidth := 78

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
