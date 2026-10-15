package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/dustin/go-humanize"

	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

// printNodeTable renders the nodes with live CPU and GPU load. A name
// shared by several machines (cloned lab PCs) gets its short id appended.
func printNodeTable(w io.Writer, nodes []clusterNode) {
	seen := map[string]int{}
	for _, n := range nodes {
		seen[nodeName(n)]++
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSTATE\tPATH\tCPU\tMEM FREE\tGPU\tJOBS\tNOTE")
	for _, n := range nodes {
		name := nodeName(n)
		if seen[name] > 1 {
			name += " (" + shortID(n.NodeID) + ")"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
			name, n.State, nodePath(n), nodeCPUCell(n),
			resourceest.FormatBytes(n.Load.AvailableMemBytes), nodeGPUCell(n), n.Load.ActiveJobs, nodeNote(n))
	}
	if err := tw.Flush(); err != nil {
		fmt.Fprintf(w, "could not format the node table: %v\n", err)
	}
}

func nodeName(n clusterNode) string {
	if h := n.Capabilities["hostname"]; h != "" {
		return truncate(h, 24)
	}
	return n.address()
}

// nodePath is how the node is reached: lan, internet or self.
func nodePath(n clusterNode) string {
	switch {
	case n.Path != "":
		return n.Path
	case n.Source == "self":
		return "self"
	case n.Capabilities[daemonapi.PathCapability] != "":
		return n.Capabilities[daemonapi.PathCapability]
	case n.Source != "":
		return n.Source
	}
	return "-"
}

// nodeNote is why a row is not healthy and whether it runs another build.
func nodeNote(n clusterNode) string {
	var notes []string
	if n.Reason != "" && n.State != "healthy" {
		notes = append(notes, n.Reason)
	}
	if v := n.Capabilities["version"]; v != "" && v != version {
		notes = append(notes, "other version "+v)
	}
	if len(notes) == 0 {
		return "-"
	}
	return strings.Join(notes, "; ")
}

func nodeCPUCell(n clusterNode) string {
	cores := n.Load.TotalCPUs
	if cores == 0 {
		return fmt.Sprintf("%.0f%%", n.Load.CPUPercent)
	}
	return fmt.Sprintf("%dc %.0f%%", cores, n.Load.CPUPercent)
}

// nodeGPUCell is "RTX 3060 12% 1.2/6.0G", or the capability name alone, or "-".
func nodeGPUCell(n clusterNode) string {
	name := n.Load.GPUModel
	if name == "" {
		name = n.Capabilities["gpu_name"]
	}
	if name == "" {
		return "-"
	}
	name = strings.TrimSpace(strings.NewReplacer("NVIDIA ", "", "GeForce ", "", "Laptop GPU", "").Replace(name))
	cell := truncate(name, 14)
	if n.Load.GPUMemBytes > 0 {
		cell += fmt.Sprintf(" %.0f%% %s/%s", n.Load.GPUUtilPercent,
			gb(n.Load.GPUMemUsedBytes), gb(n.Load.GPUMemBytes))
	}
	return cell
}

func gb(b int64) string {
	return strings.ReplaceAll(strings.ReplaceAll(humanize.IBytes(uint64(b)), " GiB", "G"), " MiB", "M")
}

// printNodes renders the daemon's view of the cluster and the peers it has
// no direct path to.
func printNodes(daemonPort int) error {
	nodes, err := fetchClusterNodes(daemonPort)
	if err != nil {
		fmt.Println("No machines found (daemon unreachable; start it with: pipedpeer start)")
		return nil
	}
	if len(nodes) == 0 {
		fmt.Println("No machines found")
	} else {
		printNodeTable(os.Stdout, nodes)
	}
	printUnreachable(os.Stdout, daemonPort)
	return nil
}

// printUnreachable lists peers with no direct path; a fetch error goes to stderr.
func printUnreachable(w io.Writer, daemonPort int) {
	bad, err := fetchUnreachable(daemonPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not list peers with no direct path: %v\n", err)
		return
	}
	if len(bad) == 0 {
		return
	}
	fmt.Fprintln(w, "\nPeers with no direct path (not offered work):")
	for _, p := range bad {
		fmt.Fprintf(w, "  %-10s %s\n", shortID(p.Peer), p.Reason)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
