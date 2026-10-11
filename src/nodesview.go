package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/dustin/go-humanize"

	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

// printNodeTable renders the nodes with live CPU and GPU load (C-NODES).
func printNodeTable(w io.Writer, nodes []clusterNode) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tHOST\tSTATE\tJOBS\tCPU\tMEM FREE\tGPU\tPATH\tSOURCE")
	for _, n := range nodes {
		id := shortID(n.NodeID)
		if n.Source == "self" {
			id += "*"
		}
		host := n.Capabilities["hostname"]
		if host == "" {
			host = hostFromEndpoint(n.SSHEndpoint)
		}
		path := n.Capabilities[daemonapi.PathCapability]
		if path == "" {
			path = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			id, truncate(host, 14), n.State, n.Load.ActiveJobs, nodeCPUCell(n),
			resourceest.FormatBytes(n.Load.AvailableMemBytes), nodeGPUCell(n), path, n.Source)
	}
	if err := tw.Flush(); err != nil {
		fmt.Fprintf(w, "could not format the node table: %v\n", err)
	}
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
