package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/clusterstats"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

// toStatsNodes adapts the daemon's node list to what clusterstats asks.
func toStatsNodes(nodes []clusterNode) []clusterstats.Node {
	out := make([]clusterstats.Node, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, clusterstats.NodeFromAPI(n.NodeID, n.SSHEndpoint, n.DaemonPort,
			n.State, n.Source, n.Capabilities, n.Load))
	}
	return out
}

func newStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "One view of the cluster: nodes, their load and pool work, and recent runs",
		RunE: func(cmd *cobra.Command, args []string) error {
			daemonPort, _ := cmd.Flags().GetInt("port")
			limit, _ := cmd.Flags().GetInt("jobs")
			asJSON, _ := cmd.Flags().GetBool("json")

			nodes, err := fetchClusterNodes(daemonPort)
			if err != nil {
				return fmt.Errorf("%w (start it with: pipedpeer start)", err)
			}
			snap := clusterstats.Collect(toStatsNodes(nodes), limit, nil)
			if asJSON {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(snap)
			}
			printStats(snap)
			return nil
		},
	}
	cmd.Flags().Int("port", 38080, "Local daemon port")
	cmd.Flags().Int("jobs", 5, "Recent runs to show")
	cmd.Flags().Bool("json", false, "Print the snapshot as JSON")
	return cmd
}

func printStats(s clusterstats.Snapshot) {
	healthy, cores, gpus, leases := 0, 0, 0, 0
	var freeMem int64
	for _, n := range s.Nodes {
		if n.State == "healthy" {
			healthy++
			cores += int(atoiOr(n.Capabilities["cpu_cores"], 0))
			freeMem += n.Load.AvailableMemBytes
			if n.Capabilities["gpu_name"] != "" {
				gpus++
			}
		}
		leases += len(n.Leases)
	}
	fmt.Printf("Cluster: %d node(s), %d healthy · %d cores · %s free memory · %d GPU node(s) · %d task(s) running\n\n",
		len(s.Nodes), healthy, cores, resourceest.FormatBytes(freeMem), gpus, leases)

	fmt.Printf("%-10s %-14s %-11s %5s %5s %10s %4s   %8s %8s %9s %8s %6s\n",
		"NODE", "HOST", "STATE", "CORES", "CPU%", "MEM FREE", "JOBS",
		"CHUNKS", "FROM PEER", "ITEMS RUN", "TO PEERS", "FAILED")
	for _, n := range s.Nodes {
		state := n.State
		if n.Source == "self" {
			state += "*"
		}
		pool := fmt.Sprintf("%8s %8s %9s %8s %6s", "-", "-", "-", "-", "-")
		if n.Pool != nil {
			p := n.Pool
			pool = fmt.Sprintf("%8d %8d %9d %8d %6d", p.ChunksReceived, p.ChunksPeer, p.ItemsExecuted, p.PartsToPeers, p.PartsFailed)
		} else if n.State == "healthy" {
			pool = "  (did not answer)"
		}
		cores := n.Capabilities["cpu_cores"]
		if cores == "" {
			cores = "-"
		}
		fmt.Printf("%-10s %-14s %-11s %5s %5.0f %10s %4d   %s\n",
			shortID(n.NodeID), truncate(n.Hostname, 14), state, cores, n.Load.CPUPercent,
			resourceest.FormatBytes(n.Load.AvailableMemBytes), len(n.Leases), pool)
	}
	fmt.Println("(* this machine; pool counters are since each daemon started)")

	if len(s.Jobs) == 0 {
		return
	}
	fmt.Printf("\nRecent runs from this machine:\n")
	fmt.Printf("%-20s %-18s %-10s %-10s %8s  %-22s %s\n", "ID", "SCRIPT", "STATUS", "NODE", "TOTAL", "SLOWEST STAGE", "DISTRIBUTED")
	for _, j := range s.Jobs {
		node := j.RunHost
		if j.TargetID != "" {
			node = shortID(j.TargetID)
		}
		slowest := "-"
		if st, ok := slowestStage(j.Stages); ok {
			slowest = fmt.Sprintf("%s %s", st.Name, (time.Duration(st.Ms) * time.Millisecond).Round(100*time.Millisecond))
		}
		dist := "-"
		if j.Receipt != nil && j.Receipt.Total() > 0 {
			dist = fmt.Sprintf("%d/%d items remote", j.Receipt.FromElsewhere(), j.Receipt.Total())
		}
		fmt.Printf("%-20s %-18s %-10s %-10s %8s  %-22s %s\n", j.ID, truncate(j.Script, 18), j.Status,
			truncate(node, 10), (time.Duration(j.TotalMs) * time.Millisecond).Round(100*time.Millisecond),
			truncate(slowest, 22), dist)
	}
	fmt.Println("\nDetail: pipedpeer job --id <ID> --timeline --explain --receipt   ·   live page: pipedpeer ui")
}

func slowestStage(stages []jobhistory.Stage) (jobhistory.Stage, bool) {
	var best jobhistory.Stage
	ok := false
	for _, s := range stages {
		if !ok || s.Ms > best.Ms {
			best, ok = s, true
		}
	}
	return best, ok
}

func atoiOr(s string, def int64) int64 {
	var v int64
	if _, err := fmt.Sscan(strings.TrimSpace(s), &v); err != nil {
		return def
	}
	return v
}
