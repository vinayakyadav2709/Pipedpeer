package ping

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type health struct {
	Status       string `json:"status"`
	NodeID       string `json:"node_id"`
	ActiveJobs   int    `json:"active_jobs"`
	AvailableMem int64  `json:"available_mem"`
	ReservedMem  int64  `json:"reserved_mem"`
	ActiveLeases int    `json:"active_leases"`
}

type target struct {
	host string
	port string
	addr string
}

func parseTargets(args []string) []target {
	var targets []target
	for _, a := range args {
		parts := strings.SplitN(a, ":", 2)
		host := parts[0]
		port := "38080"
		if len(parts) == 2 {
			port = parts[1]
		}
		targets = append(targets, target{
			host: host,
			port: port,
			addr: fmt.Sprintf("%s:%s", host, port),
		})
	}
	return targets
}

func formatBytes(b int64) string {
	if b == 0 {
		return "-"
	}
	gb := float64(b) / (1 << 30)
	return fmt.Sprintf("%.1f GiB", gb)
}

func poll(addr string) (*health, error) {
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var h health
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return nil, err
	}
	return &h, nil
}

func Run(args []string, interval time.Duration) error {
	targets := parseTargets(args)
	if len(targets) == 0 {
		return fmt.Errorf("no targets specified. Usage: pipedpeer ping <host:port> [host:port ...]")
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fmt.Print("\033[2J\033[H") // Clear + home

	for {
		printTable(targets)
		fmt.Printf("\n  Refreshing every %s | Ctrl+C to stop\n", interval)

		select {
		case <-sig:
			fmt.Println()
			return nil
		case <-ticker.C:
			fmt.Print("\033[H") // Cursor home
		}
	}
}

func printTable(targets []target) {
	fmt.Println("  PIPEDPEER WORKER STATUS")
	fmt.Println("  " + strings.Repeat("─", 72))
	fmt.Printf("  %-5s %-22s %-7s %-8s %-10s %s\n",
		"PORT", "NODE_ID", "JOBS", "RES_MEM", "AVAIL_MEM", "STATE")

	for _, t := range targets {
		h, err := poll(t.addr)
		if err != nil {
			fmt.Printf("  %-5s %-22s %-7s %-8s %-10s \033[31m%-6s\033[0m\n",
				t.port, "-", "-", "-", "-", "DOWN")
			continue
		}
		stateIcon := "OK"
		stateColor := "\033[32m"
		if h.Status != "ok" {
			stateIcon = h.Status
			stateColor = "\033[33m"
		}
		shortID := h.NodeID
		if len(shortID) > 20 {
			shortID = shortID[:20]
		}
		fmt.Printf("  %-5s %-22s %-7d %-8s %-10s %s%s\033[0m\n",
			t.port, shortID, h.ActiveJobs,
			formatBytes(h.ReservedMem), formatBytes(h.AvailableMem),
			stateColor, stateIcon)
	}
}
