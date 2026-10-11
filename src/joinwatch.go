package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// waitForPeers shows the join live (C-JOIN): daemon events as they happen and,
// on a terminal, a countdown line. It returns the number of healthy peers
// other than this node, or 0 when wait runs out.
func waitForPeers(daemonPort int, wait time.Duration) int {
	fi, err := os.Stdout.Stat()
	tty := err == nil && fi.Mode()&os.ModeCharDevice != 0
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not tell whether stdout is a terminal (%v); printing plain lines\n", err)
	}

	var mu sync.Mutex
	status := ""
	// say prints a line above the status line and redraws the status after it.
	say := func(line string) {
		mu.Lock()
		defer mu.Unlock()
		if tty {
			fmt.Print("\r\033[K")
		}
		fmt.Println(line)
		if tty && status != "" {
			fmt.Print(status)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	names := nodeNames(daemonPort)
	var namesMu sync.Mutex
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				n := nodeNames(daemonPort)
				namesMu.Lock()
				names = n
				namesMu.Unlock()
			}
		}
	}()
	go func() {
		err := followEvents(ctx, daemonctl.DefaultPaths().LogFile, false, func(e clusterEvent) {
			switch e.Kind {
			case "start", "join", "leave", "link", "nopath", "trust":
				namesMu.Lock()
				n := names
				namesMu.Unlock()
				say(e.withNames(n).String())
			}
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not follow the daemon log (%v); counting peers only\n", err)
		}
	}()

	start := time.Now()
	deadline := start.Add(wait)
	lastPlain := time.Time{}
	for {
		if n := healthyPeerCount(); n > 0 {
			mu.Lock()
			status = ""
			if tty {
				fmt.Print("\r\033[K")
			}
			mu.Unlock()
			fmt.Printf("connected (%d peer(s))\n", n)
			return n
		}
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		line := fmt.Sprintf("  waiting for peers · %d s left · 0 found", int(left.Seconds()+0.5))
		mu.Lock()
		if tty {
			status = line
			fmt.Print("\r\033[K" + line)
		} else if time.Since(lastPlain) >= 10*time.Second {
			lastPlain = time.Now()
			fmt.Println(line)
		}
		mu.Unlock()
		time.Sleep(time.Second)
	}

	mu.Lock()
	status = ""
	if tty {
		fmt.Print("\r\033[K")
	}
	mu.Unlock()
	fmt.Println("no peers found")
	printUnreachable(os.Stdout, daemonPort)
	host := clustercfg.Rendezvous()
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	fmt.Printf("  same network? ping %s\n", host)
	fmt.Println("  firewall: sudo firewall-cmd --list-ports (Fedora) / sudo ufw status (Ubuntu)")
	fmt.Println("  details: pipedpeer net-check")
	return 0
}
