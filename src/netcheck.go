package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nattype"
	"github.com/spf13/cobra"
)

// newNetCheckCmd reports whether this machine can be reached directly from
// another network, which decides how internet mode has to work here.
//
// The answer is not a property of pipedpeer, it is a property of the router in
// front of it, and it differs per network. Measuring it beats assuming it:
// building hole punching for networks that refuse it wastes the effort, and
// building only a relay for networks that would allow direct connections
// wastes the bandwidth - on a public server that is a bill.
func newNetCheckCmd() *cobra.Command {
	var servers []string
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "net-check",
		Short: "Report whether this machine can accept direct connections from the internet",
		Long: "Sends probes from one local port to two addresses on a public reflector and " +
			"compares the source address each of them saw. Run `pipedpeer rendezvous` on a " +
			"machine with a public address to provide the reflector.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(servers) == 0 {
				servers = nattype.DefaultSTUNServers
			}
			if len(servers) < 2 {
				return fmt.Errorf("need two --server addresses on the public side; " +
					"the test is a comparison, and with one there is nothing to compare")
			}
			fmt.Printf("Probing %s ...\n", strings.Join(servers, ", "))
			res, err := nattype.Probe(cmd.Context(), servers, timeout)
			if err != nil {
				return err
			}
			switch {
			case res.Blocked:
				fmt.Println("\n  No reflector answered.")
				fmt.Println("  Outbound UDP appears to be blocked here, which rules out every")
				fmt.Println("  direct transport. Check that the reflector is running and that")
				fmt.Println("  its port is open, then that this network permits outbound UDP.")
			case res.Mapping == nattype.EndpointIndependent:
				fmt.Printf("\n  Reachable at %s\n", res.External)
				fmt.Println("  This router presents the same address to every destination, so a")
				fmt.Println("  peer can be told where to send and the two can connect directly.")
				fmt.Println("  No relay needed.")
			default:
				fmt.Printf("\n  Not directly reachable (seen as %s)\n",
					strings.Join(res.Seen, ", "))
				fmt.Println("  This router allocates a different address per destination, so the")
				fmt.Println("  address a peer would be given was never valid for that peer.")
				fmt.Println("  Traffic to this machine has to be relayed.")
			}
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&servers, "server", nil,
		"reflector or STUN addresses, host:port (default: two public STUN servers)")
	cmd.Flags().DurationVar(&timeout, "timeout", 3*time.Second, "per-probe timeout")
	return cmd
}

// newNetPunchCmd tries to open a direct path to a peer across two home
// routers, which is the question internet mode turns on: if this works, the
// relay is a fallback rather than the design.
func newNetPunchCmd() *cobra.Command {
	var peer string
	var localPort int
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "net-punch",
		Short: "Open a direct connection to a peer behind another router",
		Long: "Learns this machine's external address for a fixed local port and prints it, " +
			"then, given the peer's, sends and listens until something gets through. Both " +
			"sides must run at roughly the same time: each one's outbound packets are what " +
			"lets its router accept the other's.",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, ext, err := nattype.BindAndDiscover(cmd.Context(), localPort, nil, 3*time.Second)
			if err != nil {
				return err
			}
			defer conn.Close()
			fmt.Printf("local port %d is seen from outside as %s\n", localPort, ext)
			if peer == "" {
				fmt.Println("no --peer given; run this on the other machine and pass each " +
					"the address the other printed")
				return nil
			}
			fmt.Printf("punching towards %s ...\n", peer)
			ok, took, st, err := nattype.Punch(cmd.Context(), conn, peer, timeout)
			if err != nil {
				return err
			}
			fmt.Printf("sent %d packet(s) (%d failed), received %d (%d from the peer)\n",
				st.Sent, st.SendErrs, st.Received, st.FromOther)
			if st.LastErr != "" {
				fmt.Printf("last send error: %s\n", st.LastErr)
			}
			if ok {
				fmt.Printf("\n  DIRECT PATH OPEN after %.1fs\n", took.Seconds())
				fmt.Println("  These two networks allow a direct connection, so traffic between")
				fmt.Println("  them does not have to be relayed.")
				return nil
			}
			fmt.Printf("\n  No direct path after %.0fs\n", took.Seconds())
			fmt.Println("  Either the far side was not punching at the same time, or one of")
			fmt.Println("  these routers will not allow it. Traffic would have to be relayed.")
			return fmt.Errorf("no direct path")
		},
	}
	cmd.Flags().StringVar(&peer, "peer", "", "the peer's external address, host:port")
	cmd.Flags().IntVar(&localPort, "local-port", 41641, "local UDP port (must match on both runs)")
	cmd.Flags().DurationVar(&timeout, "timeout", 20*time.Second, "how long to keep trying")
	return cmd
}

// newRendezvousCmd runs the public half: a reflector that tells whoever asks
// where their packets appear to come from.
func newRendezvousCmd() *cobra.Command {
	var addrs []string

	cmd := &cobra.Command{
		Use:   "rendezvous",
		Short: "Run the public-side reflector that net-check probes",
		Long: "Answers every UDP packet with the source address it arrived from. Two " +
			"addresses are served because the test compares them; give two ports, or two " +
			"addresses on different interfaces if this host has more than one.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(addrs) == 0 {
				addrs = []string{":38443", ":38444"}
			}
			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			errs := make(chan error, len(addrs))
			for _, a := range addrs {
				go func(a string) {
					fmt.Printf("reflector listening on %s\n", a)
					errs <- nattype.Reflect(ctx, a)
				}(a)
			}
			select {
			case <-ctx.Done():
				return nil
			case err := <-errs:
				if err != nil {
					return err
				}
			}
			<-ctx.Done()
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&addrs, "listen", nil,
		"addresses to reflect on (default :38443 and :38444)")
	return cmd
}
