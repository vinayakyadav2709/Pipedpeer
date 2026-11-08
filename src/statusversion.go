package main

import "fmt"

// statusVersion renders the version shown by `pipedpeer status`: the daemon's,
// since that is the binary doing the work, with this CLI's when they differ.
// The second return value is a warning for stderr when the daemon does not
// report a version at all.
func statusVersion(cli, daemon string, err error) (shown, warn string) {
	if err != nil {
		return withCLI("unknown", cli), fmt.Sprintf("pipedpeer: could not read the daemon's version: %v", err)
	}
	if daemon == "" {
		return withCLI("unknown", cli), "pipedpeer: the daemon does not report its version (older build)"
	}
	return withCLI(daemon, cli), ""
}

func withCLI(daemon, cli string) string {
	if cli != "" && cli != daemon {
		return daemon + " (this CLI: " + cli + ")"
	}
	return daemon
}
