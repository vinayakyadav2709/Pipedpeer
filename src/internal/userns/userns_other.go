//go:build !linux

package userns

// Install is a no-op off Linux: there is no namespace probe to re-exec into.
func Install() {}

// Available reports that user namespaces, and so the crun sandbox, are
// Linux-only. Pipedpeer's daemon targets Linux; this stub only lets the CLI
// build for other platforms.
var Available = func() (bool, string) {
	return false, "user namespaces are Linux-only; the job sandbox needs a Linux machine"
}
