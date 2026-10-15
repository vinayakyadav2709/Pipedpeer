// Package say is the CLI's own progress output. A plain `pipedpeer run`
// shows the script's output and nothing else, like python; `-v` turns these
// lines back on. Errors and warnings the user must act on always print.
package say

import (
	"fmt"
	"os"
)

// Verbose is set by `run -v`. Off by default.
var Verbose bool

// Printf prints a progress line to stdout, only when Verbose.
func Printf(format string, a ...any) {
	if Verbose {
		fmt.Fprintf(os.Stdout, format, a...)
	}
}

// Println prints a progress line to stdout, only when Verbose.
func Println(a ...any) {
	if Verbose {
		fmt.Fprintln(os.Stdout, a...)
	}
}

// Warnf prints to stderr whatever the verbosity, prefixed "pipedpeer: ".
// For things that change the result or need the user to act.
func Warnf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "pipedpeer: "+format, a...)
}
