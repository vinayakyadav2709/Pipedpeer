// Package logging provides a shared zerolog logger for the Pipedpeer project.
// All internal packages should import this instead of creating their own loggers.
package logging

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Logger is the shared application-wide logger.
var Logger zerolog.Logger

func init() {
	// Default: pretty console output for development.
	// Can be overridden by calling SetupJSON() for production.
	// Seconds in the time so `pipedpeer events` can order lines; no colour
	// when stderr is a file (the daemon writes daemon.log this way).
	output := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05",
		NoColor:    os.Getenv("NO_COLOR") != "" || !isTerminal(os.Stderr),
	}
	Logger = zerolog.New(output).With().Timestamp().Logger()

	// Default level: info. Override with PIPEDPEER_LOG_LEVEL env.
	level := os.Getenv("PIPEDPEER_LOG_LEVEL")
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

// isTerminal reports whether f is a character device (a terminal).
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// SetupCLI points the global zerolog logger (github.com/rs/zerolog/log, which
// most internal packages write to) at the console writer. Its default is raw
// JSON on stderr, which is right for daemon.log but showed up as JSON blobs in
// the CLI's own output (a lost connection, attach). Call it in every process
// except the daemon.
func SetupCLI() {
	log.Logger = Logger
}

// SetupJSON switches to JSON-structured logging (for production/daemon mode).
func SetupJSON() {
	Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
}

// WithComponent returns a sub-logger with a "component" field set.
func WithComponent(name string) zerolog.Logger {
	return Logger.With().Str("component", name).Logger()
}
