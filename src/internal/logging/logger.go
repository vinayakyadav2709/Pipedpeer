// Package logging provides a shared zerolog logger for the Pipedpeer project.
// All internal packages should import this instead of creating their own loggers.
package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Logger is the shared application-wide logger.
var Logger zerolog.Logger

func init() {
	// Default: pretty console output for development.
	// Can be overridden by calling SetupJSON() for production.
	output := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.Kitchen,
		NoColor:    os.Getenv("NO_COLOR") != "",
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

// SetupJSON switches to JSON-structured logging (for production/daemon mode).
func SetupJSON() {
	Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
}

// WithComponent returns a sub-logger with a "component" field set.
func WithComponent(name string) zerolog.Logger {
	return Logger.With().Str("component", name).Logger()
}
