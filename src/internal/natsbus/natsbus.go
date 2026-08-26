// Package natsbus provides NATS connectivity, embedded server support,
// and helpers for pub/sub, request/reply, and JetStream KV operations.
package natsbus

import (
	"encoding/json"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"os"
	"path/filepath"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Bus wraps a NATS connection with optional embedded server and JetStream context.
type Bus struct {
	Conn     *nats.Conn
	JS       jetstream.JetStream
	server   *natsserver.Server
	embedded bool
}

// Config controls NATS connection behavior.
type Config struct {
	// URL is the NATS server URL (e.g., "nats://localhost:4222").
	// If empty and Embedded is true, an embedded server is started.
	URL string

	// Embedded starts an in-process NATS server with JetStream.
	// The server listens on a random port and StoreDir is used for persistence.
	Embedded bool

	// StoreDir is the JetStream storage directory (required when Embedded is true).
	// If empty, defaults to $XDG_DATA_HOME/pipedpeer/nats or ~/.local/share/pipedpeer/nats.
	StoreDir string

	// Name is the client connection name for diagnostics.
	Name string
}

// New creates a NATS Bus. If Embedded is true and URL is empty,
// an in-process NATS server with JetStream is started.
func New(cfg Config) (*Bus, error) {
	b := &Bus{}

	if cfg.Embedded && cfg.URL == "" {
		storeDir := cfg.StoreDir
		if storeDir == "" {
			storeDir = defaultStoreDir()
		}
		if err := os.MkdirAll(storeDir, 0755); err != nil {
			return nil, fmt.Errorf("natsbus: create store dir: %w", err)
		}

		opts := &natsserver.Options{
			Host:      "127.0.0.1",
			Port:      -1, // random available port
			JetStream: true,
			StoreDir:  storeDir,
			NoLog:     true,
			NoSigs:    true,
		}
		ns, err := natsserver.NewServer(opts)
		if err != nil {
			return nil, fmt.Errorf("natsbus: create embedded server: %w", err)
		}
		go ns.Start()
		if !ns.ReadyForConnections(5 * time.Second) {
			ns.Shutdown()
			return nil, fmt.Errorf("natsbus: embedded server failed to start")
		}
		b.server = ns
		b.embedded = true
		cfg.URL = ns.ClientURL()
	}

	if cfg.URL == "" {
		cfg.URL = nats.DefaultURL
	}

	connOpts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
	}
	if cfg.Name != "" {
		connOpts = append(connOpts, nats.Name(cfg.Name))
	}

	nc, err := nats.Connect(cfg.URL, connOpts...)
	if err != nil {
		if b.server != nil {
			b.server.Shutdown()
		}
		return nil, fmt.Errorf("natsbus: connect to %s: %w", cfg.URL, err)
	}
	b.Conn = nc

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		if b.server != nil {
			b.server.Shutdown()
		}
		return nil, fmt.Errorf("natsbus: create jetstream context: %w", err)
	}
	b.JS = js

	return b, nil
}

// Close shuts down the NATS connection and embedded server (if any).
func (b *Bus) Close() {
	if b.Conn != nil {
		b.Conn.Drain()
	}
	if b.server != nil {
		b.server.Shutdown()
		b.server.WaitForShutdown()
	}
}

// ClientURL returns the NATS server URL (useful for passing to other components).
func (b *Bus) ClientURL() string {
	if b.server != nil {
		return b.server.ClientURL()
	}
	return b.Conn.ConnectedUrl()
}

// IsEmbedded returns true if the bus is running an embedded NATS server.
func (b *Bus) IsEmbedded() bool {
	return b.embedded
}

// Publish sends a message to a subject.
func (b *Bus) Publish(subject string, data []byte) error {
	return b.Conn.Publish(subject, data)
}

// PublishJSON marshals v as JSON and publishes to a subject.
func (b *Bus) PublishJSON(subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("natsbus: marshal: %w", err)
	}
	return b.Conn.Publish(subject, data)
}

// Request sends a request and waits for a reply with a timeout.
func (b *Bus) Request(subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	return b.Conn.Request(subject, data, timeout)
}

// RequestJSON sends a JSON-encoded request and decodes the JSON reply into result.
func (b *Bus) RequestJSON(subject string, req any, result any, timeout time.Duration) error {
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("natsbus: marshal request: %w", err)
	}
	msg, err := b.Conn.Request(subject, data, timeout)
	if err != nil {
		return fmt.Errorf("natsbus: request %s: %w", subject, err)
	}
	if err := json.Unmarshal(msg.Data, result); err != nil {
		return fmt.Errorf("natsbus: unmarshal reply: %w", err)
	}
	return nil
}

// Subscribe subscribes to a subject and calls handler for each message.
func (b *Bus) Subscribe(subject string, handler nats.MsgHandler) (*nats.Subscription, error) {
	return b.Conn.Subscribe(subject, handler)
}

// QueueSubscribe subscribes to a subject with a queue group for load balancing.
func (b *Bus) QueueSubscribe(subject, queue string, handler nats.MsgHandler) (*nats.Subscription, error) {
	return b.Conn.QueueSubscribe(subject, queue, handler)
}

func defaultStoreDir() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "pipedpeer", "nats")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(userdir.State(), "nats")
	}
	return filepath.Join(home, ".local", "share", "pipedpeer", "nats")
}
