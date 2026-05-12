package identity

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
)

// NodeIdentity represents a stable node identity that persists across restarts.
type NodeIdentity struct {
	NodeID    string    `json:"node_id"`
	Hostname  string    `json:"hostname"`
	Arch      string    `json:"arch"`
	CreatedAt time.Time `json:"created_at"`
}

// GetOrCreate loads an existing identity from disk or generates a new one.
func GetOrCreate() (NodeIdentity, error) {
	p := identityPath()
	if data, err := os.ReadFile(p); err == nil {
		var id NodeIdentity
		if err := json.Unmarshal(data, &id); err == nil && id.NodeID != "" {
			return id, nil
		}
	}
	return create(p)
}

func create(path string) (NodeIdentity, error) {
	hostname, _ := os.Hostname()
	id := NodeIdentity{
		NodeID:    generateUUID(),
		Hostname:  hostname,
		Arch:      nixArch(),
		CreatedAt: time.Now().UTC(),
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return NodeIdentity{}, fmt.Errorf("create identity dir: %w", err)
	}
	data, err := json.MarshalIndent(id, "", "  ")
	if err != nil {
		return NodeIdentity{}, err
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return NodeIdentity{}, fmt.Errorf("write identity: %w", err)
	}
	return id, nil
}

func identityPath() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "pipedpeer", "node_identity.json")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "pipedpeer", "node_identity.json")
	}
	return filepath.Join(home, ".local", "share", "pipedpeer", "node_identity.json")
}

// generateUUID produces a UUID v4 using google/uuid.
func generateUUID() string {
	return uuid.New().String()
}

func nixArch() string {
	arch := runtime.GOARCH
	goos := runtime.GOOS
	nixArch := "x86_64"
	switch arch {
	case "arm64":
		nixArch = "aarch64"
	case "arm":
		nixArch = "armv7l"
	}
	nixOS := "linux"
	if goos == "darwin" {
		nixOS = "darwin"
	}
	return nixArch + "-" + nixOS
}

// ShortID returns the first 8 chars of the node ID for display.
func (n NodeIdentity) ShortID() string {
	if len(n.NodeID) >= 8 {
		return n.NodeID[:8]
	}
	return n.NodeID
}

// MatchesID checks if a given ID matches this node (full or short prefix).
func (n NodeIdentity) MatchesID(id string) bool {
	return n.NodeID == id || strings.HasPrefix(n.NodeID, id)
}
