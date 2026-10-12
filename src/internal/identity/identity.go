package identity

import (
	"encoding/json"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
)

// GPUInfo describes the GPU hardware available on this node.
type GPUInfo struct {
	Vendor      gpu.Vendor `json:"vendor"`
	Name        string     `json:"name,omitempty"`
	MemoryBytes int64      `json:"memory_bytes,omitempty"`
	Count       int        `json:"count,omitempty"`
}

// NodeIdentity represents a stable node identity that persists across restarts.
type NodeIdentity struct {
	NodeID    string    `json:"node_id"`
	Hostname  string    `json:"hostname"`
	Arch      string    `json:"arch"`
	GPU       GPUInfo   `json:"gpu,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// GetOrCreate loads an existing identity from disk or generates a new one.
// GPU info is automatically detected and persisted if missing or changed.
func GetOrCreate() (NodeIdentity, error) {
	p := identityPath()
	var id NodeIdentity
	var loaded bool

	if data, err := os.ReadFile(p); err == nil {
		if err := json.Unmarshal(data, &id); err == nil && id.NodeID != "" {
			loaded = true
		}
	}

	if !loaded {
		return create(p)
	}

	// Refresh GPU info: existing identity files may lack it, or hardware changed
	gpuInfo := gpu.Detect()
	needsUpdate := false

	if gpuInfo.Vendor != gpu.VendorNone {
		if id.GPU.Vendor != gpuInfo.Vendor ||
			id.GPU.Name != gpuInfo.Name ||
			id.GPU.MemoryBytes != gpuInfo.MemoryBytes ||
			id.GPU.Count != gpuInfo.Count {
			id.GPU = GPUInfo{
				Vendor:      gpuInfo.Vendor,
				Name:        gpuInfo.Name,
				MemoryBytes: gpuInfo.MemoryBytes,
				Count:       gpuInfo.Count,
			}
			needsUpdate = true
		}
	} else if id.GPU.Vendor != gpu.VendorNone {
		// GPU was removed
		id.GPU = GPUInfo{}
		needsUpdate = true
	}

	if needsUpdate {
		data, err := json.MarshalIndent(id, "", "  ")
		if err == nil {
			os.WriteFile(p, data, 0600)
		}
	}

	return id, nil
}

func create(path string) (NodeIdentity, error) {
	hostname, _ := os.Hostname()
	gpuInfo := gpu.Detect()
	id := NodeIdentity{
		NodeID:    generateUUID(),
		Hostname:  hostname,
		Arch:      nixArch(),
		CreatedAt: time.Now().UTC(),
	}
	if gpuInfo.Vendor != gpu.VendorNone {
		id.GPU = GPUInfo{
			Vendor:      gpuInfo.Vendor,
			Name:        gpuInfo.Name,
			MemoryBytes: gpuInfo.MemoryBytes,
			Count:       gpuInfo.Count,
		}
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
		return filepath.Join(userdir.Data(), "node_identity.json")
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
