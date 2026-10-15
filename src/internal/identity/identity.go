package identity

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// GPUInfo describes the GPU hardware available on this node.
type GPUInfo struct {
	Vendor      gpu.Vendor `json:"vendor"`
	Name        string     `json:"name,omitempty"`
	MemoryBytes int64      `json:"memory_bytes,omitempty"`
	Count       int        `json:"count,omitempty"`
}

// NodeIdentity represents a stable node identity that persists across restarts.
//
// NodeID is the fingerprint of the node key (Key().Fingerprint()), so the id
// a peer is known by and the key that proves it are one thing. It used to be
// a separate random UUID, which let the internet manager (keyed by
// fingerprint) and the node rows (keyed by UUID) describe one machine as two.
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
	key, err := Key()
	if err != nil {
		return NodeIdentity{}, err
	}
	nodeID := key.Fingerprint()

	p := identityPath()
	var id NodeIdentity
	var loaded bool

	if data, err := os.ReadFile(p); err == nil {
		if err := json.Unmarshal(data, &id); err == nil {
			loaded = true
		} else {
			log.Warn().Err(err).Str("path", p).Msg("identity file unreadable; rewriting it")
		}
	}

	if !loaded {
		return create(p, nodeID)
	}

	needsUpdate := false
	// Files written before the node id became the key fingerprint hold a UUID.
	if id.NodeID != nodeID {
		id.NodeID = nodeID
		needsUpdate = true
	}

	// Refresh GPU info: existing identity files may lack it, or hardware changed
	gpuInfo := gpu.Detect()

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
			err = os.WriteFile(p, data, 0600)
		}
		if err != nil {
			// The in-memory identity is still right; only the cached copy is stale.
			log.Warn().Err(err).Str("path", p).Msg("could not update identity file")
		}
	}

	return id, nil
}

func create(path, nodeID string) (NodeIdentity, error) {
	hostname, _ := os.Hostname()
	gpuInfo := gpu.Detect()
	id := NodeIdentity{
		NodeID:    nodeID,
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

func identityPath() string { return filepath.Join(userdir.Data(), "node_identity.json") }

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
