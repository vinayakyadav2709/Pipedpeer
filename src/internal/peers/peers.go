package peers

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Peer struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func path() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "pipedpeer", "peers.json")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "pipedpeer", "peers.json")
}

func load() ([]Peer, error) {
	data, err := os.ReadFile(path())
	if err != nil {
		return nil, nil
	}
	var peers []Peer
	if err := json.Unmarshal(data, &peers); err != nil {
		return nil, nil
	}
	return peers, nil
}

func save(peers []Peer) error {
	if err := os.MkdirAll(filepath.Dir(path()), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(peers, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path(), data, 0644)
}

func Add(host string, port int) error {
	peers, _ := load()
	host = strings.TrimSpace(host)
	for _, p := range peers {
		if p.Host == host && p.Port == port {
			return fmt.Errorf("peer %s:%d already exists", host, port)
		}
	}
	peers = append(peers, Peer{Host: host, Port: port})
	return save(peers)
}

func Remove(host string) error {
	peers, _ := load()
	host = strings.TrimSpace(host)
	var kept []Peer
	removed := false
	for _, p := range peers {
		if p.Host == host {
			removed = true
			continue
		}
		kept = append(kept, p)
	}
	if !removed {
		return fmt.Errorf("peer %s not found", host)
	}
	return save(kept)
}

func List() ([]Peer, error) {
	return load()
}

func Path() string {
	return path()
}
