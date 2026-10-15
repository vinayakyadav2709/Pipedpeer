package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func TestPrintNodeTable(t *testing.T) {
	gpu := clusterNode{NodeID: "aaaaaaaa1111", State: "healthy", Source: "self",
		Capabilities: map[string]string{"hostname": "laptop-a"},
		Load: registry.LoadInfo{TotalCPUs: 8, CPUPercent: 37, GPUModel: "NVIDIA GeForce RTX 3060 Laptop GPU",
			GPUUtilPercent: 12, GPUMemBytes: 6 << 30, GPUMemUsedBytes: 1 << 30}}
	cpu := clusterNode{NodeID: "bbbbbbbb2222", State: "healthy", Source: "discovery",
		Capabilities: map[string]string{"hostname": "pc-2"}, Load: registry.LoadInfo{TotalCPUs: 4, CPUPercent: 5}}
	var b bytes.Buffer
	printNodeTable(&b, []clusterNode{gpu, cpu})
	out := b.String()
	for _, want := range []string{"8c 37%", "RTX 3060 12% 1.0G/6.0G", "self", "4c 5%"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Count(out, " - ") < 1 {
		t.Errorf("GPU-less node should show '-':\n%s", out)
	}
}

// Cloned lab PCs share a hostname: each row gets its short id, and a node on
// another build is marked.
func TestPrintNodeTableTellsClonesApart(t *testing.T) {
	a := clusterNode{NodeID: "1111111122", State: "healthy", Capabilities: map[string]string{"hostname": "lab-pc"}}
	b := clusterNode{NodeID: "3333333344", State: "unreachable", Reason: "no route",
		Capabilities: map[string]string{"hostname": "lab-pc", "version": "0.0.1-other"}}
	var buf bytes.Buffer
	printNodeTable(&buf, []clusterNode{a, b})
	out := buf.String()
	for _, want := range []string{"lab-pc (11111111)", "lab-pc (33333333)", "no route", "other version 0.0.1-other"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
