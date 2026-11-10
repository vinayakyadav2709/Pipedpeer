package coordinator

import (
	"strconv"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func benchNodes(n int) []registry.NodeRecord {
	nodes := make([]registry.NodeRecord, n)
	for i := range nodes {
		nodes[i].NodeID = "node-" + strconv.Itoa(i)
		nodes[i].HealthScore = 1
		nodes[i].Load.CPUPercent = float64(i % 100)
		nodes[i].Load.TotalCPUs = 8 + i%8
		if i%2 == 0 {
			nodes[i].Capabilities = map[string]string{"cpu_cores": "16", "cpu_mhz": "3200"}
		}
	}
	return nodes
}

func BenchmarkScoreNodes(b *testing.B) {
	for _, n := range []int{16, 256, 4096} {
		nodes := benchNodes(n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			var sink float64
			for b.Loop() {
				for i := range nodes {
					sink += scoreNode(nodes[i]) + scoreGPUNode(nodes[i])
				}
			}
			_ = sink
		})
	}
}
