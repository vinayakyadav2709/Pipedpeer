package nodestore

import (
	"path/filepath"
	"strconv"
	"testing"
)

// Payloads the size a real health poll stores: a daemon's capability map and a
// load sample with one GPU.
const (
	benchCaps = `{"arch":"x86_64-linux","hostname":"bench-host","cpu_cores":"16","cpu_mhz":"3200","gpu":"cuda","gpu_compute_cap":"8.6","os":"linux"}`
	benchLoad = `{"cpu_percent":12.5,"memory_percent":41.2,"active_jobs":1,"queue_depth":0,"recent_failures":0,"total_mem_bytes":68719476736,"available_mem_bytes":34359738368,"total_cpus":16,"gpu_model":"bench-gpu","gpu_mem_bytes":25769803776,"gpu_mem_used_bytes":1073741824,"gpu_util_percent":3,"gpus":[{"index":0,"name":"bench-gpu","memory_total_bytes":25769803776,"memory_free_bytes":24696061952,"utilization_gpu_percent":3}]}`
)

func benchNode(i int) Node {
	id := strconv.Itoa(i)
	return Node{
		NodeID:       "0b9c2f6e-5d1a-4c3b-9e7f-" + id,
		Arch:         "x86_64-linux",
		Hostname:     "bench-host-" + id,
		ActiveJobs:   i % 4,
		AvailableMem: 32 << 30,
		TotalMem:     64 << 30,
		CPUPercent:   12.5,
		CapsJSON:     benchCaps,
		LoadJSON:     benchLoad,
	}
}

// BenchmarkPollRound is the store writes of one daemon peer-poll round: a
// fresh health result for every node. Set TMPDIR to a disk-backed directory:
// on tmpfs a commit's fsync costs nothing. The row changes every round, as a
// real load sample does, so SQLite cannot skip the write.
func BenchmarkPollRound(b *testing.B) {
	for _, n := range []int{16, 256} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			s, err := NewAt(filepath.Join(b.TempDir(), "nodes.db"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { s.Close() })
			nodes := make([]Node, n)
			for i := range nodes {
				nodes[i] = benchNode(i)
			}
			b.ReportAllocs()
			for b.Loop() {
				for i := range nodes {
					nodes[i].AvailableMem++
				}
				if err := s.MarkHealthy(nodes...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
