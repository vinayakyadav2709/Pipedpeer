package heartbeat

import "testing"

func BenchmarkCollectLoad(b *testing.B) {
	for b.Loop() {
		CollectLoad(1, 1<<20)
	}
}
