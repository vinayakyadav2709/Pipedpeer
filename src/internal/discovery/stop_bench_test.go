package discovery

import "testing"

func BenchmarkAdvertiserStartStop(b *testing.B) {
	for b.Loop() {
		a := NewAdvertiser(ServiceInfo{NodeID: "n", DaemonPort: 1})
		if err := a.Start(); err != nil {
			b.Skip(err)
		}
		a.Stop()
	}
}
