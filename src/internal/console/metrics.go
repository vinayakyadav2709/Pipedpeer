package console

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clusterview"
)

// How far back node metrics are kept.
const metricsRetention = 6 * time.Hour

// The windows GET /api/metrics accepts.
var metricsWindows = map[string]time.Duration{
	"5m":  5 * time.Minute,
	"15m": 15 * time.Minute,
	"1h":  time.Hour,
	"6h":  metricsRetention,
}

const defaultMetricsWindow = "15m"

// metricsBuffer holds a ring of samples per node covering metricsRetention.
type metricsBuffer struct {
	interval time.Duration
	capacity int

	mu     sync.Mutex
	series map[string]*nodeSeries
}

type nodeSeries struct {
	hostname string
	// points is a ring: next is where the following sample goes, and full
	// says whether it has wrapped.
	points []MetricPoint
	next   int
	full   bool
}

func newMetricsBuffer(interval time.Duration) *metricsBuffer {
	capacity := int(metricsRetention / interval)
	if capacity < 1 {
		capacity = 1
	}
	return &metricsBuffer{interval: interval, capacity: capacity, series: map[string]*nodeSeries{}}
}

func (s *nodeSeries) add(p MetricPoint, capacity int) {
	if len(s.points) < capacity {
		s.points = append(s.points, p)
		return
	}
	s.points[s.next] = p
	s.next = (s.next + 1) % capacity
	s.full = true
}

func (s *nodeSeries) ordered() []MetricPoint {
	if !s.full {
		return s.points
	}
	out := make([]MetricPoint, 0, len(s.points))
	out = append(out, s.points[s.next:]...)
	return append(out, s.points[:s.next]...)
}

func (s *nodeSeries) newest() int64 {
	if len(s.points) == 0 {
		return 0
	}
	if !s.full {
		return s.points[len(s.points)-1].T
	}
	return s.points[(s.next+len(s.points)-1)%len(s.points)].T
}

// record stores one sampling round. reachable is false when the daemon did
// not answer, in which case every known node gets a point with no readings so
// the gap is visible rather than joined up.
func (b *metricsBuffer) record(now time.Time, nodes []clusterview.Node, reachable bool) {
	t := now.UnixMilli()
	b.mu.Lock()
	defer b.mu.Unlock()

	if !reachable {
		for _, s := range b.series {
			s.add(MetricPoint{T: t}, b.capacity)
		}
		return
	}
	for _, n := range nodes {
		s, ok := b.series[n.NodeID]
		if !ok {
			s = &nodeSeries{}
			b.series[n.NodeID] = s
		}
		s.hostname = n.Hostname()
		s.add(pointOf(t, n), b.capacity)
	}
	// A node the daemon no longer lists stops getting points; drop its series
	// once everything in it has aged out.
	cutoff := now.Add(-metricsRetention).UnixMilli()
	for id, s := range b.series {
		if s.newest() < cutoff {
			delete(b.series, id)
		}
	}
}

// pointOf turns a node's reported load into a sample. Load reported for a
// node that is not healthy is its last known load, not a reading.
func pointOf(t int64, n clusterview.Node) MetricPoint {
	p := MetricPoint{T: t}
	if !n.Healthy() {
		return p
	}
	load := n.Load
	cpu, mem, jobs := load.CPUPercent, load.MemPercent, load.ActiveJobs
	p.CPU, p.Mem, p.ActiveJobs = &cpu, &mem, &jobs

	gpu := gpuTotals(n)
	if gpu.count == 0 {
		return p
	}
	util := gpu.utilPercent
	p.GPUUtil = &util
	if gpu.memTotal > 0 {
		used := float64(gpu.memUsed) / float64(gpu.memTotal) * 100
		p.GPUMem = &used
	}
	return p
}

// gpuSummary is a node's GPUs folded into one figure each.
type gpuSummary struct {
	count       int
	memTotal    int64
	memUsed     int64
	utilPercent float64
}

// gpuTotals prefers the per-device list and falls back to the single-GPU
// fields older daemons report.
func gpuTotals(n clusterview.Node) gpuSummary {
	load := n.Load
	if len(load.GPUs) > 0 {
		var g gpuSummary
		for _, d := range load.GPUs {
			g.count++
			g.memTotal += d.MemoryTotalBytes
			g.memUsed += d.MemoryTotalBytes - d.MemoryFreeBytes
			g.utilPercent += d.UtilizationGPU
		}
		g.utilPercent /= float64(g.count)
		return g
	}
	if load.GPUModel == "" && load.GPUMemBytes == 0 {
		return gpuSummary{}
	}
	return gpuSummary{count: 1, memTotal: load.GPUMemBytes, memUsed: load.GPUMemUsedBytes, utilPercent: load.GPUUtilPercent}
}

func (b *metricsBuffer) snapshot(now time.Time, window time.Duration) Metrics {
	cutoff := now.Add(-window).UnixMilli()
	out := Metrics{IntervalMs: b.interval.Milliseconds(), Series: []MetricSeries{}}

	b.mu.Lock()
	defer b.mu.Unlock()
	for id, s := range b.series {
		points := []MetricPoint{}
		for _, p := range s.ordered() {
			if p.T >= cutoff {
				points = append(points, p)
			}
		}
		if len(points) == 0 {
			continue
		}
		out.Series = append(out.Series, MetricSeries{NodeID: id, Hostname: s.hostname, Points: points})
	}
	sort.Slice(out.Series, func(i, j int) bool {
		a, c := out.Series[i], out.Series[j]
		if a.Hostname != c.Hostname {
			return a.Hostname < c.Hostname
		}
		return a.NodeID < c.NodeID
	})
	return out
}

// sampleLoop samples the daemon until ctx ends, starting immediately so the
// first chart has a point.
func (s *Server) sampleLoop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.SampleInterval)
	defer ticker.Stop()
	for {
		nodes, err := clusterview.FetchNodes(ctx, s.opts.DaemonPort)
		if ctx.Err() != nil {
			return
		}
		s.metrics.record(time.Now(), nodes, err == nil)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
