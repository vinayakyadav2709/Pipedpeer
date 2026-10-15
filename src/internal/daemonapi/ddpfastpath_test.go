package daemonapi

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// U26: the butterfly reduce (rank k's daemon averages shard k) and the
// multi-GPU fast path (ranks on one machine average there first).

// blockCuts is the shim's _shard_cuts: n ranges of a vector cut on block
// boundaries, as (lo, hi, blockLo, blockHi).
func blockCuts(counts []int, n int) [][4]int {
	ends := []int{0}
	for _, c := range counts {
		ends = append(ends, ends[len(ends)-1]+c)
	}
	nb := len(counts)
	out := make([][4]int, n)
	for k := 0; k < n; k++ {
		blo, bhi := nb*k/n, nb*(k+1)/n
		out[k] = [4]int{ends[blo], ends[bhi], blo, bhi}
	}
	return out
}

// fastpathPayload is rank r's contribution: n values of dtype, and one scale
// per block for int8.
func fastpathPayload(dtype ddpDType, n, nblocks, r int, exact bool) ([]byte, []float64) {
	switch dtype {
	case ddpI8:
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(int8((i*7+r*13)%255 - 127))
		}
		sc := make([]float64, nblocks)
		for i := range sc {
			sc[i] = 1 / float64(int(1)<<(i%5)) // powers of two: sums exact in any order
			if !exact {
				sc[i] = 0.001 * float64(1+(i*3+r)%17)
			}
		}
		return b, sc
	default:
		b := make([]byte, 4*n)
		for i := 0; i < n; i++ {
			v := float32((i*5+r*11)%97-48) / 8 // eighths: sums exact in any order
			if !exact {
				v = float32(math.Sin(float64(i*(r+1)))) * 0.37
			}
			binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(v))
		}
		return b, []float64{0}
	}
}

// TestButterflyShardsEqualTheHubBitForBit: averaging each block-aligned
// shard on its own and joining the results gives the bytes (and int8
// scales) the hub's one average of the whole vector gives, over two rounds
// so the int8 reply's carried rounding error is covered too.
func TestButterflyShardsEqualTheHubBitForBit(t *testing.T) {
	counts := []int{4096, 4096, 1808, 4096, 7}
	n := 0
	for _, c := range counts {
		n += c
	}
	weights := []float64{16, 8, 24}
	for _, dtype := range []ddpDType{ddpF32, ddpI8} {
		const shards = 3
		cuts := blockCuts(counts, shards)
		hubRes := make([]float64, n)
		shardRes := make([][]float64, shards)
		for k, c := range cuts {
			shardRes[k] = make([]float64, c[1]-c[0])
		}
		for round := 0; round < 2; round++ {
			hub := newDDPAccumulator(dtype, n)
			if err := hub.withSegments(counts); err != nil {
				t.Fatal(err)
			}
			parts := make([]*ddpAccumulator, shards)
			for k, c := range cuts {
				parts[k] = newDDPAccumulator(dtype, c[1]-c[0])
				if err := parts[k].withSegments(counts[c[2]:c[3]]); err != nil {
					t.Fatal(err)
				}
			}
			for r, w := range weights {
				payload, scales := fastpathPayload(dtype, n, len(counts), r+round, false)
				if err := hub.addSegmented(payload, scales, w); err != nil {
					t.Fatal(err)
				}
				for k, c := range cuts {
					lo, hi := c[0]*dtype.size(), c[1]*dtype.size()
					sc := scales
					if dtype == ddpI8 {
						sc = scales[c[2]:c[3]]
					}
					if err := parts[k].addSegmented(payload[lo:hi], sc, w); err != nil {
						t.Fatal(err)
					}
				}
			}
			want, wantScales, err := hub.meanResidual(hubRes)
			if err != nil {
				t.Fatal(err)
			}
			var got []byte
			var gotScales []float64
			for k := range parts {
				b, sc, err := parts[k].meanResidual(shardRes[k])
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, b...)
				gotScales = append(gotScales, sc...)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("%s round %d: butterfly mean differs from the hub's", dtype, round)
			}
			if dtype == ddpI8 && fmt.Sprint(gotScales) != fmt.Sprint(wantScales) {
				t.Fatalf("%s round %d: scales differ\n got %v\nwant %v", dtype, round, gotScales, wantScales)
			}
		}
	}
}

// ddpPostRaw sends one reduce contribution and returns the reply header and
// frame.
func ddpPostRaw(t *testing.T, url string, hdr map[string]any, payload []byte) (map[string]any, []byte) {
	t.Helper()
	head, _ := json.Marshal(hdr)
	resp, err := http.Post(url, DDPReduceContentType, bytes.NewReader(putFrame(append(head, '\n'), payload)))
	if err != nil {
		t.Error(err)
		return nil, nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST %s: HTTP %d: %s", url, resp.StatusCode, body)
		return nil, nil
	}
	nl := bytes.IndexByte(body, '\n')
	var reply map[string]any
	if err := json.Unmarshal(body[:nl], &reply); err != nil {
		t.Error(err)
		return nil, nil
	}
	frame, _, err := readFrame(body[nl+1:])
	if err != nil {
		t.Error(err)
	}
	return reply, frame
}

// TestButterflyOverHTTPMatchesTheHub: three ranks send shard k to daemon k
// (shards 1 and 2 to one daemon, as when a node hosts two ranks) and get the
// hub's result back, int8 scales included; shards above 0 leave the run's
// notes alone.
func TestButterflyOverHTTPMatchesTheHub(t *testing.T) {
	counts := []int{4096, 4096, 4096, 100}
	n := 4096*3 + 100
	cuts := blockCuts(counts, 3)
	for _, dtype := range []ddpDType{ddpF32, ddpI8} {
		hub := httptest.NewServer(New("hub").Handler())
		s0 := httptest.NewServer(New("s0").Handler())
		s1 := httptest.NewServer(New("s1").Handler())
		shardURL := []string{s0.URL, s1.URL, s1.URL}

		hubOut := make([][]byte, 3)
		var parts [3][3][]byte // rank, shard
		var wg sync.WaitGroup
		for r := 0; r < 3; r++ {
			payload, scales := fastpathPayload(dtype, n, len(counts), r, true)
			base := map[string]any{"group": "g-" + string(dtype), "seq": 2, "rank": r, "world": 3,
				"kind": "grads", "dtype": string(dtype), "samples": 16 * (r + 1)}
			wg.Add(1)
			go func() {
				defer wg.Done()
				h := map[string]any{"count": n, "scales": scales, "counts": counts, "scale": scales[0]}
				for k, v := range base {
					h[k] = v
				}
				_, hubOut[r] = ddpPostRaw(t, hub.URL+"/v1/ddp/sync", h, payload)
			}()
			for k, c := range cuts {
				wg.Add(1)
				go func() {
					defer wg.Done()
					h := map[string]any{"count": c[1] - c[0], "shard": k, "shards": 3}
					if dtype == ddpI8 {
						h["scales"], h["counts"], h["scale"] = scales[c[2]:c[3]], counts[c[2]:c[3]], scales[c[2]]
					}
					for key, v := range base {
						h[key] = v
					}
					lo, hi := c[0]*dtype.size(), c[1]*dtype.size()
					reply, frame := ddpPostRaw(t, shardURL[k]+"/v1/ddp/sync", h, payload[lo:hi])
					if k > 0 && reply != nil && reply["weights"] != nil {
						t.Errorf("shard %d's reply carried a ring note", k)
					}
					parts[r][k] = frame
				}()
			}
		}
		wg.Wait()
		hub.Close()
		s0.Close()
		s1.Close()
		if t.Failed() {
			return
		}
		for r := 0; r < 3; r++ {
			if !bytes.Equal(hubOut[r], hubOut[0]) {
				t.Fatalf("%s: the hub gave rank %d a different result", dtype, r)
			}
			if !bytes.Equal(bytes.Join(parts[r][:], nil), hubOut[0]) {
				t.Fatalf("%s: rank %d's butterfly result differs from the hub's", dtype, r)
			}
		}
	}
}

// ddpCountingServer counts the sync POSTs a daemon receives.
func ddpCountingServer(name string) (*httptest.Server, *atomic.Int64) {
	var n atomic.Int64
	h := New(name).Handler()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/ddp/sync" {
			n.Add(1)
		}
		h.ServeHTTP(w, r)
	})), &n
}

// TestSameMachineRanksSyncOncePerMachine: ranks 1 and 2 share a node; their
// daemon averages them and the lead sees one sync for the machine, and every
// rank gets the same sample-weighted average of all three.
func TestSameMachineRanksSyncOncePerMachine(t *testing.T) {
	lead, leadPosts := ddpCountingServer("lead")
	defer lead.Close()
	node, nodePosts := ddpCountingServer("node")
	defer node.Close()
	const n = 4096 + 10
	samples := []int{16, 16, 32}
	frames := make([][]byte, 3)
	var wg sync.WaitGroup
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload, _ := fastpathPayload(ddpF32, n, 2, r, true)
			h := map[string]any{"group": "g", "seq": 2, "rank": r, "world": 3, "kind": "grads",
				"dtype": "float32", "count": n, "samples": samples[r], "step_ms": 10.0 + float64(r)}
			url := lead.URL
			if r > 0 {
				url = node.URL
				h["local"], h["upstream"] = []int{1, 2}, lead.URL+"/v1/ddp/sync"
			}
			reply, frame := ddpPostRaw(t, url+"/v1/ddp/sync", h, payload)
			if reply != nil && reply["ranks"] != float64(3) {
				t.Errorf("rank %d: reply says %v ranks, want 3", r, reply["ranks"])
			}
			frames[r] = frame
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	if got := leadPosts.Load(); got != 2 {
		t.Fatalf("lead received %d syncs, want 2 (one per machine)", got)
	}
	if got := nodePosts.Load(); got != 2 {
		t.Fatalf("node daemon received %d syncs, want 2", got)
	}
	for r := 1; r < 3; r++ {
		if !bytes.Equal(frames[r], frames[0]) {
			t.Fatalf("rank %d got a different average than rank 0", r)
		}
	}
	var p [3][]byte
	for r := range p {
		p[r], _ = fastpathPayload(ddpF32, n, 2, r, true)
	}
	f := func(b []byte, i int) float64 {
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:])))
	}
	for i := 0; i < n; i++ {
		want := (16*f(p[0], i) + 16*f(p[1], i) + 32*f(p[2], i)) / 64
		if got := f(frames[0], i); math.Abs(got-want) > 1e-6*(1+math.Abs(want)) {
			t.Fatalf("value %d: got %g, want the weighted average %g", i, got, want)
		}
	}
}

// TestShimButterflyEqualsTheHub runs three real ranks twice, on the hub and
// on the butterfly (three daemons), and compares their weights bit for bit,
// on the float32 wire and on the default int8 one.
func TestShimButterflyEqualsTheHub(t *testing.T) {
	dir, run := ddpCoreSetup(t)
	for _, wire := range [][]string{{"PIPEDPEER_DDP_INT8=0", "PIPEDPEER_DDP_FP32=1"}, nil} {
		name := "int8"
		if wire != nil {
			name = "fp32"
		}
		var shardPosts int64
		for _, mode := range []string{"hub", "bfly"} {
			servers := make([]*httptest.Server, 3)
			counts := make([]*atomic.Int64, 3)
			for i := range servers {
				servers[i], counts[i] = ddpCountingServer(fmt.Sprintf("n%d", i))
			}
			urls := make([]string, 3)
			for i, s := range servers {
				urls[i] = s.URL + "/v1/ddp/sync"
			}
			var wg sync.WaitGroup
			outs, errs := make([]string, 3), make([]error, 3)
			for rank := 0; rank < 3; rank++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					env := append([]string{"PIPEDPEER_DDP=1", fmt.Sprintf("PIPEDPEER_RANK=%d", rank),
						"PIPEDPEER_WORLD_SIZE=3", "PIPEDPEER_DDP_SYNC=" + urls[0], "PIPEDPEER_DDP_GROUP=g",
						"PIPEDPEER_DDP_START_WAIT=60", "TEST_WIDTH=12288",
						"TEST_PREFIX=" + name + "-" + mode}, wire...)
					if mode == "bfly" {
						env = append(env, "PIPEDPEER_DDP_PEERS="+strings.Join(urls, ","))
					}
					out, err := run(env...).CombinedOutput()
					outs[rank], errs[rank] = string(out), err
				}()
			}
			wg.Wait()
			for _, s := range servers {
				s.Close()
			}
			for rank, err := range errs {
				if err != nil {
					t.Fatalf("%s %s rank %d: %v\n%s", name, mode, rank, err, outs[rank])
				}
			}
			if mode == "bfly" {
				shardPosts = counts[1].Load() + counts[2].Load()
			}
		}
		if shardPosts == 0 {
			t.Fatalf("%s: the butterfly run never posted a shard to another rank's daemon", name)
		}
		check := exec.Command("python3", "-c", `
import os, sys, torch
d, name = sys.argv[1], sys.argv[2]
for r in range(3):
    a = torch.load(os.path.join(d, "%s-hub-%d.pt" % (name, r)))
    b = torch.load(os.path.join(d, "%s-bfly-%d.pt" % (name, r)))
    assert torch.equal(a["w"], b["w"]), "rank %d: butterfly weights differ from the hub's" % r
print("BFLY-OK")
`, dir, name)
		if out, err := check.CombinedOutput(); err != nil || !strings.Contains(string(out), "BFLY-OK") {
			t.Fatalf("%s: %v\n%s", name, err, out)
		}
	}
}

// TestShimSameMachineRanksSyncOnce: ranks 1 and 2 on one node send their
// gradient step through their own daemon; the lead sees the three opening
// broadcasts and two gradient syncs, not three, and the ranks agree.
func TestShimSameMachineRanksSyncOnce(t *testing.T) {
	dir, run := ddpCoreSetup(t)
	lead, leadPosts := ddpCountingServer("lead")
	defer lead.Close()
	node, nodePosts := ddpCountingServer("node")
	defer node.Close()
	var wg sync.WaitGroup
	outs, errs := make([]string, 3), make([]error, 3)
	for rank := 0; rank < 3; rank++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			env := []string{"PIPEDPEER_DDP=1", fmt.Sprintf("PIPEDPEER_RANK=%d", rank),
				"PIPEDPEER_WORLD_SIZE=3", "PIPEDPEER_DDP_SYNC=" + lead.URL + "/v1/ddp/sync",
				"PIPEDPEER_DDP_GROUP=g", "PIPEDPEER_DDP_START_WAIT=60", "TEST_PREFIX=local"}
			if rank > 0 {
				env = append(env, "PIPEDPEER_DDP_LOCAL=1,2", "PIPEDPEER_DAEMON_URL="+node.URL)
			}
			out, err := run(env...).CombinedOutput()
			outs[rank], errs[rank] = string(out), err
		}()
	}
	wg.Wait()
	for rank, err := range errs {
		if err != nil {
			t.Fatalf("rank %d: %v\n%s", rank, err, outs[rank])
		}
	}
	if got := leadPosts.Load(); got != 5 {
		t.Fatalf("lead received %d syncs, want 5 (3 opening broadcasts + 1 per machine)", got)
	}
	if got := nodePosts.Load(); got != 2 {
		t.Fatalf("node daemon received %d syncs, want its 2 ranks' gradients", got)
	}
	check := exec.Command("python3", "-c", `
import os, sys, torch
d = sys.argv[1]
w = [torch.load(os.path.join(d, "local-%d.pt" % r))["w"] for r in range(3)]
assert torch.equal(w[0], w[1]) and torch.equal(w[0], w[2]), "ranks hold different weights"
print("LOCAL-OK")
`, dir)
	if out, err := check.CombinedOutput(); err != nil || !strings.Contains(string(out), "LOCAL-OK") {
		t.Fatalf("%v\n%s", err, out)
	}
}
