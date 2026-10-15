package daemonapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type ddpSyncRequest struct {
	Group string `json:"group"`
	Seq   int64  `json:"seq"`
	Rank  int    `json:"rank"`
	World int    `json:"world"`
	Data  string `json:"data"` // b64, opaque to the daemon

	// Reduce mode only.
	DType ddpDType `json:"dtype,omitempty"`
	Count int      `json:"count,omitempty"`
	// Scale is the int8 quantisation step this rank used. Each rank picks its
	// own from its own values, so it travels with the payload.
	Scale float64 `json:"scale,omitempty"`
	// Scales and Counts give each tensor its own quantisation step. One scale
	// for a whole model lets the largest layer set the step size for every
	// other, which costs most of the precision - see ddpreduce.go.
	Scales []float64 `json:"scales,omitempty"`
	Counts []int     `json:"counts,omitempty"`
	// Kind is "grads" or "weights". Only gradients are checked for ranks
	// duplicating work: the run opens by broadcasting the initial weights,
	// and those are identical by construction whenever the script seeds its
	// generator - which every sane training script does. Fingerprinting that
	// sync reported duplicated work on a correctly sharded run.
	Kind string `json:"kind,omitempty"`
	// SyncEvery is this rank's proposal for how often to average. Ranks
	// measure their own link and compute independently and so arrive at
	// different numbers; averaging at different points in each rank's step
	// sequence is not local SGD, it is nonsense, and it showed up as a
	// measurably worse final loss. The daemon hands everyone one answer.
	SyncEvery int `json:"sync_every,omitempty"`
	// StepMillis is this rank's mean compute time for one step, which every
	// rank already measures for its own sync tuning and which nothing has
	// ever collected.
	//
	// Gathered here it becomes the one measurement the scheduler has never
	// had: what a device is worth for this job, in units that compare across
	// devices. The throughput probe used for placement is an integer loop on
	// the CPU, so a GPU node's score is its CPU's score and a GPU cannot be
	// ranked against a CPU at all. This is the same model, the same batch,
	// on whatever hardware the rank actually has - comparable by
	// construction, and free, because the work is being done anyway.
	StepMillis float64 `json:"step_ms,omitempty"`
	// Samples is how many samples produced this gradient.
	//
	// With equal batches it is the same on every rank and changes nothing.
	// With proportional batches it is the whole correctness argument: a rank
	// training on twice the data computed its mean gradient over twice the
	// samples, and averaging the per-rank means as equals would give the
	// smaller shard twice the influence per sample it earned. The combined
	// gradient is sum(n_i * g_i) / sum(n_i).
	//
	// Absent (0) from a rank running the equal-batch code, which the
	// accumulator reads as a weight of 1 - reproducing the plain average
	// exactly, so a mixed-version ring is wrong in no new way.
	Samples int `json:"samples,omitempty"`
	// SyncMillis is what one gradient exchange has cost this rank on average.
	// The daemon needs it to decide whether a rank pays for itself: every
	// rank costs an exchange per step, and one contributing less compute than
	// it adds in sync is making the run slower.
	SyncMillis float64 `json:"sync_ms,omitempty"`
	// Sharded says this rank trains from a DataLoader the shim reshards by
	// hash. Only such a ring can take a new rank mid-run.
	Sharded bool `json:"sharded,omitempty"`
	// StartWait is how long, in seconds, the opening round waits for every
	// rank before the ring starts without the missing ones
	// (PIPEDPEER_DDP_START_WAIT). Zero: ddpStartWait.
	StartWait float64 `json:"start_wait,omitempty"`
	// Manifest describes the parameters behind the opening round, so a ring
	// whose ranks built different models stops with both described instead
	// of a byte-count error from the accumulator.
	Manifest *ddpManifest `json:"manifest,omitempty"`
	// Fixed: the user chose this ring (--ddp N); never shrink it.
	Fixed bool `json:"fixed,omitempty"`
	// SyncMode is "auto" from a rank that runs exact sync by default and can
	// switch to DiLoCo when the lead says so (U32); "exact" or "fast" when the
	// user chose with --ddp-sync. Absent from older shims: never switched.
	SyncMode string `json:"sync_mode,omitempty"`

	// Shard/Shards: one shard of a butterfly reduce (U26). Rank k's daemon
	// averages shard k of every rank's vector, so no one link carries the
	// whole ring (N-1 gradients in and out at the hub's lead). Shard 0 goes to
	// the lead with the full header and carries the ring's control; shards
	// above 0 are bare sums under their own key. Absent: the whole vector.
	Shard  int `json:"shard,omitempty"`
	Shards int `json:"shards,omitempty"`
	// Local/Upstream: ranks sharing this daemon's machine (multi-GPU) average
	// here first, and this daemon sends the lead at Upstream one contribution
	// standing for all of them: one network sync per machine.
	Local    []int  `json:"local,omitempty"`
	Upstream string `json:"upstream,omitempty"`
	// Members: the ranks a forwarded contribution stands for, each with its
	// own report, so the lead counts, measures and tells every one of them.
	Members []ddpMember `json:"members,omitempty"`
}

// ddpMember is one rank inside a machine's combined contribution.
type ddpMember struct {
	Rank       int     `json:"rank"`
	StepMillis float64 `json:"step_ms,omitempty"`
	SyncMillis float64 `json:"sync_ms,omitempty"`
	Samples    int     `json:"samples,omitempty"`
}

// ddpManifest is one rank's view of the model it syncs.
type ddpManifest struct {
	Names  []string `json:"names,omitempty"`
	Shapes [][]int  `json:"shapes"`
	DTypes []string `json:"dtypes"`
	Device string   `json:"device,omitempty"`
	Host   string   `json:"host,omitempty"`
}

// values is how many numbers the manifest's tensors hold.
func (m *ddpManifest) values() int {
	n := 0
	for _, sh := range m.Shapes {
		v := 1
		for _, d := range sh {
			v *= d
		}
		n += v
	}
	return n
}

// who names a rank for a person: "rank 1 on gpu1 (cuda)".
func (m *ddpManifest) who(rank int) string {
	s := fmt.Sprintf("rank %d", rank)
	if m.Host != "" {
		s += " on " + m.Host
	}
	if m.Device != "" {
		s += " (" + m.Device + ")"
	}
	return s
}

// manifestMismatch returns why two ranks' models cannot be averaged, or "".
func manifestMismatch(a *ddpManifest, ra int, b *ddpManifest, rb int) string {
	diff := ""
	if len(a.Shapes) != len(b.Shapes) {
		diff = fmt.Sprintf("%d tensors against %d", len(a.Shapes), len(b.Shapes))
	} else {
		for i := range a.Shapes {
			sameShape := fmt.Sprint(a.Shapes[i]) == fmt.Sprint(b.Shapes[i])
			sameType := i >= len(a.DTypes) || i >= len(b.DTypes) || a.DTypes[i] == b.DTypes[i]
			if sameShape && sameType {
				continue
			}
			name := fmt.Sprintf("tensor %d", i)
			if i < len(a.Names) && a.Names[i] != "" {
				name = a.Names[i]
			}
			da, db := "", ""
			if i < len(a.DTypes) && i < len(b.DTypes) {
				da, db = " "+a.DTypes[i], " "+b.DTypes[i]
			}
			diff = fmt.Sprintf("%s is %v%s against %v%s", name, a.Shapes[i], da, b.Shapes[i], db)
			break
		}
	}
	if diff == "" {
		return ""
	}
	return fmt.Sprintf("the ranks built different models: %s has %d tensors (%d values), "+
		"%s has %d tensors (%d values); first difference: %s. Every rank must build the "+
		"same model - a script that sizes its model by device builds a different one on "+
		"a CPU than on a GPU",
		a.who(ra), len(a.Shapes), a.values(), b.who(rb), len(b.Shapes), b.values(), diff)
}

// ddpMode is how a request wants its payload handled.
type ddpMode int

const (
	ddpModeJSON   ddpMode = iota // base64 envelope, blackboard
	ddpModeFrames                // binary frames, blackboard
	ddpModeReduce                // binary frames, averaged here
)

// readDDPRequest accepts all three forms.
func readDDPRequest(r *http.Request) (ddpSyncRequest, []byte, ddpMode, error) {
	ct := r.Header.Get("Content-Type")
	if ct != DDPFramesContentType && ct != DDPReduceContentType {
		var req ddpSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return req, nil, ddpModeJSON, err
		}
		// Deliberately not decoded. In blackboard mode whatever a rank
		// deposits is what its peers take home, and validating the encoding
		// would reject payloads the daemon has no business reading.
		return req, []byte(req.Data), ddpModeJSON, nil
	}
	mode := ddpModeFrames
	if ct == DDPReduceContentType {
		mode = ddpModeReduce
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ddpSyncRequest{}, nil, mode, err
	}
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		return ddpSyncRequest{}, nil, mode, fmt.Errorf("ddp frames body has no header line")
	}
	var req ddpSyncRequest
	if err := json.Unmarshal(body[:nl], &req); err != nil {
		return req, nil, mode, err
	}
	blob, _, err := readFrame(body[nl+1:])
	return req, blob, mode, err
}
