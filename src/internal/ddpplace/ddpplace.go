// Package ddpplace decides which nodes join a training ring.
//
// The rule it replaces was a gate: take GPU nodes if the job wants a GPU,
// otherwise CPU nodes, sort by reported CPU load, and use the first N. That
// gets two things wrong on a real cluster.
//
// It trusts advertised capacity. A node's core count says nothing about what
// is already running on it, or about any cap the daemon is under - every
// worker in the local test cluster advertises the host's 16 cores while being
// held to 8, 2 and 1 by its cgroup, and their measured throughput differs by
// nearly 13x.
//
// And in synchronous training every rank waits for the slowest, so a rank that
// is a fraction of the speed of the others does not add capacity, it sets the
// pace. Adding it can leave the run slower than it would have been without it.
// Nodes are therefore admitted only while admitting them helps.
package ddpplace

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/schedule"
)

// Candidate is a node that could take a rank.
type Candidate struct {
	NodeID string
	Host   string
	Port   int
	// Cores is what the node advertises. Kept only as a fallback for a node
	// that cannot be measured, and never preferred over a measurement.
	Cores int
	// MemBytes is what the node has free for the job.
	MemBytes int64
	// HasGPU marks a node whose accelerator would be used.
	HasGPU bool
	// Slots is how many ranks this node can host at once. One unless the node
	// has several accelerators: a machine with two GPUs running one rank
	// leaves one of them idle for the whole run, which is the single biggest
	// waste a heterogeneous cluster can produce.
	//
	// More than one slot on a CPU-only node is not a win and should not be
	// asked for - the ranks would divide the same cores between them and pay
	// the sync on top - so callers leave this at one there.
	//
	// Zero is read as one, so a caller that does not know stays on today's
	// behaviour.
	Slots int
}

// Choice is the outcome for one candidate.
type Choice struct {
	Candidate Candidate
	// Score is measured throughput, comparable only against other members of
	// the same run.
	Score float64
	// Weight is this rank's share of a step, summing to 1 across the ring.
	// Equal weights mean equal batches; unequal ones need the shim to size
	// batches per rank.
	Weight float64
	// Slot distinguishes ranks sharing a node, and is the accelerator index
	// the rank should pin. Two ranks on one box both taking device 0 is not
	// two ranks of work, it is two ranks fighting over one GPU.
	Slot int
}

// Rejection records a candidate that was not used, and why.
type Rejection struct {
	Candidate Candidate
	Reason    string
}

// Plan is what the ring should be.
type Plan struct {
	Chosen   []Choice
	Rejected []Rejection
	// Measured says whether the decision rests on measurement. False means
	// the scores are advertised core counts, which is a guess - callers
	// should say so rather than present the result as a measurement.
	Measured bool
}

// nominalStep is the notional per-step work the shares are cut from. Only the
// ratios matter; a large number keeps rounding from deciding who is included.
const nominalStep = 100000

// Options configures a placement decision.
type Options struct {
	// Max caps the ring size; 0 means every node worth using.
	Max int
	// Token authenticates the probe against peer daemons.
	Token string
	// ProbeMillis is how long each node is asked to measure for.
	ProbeMillis int
	// WorkingSetBytes is what a rank must be able to hold: model, optimiser
	// state and activations. A node with less than this does not train
	// slowly, it fails, so it is excluded rather than given a small share.
	WorkingSetBytes int64
	// ProportionalBatches says the shim will size each rank's batch by its
	// weight. Until it does, every rank computes the same batch and waits for
	// the slowest, which changes which nodes are worth having.
	ProportionalBatches bool
}

// slotRef remembers which node and accelerator a scheduled device came from.
type slotRef struct {
	cand  Candidate
	slot  int
	score float64
}

// slotID names a node's accelerator. The scheduler keys on device identity, so
// two ranks on one node need two names.
func slotID(nodeID string, slot int) string {
	if slot == 0 {
		return nodeID
	}
	return fmt.Sprintf("%s#%d", nodeID, slot)
}

// Select measures the candidates and returns those worth including.
//
// Nodes that cannot be measured are not discarded - a node that fails to
// answer one HTTP request may still train perfectly well - but they fall back
// to their advertised core count, and the plan says so.
func Select(ctx context.Context, cands []Candidate, opts Options) Plan {
	if len(cands) == 0 {
		return Plan{}
	}
	scores, measured := probeAll(ctx, cands, opts.Token, opts.ProbeMillis)

	// One device per slot, not per node. A node's slots are separate
	// accelerators, so each is its own thing to schedule - and whether the
	// second one is worth using is then decided by the same rule as any other
	// device, rather than assumed either way.
	devices := make([]schedule.Device, 0, len(cands))
	slotOf := map[string]slotRef{}
	for i, c := range cands {
		slots := c.Slots
		if slots < 1 {
			slots = 1
		}
		for slot := 0; slot < slots; slot++ {
			id := slotID(c.NodeID, slot)
			slotOf[id] = slotRef{cand: c, slot: slot, score: scores[i]}
			devices = append(devices, schedule.Device{
				ID:   id,
				Node: c.Host,
				Kind: kindOf(c),
				Rate: scores[i],
				// Every rank pays roughly the same startup: process launch,
				// closure materialisation, and joining the group. Charging a
				// uniform cost keeps this from silently becoming a proxy for
				// "nodes I have talked to recently".
				SetupSec: 0,
				// Ranks sharing a node share its free memory, so the
				// working-set gate has to see a share of it. Giving each slot
				// the whole node's figure would admit two ranks that between
				// them cannot fit, and the failure would arrive at run time
				// as an OOM rather than here as a refusal.
				MemBytes: c.MemBytes / int64(slots),
			})
		}
	}

	// Memory is a hard gate wherever it applies; the scheduler applies it.
	sized := schedule.Compute(schedule.Options{
		Items:           nominalStep,
		WorkingSetBytes: opts.WorkingSetBytes,
	}, devices)

	// How many of them are worth having is a different question, and it turns
	// on how a step is divided.
	//
	// With equal batches - which is what a DistributedSampler gives, and what
	// training does today - every rank computes the same number of samples and
	// waits for the slowest, so a step costs B/min(rate) and the ring's
	// throughput is k*min(rate) over the k fastest nodes. Adding a node that
	// is a fraction of the ring's speed multiplies the step time by more than
	// it adds in parallelism, and the run ends up slower than it would have
	// been without it. The best k is therefore just the k maximising
	// k*rate_k over nodes sorted fastest first - no tuning constant, and
	// exactly the "only the ones that give gain" rule.
	//
	// Once batches are sized per rank this stops being true: a slow rank then
	// takes a proportionally smaller batch and always helps a little, which is
	// what `sized` already models. The two are kept apart deliberately rather
	// than one being made to stand in for the other.
	plan := sized
	if !opts.ProportionalBatches {
		plan = equalBatchAdmission(sized, opts)
	}

	out := Plan{Measured: measured}
	var chosen []Choice
	for _, s := range plan.Shares {
		ref := slotOf[s.Device.ID]
		if s.Items <= 0 {
			out.Rejected = append(out.Rejected, Rejection{
				Candidate: ref.cand,
				Reason:    "too slow to contribute a share of a step",
			})
			continue
		}
		chosen = append(chosen, Choice{
			Candidate: ref.cand,
			Score:     ref.score,
			Weight:    float64(s.Items) / float64(nominalStep),
			Slot:      ref.slot,
		})
	}
	for _, r := range plan.Rejected {
		out.Rejected = append(out.Rejected, Rejection{Candidate: slotOf[r.Device.ID].cand, Reason: r.Reason})
	}

	// Fastest first, so a caller taking a prefix takes the best of them.
	// Ties break on slot so a node's first accelerator is preferred over its
	// second: a ring capped below what is available should leave a machine's
	// spare device idle rather than its primary one.
	sort.Slice(chosen, func(i, j int) bool {
		if chosen[i].Score != chosen[j].Score {
			return chosen[i].Score > chosen[j].Score
		}
		return chosen[i].Slot < chosen[j].Slot
	})
	if opts.Max > 0 && len(chosen) > opts.Max {
		for _, c := range chosen[opts.Max:] {
			out.Rejected = append(out.Rejected, Rejection{
				Candidate: c.Candidate,
				Reason:    fmt.Sprintf("ring capped at %d rank(s)", opts.Max),
			})
		}
		chosen = chosen[:opts.Max]
	}

	// Renormalise: weights have to sum to 1 over the ranks that actually run,
	// or the batch sizes derived from them do not add up to a step.
	var total float64
	for _, c := range chosen {
		total += c.Weight
	}
	if total > 0 {
		for i := range chosen {
			chosen[i].Weight /= total
		}
	}
	out.Chosen = chosen
	return out
}

// equalBatchAdmission keeps only the prefix of fastest devices that maximises
// k*rate_k, and returns the remainder as rejections.
func equalBatchAdmission(p schedule.Plan, opts Options) schedule.Plan {
	shares := make([]schedule.Share, 0, len(p.Shares))
	shares = append(shares, p.Shares...)
	sort.Slice(shares, func(i, j int) bool { return shares[i].Device.Rate > shares[j].Device.Rate })

	best, bestThroughput := 0, 0.0
	for k := 1; k <= len(shares); k++ {
		slowest := shares[k-1].Device.Rate
		if throughput := float64(k) * slowest; throughput > bestThroughput {
			best, bestThroughput = k, throughput
		}
	}

	out := schedule.Plan{Makespan: p.Makespan, Alone: p.Alone, Rejected: p.Rejected}
	for i, s := range shares {
		if i < best {
			// Equal batches: every admitted rank gets the same share.
			s.Items = nominalStep / best
			out.Shares = append(out.Shares, s)
			continue
		}
		out.Rejected = append(out.Rejected, schedule.Rejection{
			Device: s.Device,
			Reason: fmt.Sprintf("would set the pace for the whole ring: every rank waits "+
				"for the slowest, and %d rank(s) at this speed is slower than %d without it",
				i+1, best),
		})
	}
	return out
}

func kindOf(c Candidate) schedule.Kind {
	if c.HasGPU {
		return schedule.GPU
	}
	return schedule.CPU
}

// probeAll measures every candidate at once. Reports whether any measurement
// succeeded, since a plan built entirely from advertised core counts is a
// guess and should not be described as anything else.
func probeAll(ctx context.Context, cands []Candidate, token string, probeMillis int) ([]float64, bool) {
	if probeMillis <= 0 {
		probeMillis = 250
	}
	scores := make([]float64, len(cands))
	ok := make([]bool, len(cands))

	// One at a time, not all at once. Probing concurrently is quicker and is
	// what this did first, but it measures the wrong thing whenever the
	// candidates share hardware - several daemons on one host, which is an
	// ordinary deployment and exactly what the local test cluster is. Run
	// together they contend for the same cores, so each reads low by a
	// different amount and the ranking is decided by the contention rather
	// than by the machines. Sequential costs one probe window per node.
	for i, c := range cands {
		if s, err := probe(ctx, c, token, probeMillis); err == nil && s > 0 {
			scores[i], ok[i] = s, true
		}
	}

	var sum, n float64
	for i := range cands {
		if ok[i] {
			sum += scores[i]
			n++
		}
	}
	if n == 0 {
		// Nothing answered. Advertised cores are all there is; the caller is
		// told the plan is not measured.
		for i, c := range cands {
			if c.Cores > 0 {
				scores[i] = float64(c.Cores)
			} else {
				scores[i] = 1
			}
		}
		return scores, false
	}

	// Mixing a measured score with a core count would compare a number in the
	// billions against one under a hundred, and the unmeasured node would
	// never be used. Put the unmeasured ones on the same scale by assuming
	// they are average - optimistic on purpose, because a pessimistic guess
	// excludes a node that then never gets a chance to be measured.
	mean := sum / n
	for i := range cands {
		if !ok[i] {
			scores[i] = mean
		}
	}
	return scores, true
}

func probe(ctx context.Context, c Candidate, token string, millis int) (float64, error) {
	url := fmt.Sprintf("http://%s:%d/v1/bench?ms=%d", c.Host, c.Port, millis)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	if token != "" {
		req.Header.Set("X-Pipedpeer-Token", token)
	}
	client := &http.Client{Timeout: time.Duration(millis)*time.Millisecond + 5*time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("bench returned %s", resp.Status)
	}
	var body struct {
		Score float64 `json:"score"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	return body.Score, nil
}
