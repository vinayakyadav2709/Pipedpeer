// Package schedule decides which devices in a heterogeneous cluster should
// take part in a piece of work, and how much of it each should get.
//
// The behaviour it replaces was gated, not scheduled: pick GPU nodes if any
// exist, otherwise CPU nodes, and split the work evenly. That is wrong in both
// directions on a real cluster. An even split across a 20-core box and a
// 4-core box makes the fast machine wait for the slow one, so adding the
// second machine can make the job slower than not adding it. And "GPUs first"
// silently wastes every CPU on a node whose GPU is already busy, or ships work
// to a GPU across a link so slow that the transfer costs more than the compute
// saves.
//
// What this does instead: measure each device, then admit a device only when
// admitting it lowers the makespan. Devices are equalised on finish time
// rather than given equal shares, so a machine three times faster gets three
// times the work and everyone stops at once.
//
// The model is deliberately small enough to reason about. For a device with
// effective rate r (items/second, transfer already folded in) and fixed setup
// cost o (seconds before it can start at all), a share of w items finishes at
// o + w/r. Given a makespan T every device can absorb max(0, T-o)*r items, so
// the T that clears exactly W items is the root of
//
//	sum over devices of max(0, T - o_i) * r_i = W
//
// which is piecewise linear and increasing in T, so it solves directly. A
// device is worth using exactly when o_i < T: past that it cannot start before
// everyone else has finished. That gives an admission rule with no tuning
// constants, and it is why the result is never worse than running locally
// alone - the local device is always a candidate, and T can only fall as
// devices are added.
package schedule

import (
	"math"
	"sort"
)

// Kind distinguishes the two sorts of worker a node can offer. A node with a
// GPU offers both: its CPU pool does not stop existing because a GPU is
// present, and on a small model the CPUs are often worth using alongside it.
type Kind string

const (
	CPU Kind = "cpu"
	GPU Kind = "gpu"
)

// Device is one independently schedulable worker: one GPU, or one node's CPU
// pool.
type Device struct {
	ID   string
	Node string
	Kind Kind

	// Rate is measured throughput in items per second, from a calibration
	// burst rather than from any static guess about the hardware. A cluster
	// contains devices nobody has benchmarked, and the same GPU is a
	// different device when something else is already using it.
	Rate float64

	// BytesPerSec is the measured link bandwidth to this device, and
	// SetupSec what must be paid before it can start: shipping a closure it
	// does not have, spawning workers, warming a GPU context. Both are zero
	// for a local device.
	BytesPerSec float64
	SetupSec    float64

	// MemBytes is what this device can actually use for the job. A device
	// that cannot hold the working set is not a slow option, it is not an
	// option.
	MemBytes int64
}

// Share is one device's part of the work.
type Share struct {
	Device Device
	Items  int
}

// Plan is the result of scheduling: who works, on how much, and how long it
// should take.
type Plan struct {
	Shares []Share
	// Makespan is the predicted finish time in seconds, and Alone what the
	// best single device would have taken. Reported so callers can log the
	// decision, and so a regression shows up as a number rather than as a
	// job that is merely slower than it used to be.
	Makespan float64
	Alone    float64
	// Rejected records devices considered and turned down, with the reason.
	// A scheduler that silently drops half a cluster is indistinguishable
	// from one that never saw it.
	Rejected []Rejection
}

// Rejection explains why a device was not used.
type Rejection struct {
	Device Device
	Reason string
}

// Options describes the work to be spread.
type Options struct {
	// Items is the total unit count: dataset samples for training, chunk
	// items for a pool map.
	Items int
	// BytesPerItem is what has to cross the wire per item. Folded into each
	// device's effective rate, which is what makes a fast device behind a
	// slow link correctly unattractive.
	BytesPerItem float64
	// WorkingSetBytes is what a device needs to hold to take part at all -
	// model plus activations for training, or the payload for a map.
	WorkingSetBytes int64
	// MinItems is the smallest share worth giving anybody. Below it the
	// per-share overheads this model does not capture (a round trip, a
	// result merge) dominate.
	MinItems int
}

// Compute chooses the devices to use and their shares.
//
// The set is found by starting with every eligible device and dropping the
// ones that cannot start before the others finish, repeatedly, until it
// settles. Dropping a device raises the makespan, which can make another
// device not worth it either, so a single pass is not enough - and starting
// from all and shrinking finds the optimum for this model, where a greedy
// add-one-at-a-time pass does not.
func Compute(opts Options, devices []Device) Plan {
	plan := Plan{}
	if opts.Items <= 0 {
		return plan
	}

	eligible := make([]Device, 0, len(devices))
	for _, d := range devices {
		switch {
		case d.Rate <= 0:
			plan.Rejected = append(plan.Rejected, Rejection{d, "no measured throughput"})
		case opts.WorkingSetBytes > 0 && d.MemBytes < opts.WorkingSetBytes:
			plan.Rejected = append(plan.Rejected, Rejection{d, "not enough memory for the working set"})
		default:
			eligible = append(eligible, d)
		}
	}
	if len(eligible) == 0 {
		return plan
	}

	// Best-single-device time, for the "never slower than one machine"
	// comparison the caller reports.
	plan.Alone = math.Inf(1)
	for _, d := range eligible {
		if t := d.SetupSec + float64(opts.Items)/effRate(d, opts); t < plan.Alone {
			plan.Alone = t
		}
	}

	active := eligible
	var makespan float64
	for {
		makespan = solveMakespan(active, opts)
		kept := active[:0:0]
		var dropped []Device
		for _, d := range active {
			// A device that cannot begin before the others are done
			// contributes nothing; keeping it only adds coordination.
			if d.SetupSec < makespan {
				kept = append(kept, d)
			} else {
				dropped = append(dropped, d)
			}
		}
		if len(dropped) == 0 || len(kept) == 0 {
			break
		}
		for _, d := range dropped {
			plan.Rejected = append(plan.Rejected,
				Rejection{d, "cannot start before the rest of the cluster finishes"})
		}
		active = kept
	}

	plan.Shares = allocate(active, opts, makespan)

	// Shares too small to be worth a round trip are given back. Doing this
	// after the split rather than before keeps the rule from interacting with
	// admission: a device is dropped for being slow, not for being unlucky in
	// one rounding.
	if opts.MinItems > 1 {
		plan = dropTinyShares(plan, opts, active)
	}
	plan.Makespan = predict(plan.Shares, opts)
	return plan
}

// effRate folds the wire cost into a device's compute rate: a device twice as
// fast behind a link half as quick is not twice as useful.
func effRate(d Device, opts Options) float64 {
	if d.BytesPerSec <= 0 || opts.BytesPerItem <= 0 {
		return d.Rate // local, or nothing to ship
	}
	secPerItem := 1/d.Rate + opts.BytesPerItem/d.BytesPerSec
	if secPerItem <= 0 {
		return d.Rate
	}
	return 1 / secPerItem
}

// solveMakespan finds the T where the devices between them absorb exactly the
// work. Increasing in T and piecewise linear, with a breakpoint wherever a
// device becomes available, so it is solved by walking the breakpoints in
// order rather than by iterating.
func solveMakespan(devices []Device, opts Options) float64 {
	if len(devices) == 0 {
		return math.Inf(1)
	}
	type dev struct {
		setup float64
		rate  float64
	}
	ds := make([]dev, 0, len(devices))
	for _, d := range devices {
		ds = append(ds, dev{d.SetupSec, effRate(d, opts)})
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i].setup < ds[j].setup })

	work := float64(opts.Items)
	var sumRate float64
	prev := ds[0].setup
	done := 0.0
	for i := 0; i <= len(ds); i++ {
		var next float64
		if i < len(ds) {
			next = ds[i].setup
		} else {
			next = math.Inf(1)
		}
		// Everything cleared in [prev, next) by the devices already started.
		if sumRate > 0 {
			span := next - prev
			if capacity := sumRate * span; done+capacity >= work {
				return prev + (work-done)/sumRate
			} else {
				done += capacity
			}
		}
		if i < len(ds) {
			sumRate += ds[i].rate
			prev = next
		}
	}
	return math.Inf(1)
}

// allocate hands each device the work it can finish by the makespan, then
// settles rounding on the fastest device so the shares add up exactly.
func allocate(devices []Device, opts Options, makespan float64) []Share {
	shares := make([]Share, 0, len(devices))
	assigned := 0
	best, bestRate := -1, 0.0
	for _, d := range devices {
		r := effRate(d, opts)
		items := int((makespan - d.SetupSec) * r)
		if items < 0 {
			items = 0
		}
		if r > bestRate {
			bestRate, best = r, len(shares)
		}
		assigned += items
		shares = append(shares, Share{Device: d, Items: items})
	}
	if best >= 0 && assigned != opts.Items {
		shares[best].Items += opts.Items - assigned
		if shares[best].Items < 0 {
			shares[best].Items = 0
		}
	}
	return shares
}

// dropTinyShares removes shares below the caller's floor and re-splits what
// they were holding across the rest.
func dropTinyShares(plan Plan, opts Options, active []Device) Plan {
	var keep []Device
	for _, s := range plan.Shares {
		if s.Items >= opts.MinItems || len(active) == 1 {
			keep = append(keep, s.Device)
		} else {
			plan.Rejected = append(plan.Rejected,
				Rejection{s.Device, "share too small to be worth a round trip"})
		}
	}
	if len(keep) == 0 || len(keep) == len(plan.Shares) {
		return plan
	}
	plan.Shares = allocate(keep, opts, solveMakespan(keep, opts))
	return plan
}

// predict is the finish time actually implied by the shares handed out, which
// is not quite the solved makespan once rounding has moved items around.
func predict(shares []Share, opts Options) float64 {
	var worst float64
	for _, s := range shares {
		if s.Items == 0 {
			continue
		}
		if t := s.Device.SetupSec + float64(s.Items)/effRate(s.Device, opts); t > worst {
			worst = t
		}
	}
	return worst
}
