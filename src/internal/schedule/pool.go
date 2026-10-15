package schedule

import "math"

// Pool-map pacing (research R5/R8). A part is one node's share of a chunk.

// PartSec is the target length of one part on a node: long enough that the
// node's fixed cost per request (setup) and the round trip each stay under
// 10% of it, and never under a quarter second.
func PartSec(setup, rtt float64) float64 {
	return max(9*setup, 9*rtt, 0.25)
}

// Depth is how many parts a node should hold at once (Little's law): the one
// it is running plus enough queued to cover the lead time from dispatch to
// first compute, so it never idles between parts. Capped at 3: a deeper queue
// binds work to a node early and fattens the tail.
func Depth(lead, part float64) int {
	if part <= 0 {
		return 1
	}
	d := 1 + int(math.Ceil(max(0, lead)/part))
	return min(max(d, 1), 3)
}

// Backlog is the predicted seconds of work a node still has: each running
// part's predicted length minus how long it has run, floored at zero. The
// split adds it to the node's setup cost, so a busy node starts late and is
// given less of the next chunk (Dask's "start soonest").
func Backlog(predicted, elapsed []float64) float64 {
	var q float64
	for i, p := range predicted {
		if i < len(elapsed) {
			q += max(0, p-elapsed[i])
		}
	}
	return q
}

// BackupWorth reports whether a second copy of a late part should start on
// another device, and the expected seconds it saves (R8 step 7, Mantri):
// expected is the part's predicted length on its node, elapsed how long it
// has run, fresh what the backup device needs for the whole part. An overdue
// part is assumed to need as long again as it was expected to take. The
// saving must beat the backup's own setup or 0.2 s, whichever is larger.
func BackupWorth(expected, elapsed, fresh, freshSetup float64) (bool, float64) {
	rem := expected - elapsed
	if elapsed > expected {
		rem = max(elapsed-expected, expected)
	}
	gain := rem - fresh
	return gain > max(freshSetup, 0.2), gain
}
