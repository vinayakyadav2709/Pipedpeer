package jobhistory

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Receipt is the shim's own account of a run: how much work it handed to the
// cluster, where that work actually ran, and why each cost model shipped or
// kept what it was offered. Written by the job, carried back by the daemon.
type Receipt struct {
	RemoteItems     int `json:"remote_items"`
	LocalItems      int `json:"local_items"`
	DispatchedItems int `json:"dispatched_items"`
	RemoteFailures  int `json:"remote_failures"`
	Unshippable     int `json:"unshippable"`
	ShippedPickled  int `json:"shipped_pickled"`
	// MappedItems is every item handed to an intercepted map, once each, and
	// RemoteUsed how many of those had their kept result computed elsewhere.
	// RemoteItems and LocalItems count work done, which exceeds MappedItems
	// when local cores race a slow remote chunk and both finish.
	MappedItems int           `json:"mapped_items"`
	RemoteUsed  int           `json:"remote_used"`
	Parts       []ReceiptPart `json:"parts"`
	// ByWhere counts work done per place: "local" or "peer:<host:port>". It
	// must survive a decode and re-encode: attach rebuilt its summary from
	// this struct without it and printed "169 on 1 other (other machines
	// 169)" for work four peers did.
	ByWhere          map[string]int `json:"by_where,omitempty"`
	Decisions        []Decision     `json:"decisions"`
	DecisionsDropped int            `json:"decisions_dropped"`
}

// ReceiptPart is one dispatch to the cluster.
type ReceiptPart struct {
	Kind         string         `json:"kind"`
	Items        int            `json:"items"`
	OK           bool           `json:"ok"`
	Error        string         `json:"error,omitempty"`
	Ms           float64        `json:"ms"`
	RanElsewhere int            `json:"ran_elsewhere"`
	RanOnOrigin  int            `json:"ran_on_origin"`
	Via          string         `json:"via,omitempty"`
	Peers        map[string]int `json:"peers,omitempty"`
}

// Decision is one cost-model verdict.
type Decision struct {
	Op      string  `json:"op"`
	Shipped bool    `json:"shipped"`
	Why     string  `json:"why"`
	MB      float64 `json:"mb"`
}

const receiptFile = "receipt.json"

// SaveReceipt stores a run's receipt beside its record.
func SaveReceipt(dir string, raw []byte) error {
	return os.WriteFile(filepath.Join(dir, receiptFile), raw, 0644)
}

// ReadReceipt loads a run's receipt; ok is false when the run has none.
func ReadReceipt(dir string) (Receipt, bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, receiptFile))
	if os.IsNotExist(err) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, err
	}
	var r Receipt
	if err := json.Unmarshal(b, &r); err != nil {
		return Receipt{}, false, err
	}
	return r, true, nil
}

// Total is every item the shim accounted for, each once.
func (r Receipt) Total() int {
	if r.MappedItems > 0 {
		return r.MappedItems
	}
	// Unshippable items ran here and are already in LocalItems.
	return r.RemoteItems + r.LocalItems
}

// FromElsewhere is how many items' results came from another machine.
func (r Receipt) FromElsewhere() int {
	if r.MappedItems > 0 {
		return min(r.RemoteUsed, r.MappedItems)
	}
	return r.RemoteItems
}

// Raced is how many items were computed twice, once here and once elsewhere.
func (r Receipt) Raced() int {
	if r.MappedItems == 0 {
		return 0
	}
	return max(0, r.RemoteItems+r.LocalItems-r.MappedItems)
}

// Summary is a one-line account for the end of a run, or "" when the shim
// had nothing to intercept.
func (r Receipt) Summary() string {
	if r.Total() == 0 && len(r.Decisions) == 0 {
		return ""
	}
	parts := []string{fmt.Sprintf("%d of %d items ran on other machines", r.FromElsewhere(), r.Total())}
	if peers := r.peerItems(); len(peers) > 0 {
		parts = append(parts, fmt.Sprintf("%d peer(s)", len(peers)))
	}
	shipped, kept := 0, 0
	for _, d := range r.Decisions {
		if d.Shipped {
			shipped++
		} else {
			kept++
		}
	}
	if shipped+kept > 0 {
		parts = append(parts, fmt.Sprintf("%d shipped / %d kept local", shipped, kept))
	}
	if r.RemoteFailures > 0 {
		parts = append(parts, fmt.Sprintf("%d remote failure(s), redone locally", r.RemoteFailures))
	}
	return strings.Join(parts, " · ")
}

func (r Receipt) peerItems() map[string]int {
	out := map[string]int{}
	for _, p := range r.Parts {
		for peer, n := range p.Peers {
			out[peer] += n
		}
	}
	return out
}

// WriteReceipt renders a receipt for `pipedpeer job --receipt`.
func WriteReceipt(w io.Writer, r Receipt) {
	total := r.Total()
	if total == 0 && len(r.Decisions) == 0 {
		fmt.Fprintln(w, "The shim intercepted nothing in this run: no pool, joblib, numpy or pandas")
		fmt.Fprintln(w, "call was big enough to consider. Everything ran as plain Python on one node.")
		return
	}
	elsewhere := r.FromElsewhere()
	fmt.Fprintf(w, "Items: %d total\n", total)
	fmt.Fprintf(w, "  on other machines   %6d  %s\n", elsewhere, bar(elsewhere, total, 30))
	fmt.Fprintf(w, "  on the running node %6d  %s\n", total-elsewhere-r.Unshippable, bar(total-elsewhere-r.Unshippable, total, 30))
	if n := r.Raced(); n > 0 {
		fmt.Fprintf(w, "  (%d computed on both: idle local cores raced the slowest remote chunks)\n", n)
	}
	if r.Unshippable > 0 {
		fmt.Fprintf(w, "  could not ship      %6d  %s\n", r.Unshippable, bar(r.Unshippable, total, 30))
	}
	if r.ShippedPickled > 0 {
		fmt.Fprintf(w, "  (%d sent by value: lambdas, closures, partials or methods)\n", r.ShippedPickled)
	}
	if r.RemoteFailures > 0 {
		fmt.Fprintf(w, "  %d dispatch(es) failed and were redone locally\n", r.RemoteFailures)
	}

	if peers := r.peerItems(); len(peers) > 0 {
		names := make([]string, 0, len(peers))
		for p := range peers {
			names = append(names, p)
		}
		sort.Slice(names, func(i, j int) bool { return peers[names[i]] > peers[names[j]] })
		fmt.Fprintln(w, "\nBy peer:")
		for _, p := range names {
			fmt.Fprintf(w, "  %-22s %6d  %s\n", p, peers[p], bar(peers[p], total, 30))
		}
	}

	if len(r.Parts) > 0 {
		fmt.Fprintln(w, "\nDispatches:")
		fmt.Fprintf(w, "  %-6s %6s %9s %9s %8s  %s\n", "KIND", "ITEMS", "ELSEWHERE", "ON ORIGIN", "TIME", "")
		for _, p := range r.Parts {
			note := ""
			if !p.OK {
				note = "failed: " + p.Error
			}
			fmt.Fprintf(w, "  %-6s %6d %9d %9d %8s  %s\n", p.Kind, p.Items, p.RanElsewhere, p.RanOnOrigin,
				fmtMs(int64(p.Ms)), note)
		}
	}

	if len(r.Decisions) > 0 {
		fmt.Fprintln(w, "\nCost model decisions:")
		for _, d := range r.Decisions {
			verdict := "kept local"
			if d.Shipped {
				verdict = "shipped   "
			}
			size := ""
			if d.MB > 0 {
				size = fmt.Sprintf(" (%.0f MB)", d.MB)
			}
			fmt.Fprintf(w, "  %s  %-20s %s%s\n", verdict, d.Op, d.Why, size)
		}
		if r.DecisionsDropped > 0 {
			fmt.Fprintf(w, "  … and %d more not recorded\n", r.DecisionsDropped)
		}
	}
}

func bar(n, total, width int) string {
	if total <= 0 || n <= 0 {
		return ""
	}
	w := n * width / total
	if w < 1 {
		w = 1
	}
	return strings.Repeat("█", w)
}
