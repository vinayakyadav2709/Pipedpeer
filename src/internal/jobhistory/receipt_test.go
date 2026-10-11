package jobhistory

import (
	"strings"
	"testing"
)

func TestReceiptSummaryAndRendering(t *testing.T) {
	dir := t.TempDir()
	raw := `{"remote_items": 60, "local_items": 20, "dispatched_items": 60,
	  "parts": [{"kind": "pool", "items": 60, "ok": true, "ms": 1500, "ran_elsewhere": 60,
	             "ran_on_origin": 0, "peers": {"10.0.0.2:38080": 40, "10.0.0.3:38080": 20}}],
	  "decisions": [{"op": "pool.map", "shipped": true, "why": "60 items x 0.1s"},
	                {"op": "numpy.matmul", "shipped": false, "why": "here ~0.02s", "mb": 33}]}`
	if err := SaveReceipt(dir, []byte(raw)); err != nil {
		t.Fatal(err)
	}
	r, ok, err := ReadReceipt(dir)
	if err != nil || !ok {
		t.Fatalf("read: ok=%v err=%v", ok, err)
	}
	if got, want := r.Summary(), "60 of 80 items ran on other machines · 2 peer(s) · 1 shipped / 1 kept local"; got != want {
		t.Errorf("Summary = %q, want %q", got, want)
	}
	var b strings.Builder
	WriteReceipt(&b, r)
	out := b.String()
	for _, want := range []string{"Items: 80 total", "10.0.0.2:38080", "shipped     pool.map",
		"kept local  numpy.matmul", "(33 MB)"} {
		if !strings.Contains(out, want) {
			t.Errorf("receipt lacks %q:\n%s", want, out)
		}
	}
	// The busier peer is listed first.
	if strings.Index(out, "10.0.0.2") > strings.Index(out, "10.0.0.3") {
		t.Errorf("peers not ordered by work:\n%s", out)
	}

	if _, ok, _ := ReadReceipt(t.TempDir()); ok {
		t.Error("a run without a receipt reported one")
	}
}

func TestEmptyReceiptSaysNothingWasIntercepted(t *testing.T) {
	var b strings.Builder
	WriteReceipt(&b, Receipt{})
	if !strings.Contains(b.String(), "intercepted nothing") {
		t.Errorf("got %q", b.String())
	}
	if (Receipt{}).Summary() != "" {
		t.Error("an empty receipt should have no summary line")
	}
}

// Local cores race slow remote chunks, so an item can be computed in both
// places. The headline counts each item once, by where its kept result came
// from; a live run read "90 of 230 items" for a 200-item map before this.
func TestReceiptCountsRacedItemsOnce(t *testing.T) {
	r := Receipt{MappedItems: 200, RemoteUsed: 60, RemoteItems: 90, LocalItems: 140}
	if got, want := r.Summary(), "60 of 200 items ran on other machines"; got != want {
		t.Errorf("Summary = %q, want %q", got, want)
	}
	if r.Raced() != 30 {
		t.Errorf("Raced = %d, want 30", r.Raced())
	}
	var b strings.Builder
	WriteReceipt(&b, r)
	if !strings.Contains(b.String(), "on the running node    140") || !strings.Contains(b.String(), "30 computed on both") {
		t.Errorf("rendering:\n%s", b.String())
	}
}

// The shim runs an unshippable tail locally, so those items are already in
// local_items; unshippable only says why they stayed.
func TestUnshippableItemsAreNotCountedTwice(t *testing.T) {
	// 100 mapped: 20 measured here, 80 could not ship and also ran here.
	r := Receipt{MappedItems: 100, LocalItems: 100, Unshippable: 80}
	if r.Raced() != 0 {
		t.Errorf("Raced = %d, want 0", r.Raced())
	}
	old := Receipt{LocalItems: 100, Unshippable: 80}
	if old.Total() != 100 {
		t.Errorf("Total without mapped_items = %d, want 100", old.Total())
	}
	var b strings.Builder
	WriteReceipt(&b, r)
	if !strings.Contains(b.String(), "on the running node     20") || strings.Contains(b.String(), "computed on both") {
		t.Errorf("rendering:\n%s", b.String())
	}
}
