package daemonctl

import (
	"reflect"
	"strings"
	"testing"
)

func TestReceiptFilterSplitAcrossChunks(t *testing.T) {
	var f receiptFilter
	chunks := []string{
		"warming up\n[pipe",
		"dpeer] receipt {\"remote_",
		"items\":40}\nafter\n",
	}
	var shown strings.Builder
	var got []string
	for _, c := range chunks {
		s, r := f.filter(c)
		shown.WriteString(s)
		got = append(got, r...)
	}
	shown.WriteString(f.flush())
	if shown.String() != "warming up\nafter\n" {
		t.Fatalf("shown = %q", shown.String())
	}
	want := []string{`[pipedpeer] receipt {"remote_items":40}`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("receipts = %q, want %q", got, want)
	}
}

func TestReceiptFilterPassesProgressBarsAtOnce(t *testing.T) {
	var f receiptFilter
	// Each redraw must print the moment it arrives, not at the next newline.
	for _, c := range []string{" 10%|#  |\r", " 50%|## |\r", "100%|###|\n"} {
		s, r := f.filter(c)
		if s != c || r != nil {
			t.Fatalf("filter(%q) = %q, %q", c, s, r)
		}
	}
	// A receipt right after an unterminated bar is still caught.
	s, r := f.filter(" 99%|##|\r[pipedpeer] receipt {}\n")
	if s != " 99%|##|\r" || len(r) != 1 {
		t.Fatalf("after bar: shown %q receipts %q", s, r)
	}
}

func TestReceiptFilterLeavesOtherShimLinesAndMidLineText(t *testing.T) {
	var f receiptFilter
	in := "[pipedpeer] remote ok: 3 items\nsay [pipedpeer] receipt not a line start\n"
	s, r := f.filter(in)
	if s != in || r != nil {
		t.Fatalf("shown %q receipts %q", s, r)
	}
	// Text that only looked like a receipt start comes out at the end.
	if s, _ := f.filter("[pipedpeer] rec"); s != "" {
		t.Fatalf("candidate printed early: %q", s)
	}
	if got := f.flush(); got != "[pipedpeer] rec" {
		t.Fatalf("flush = %q", got)
	}
}
