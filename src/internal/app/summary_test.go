package app

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

func TestSummaryRendering(t *testing.T) {
	stderr := "warn: x\n" + receiptPrefix + `{"remote_items":80,"local_items":40,"by_where":{"local":40,"peer:10.88.0.12:38080":40,"peer:10.88.0.13:38080":40}}` + "\nmore\n"
	s := summary{
		Host:       "10.88.0.1",
		BuildTime:  2100 * time.Millisecond,
		Closure:    closureLine(&daemonctl.UploadResponse{Closure: "partial", SentPaths: 7, TotalPaths: 43, SentBytes: 28_600_000}, 0, false),
		ScriptRan:  true,
		ScriptTime: 12300 * time.Millisecond,
		Peak:       412_000_000,
		Stderr:     stderr,
		Files:      &daemonctl.ResultManifest{New: []string{"a"}, Updated: []string{"b", "c"}},
		Total:      14900 * time.Millisecond,
	}
	names := map[string]string{"10.88.0.12:38080": "pc-2", "10.88.0.13:38080": "pc-3"}
	lines, unresolved := summaryLines(s, "laptop-a", names)
	got := strings.Join(lines, "\n")
	want := strings.Join([]string{
		"  ran on       laptop-a (10.88.0.1)",
		"  environment  built in 2.1 s",
		"  closure      sent 7 of 43 paths (29 MB)",
		"  script       12.3 s · peak memory 412 MB",
		"  spread       120 items: 40 on laptop-a, 80 on 2 others (pc-2 40, pc-3 40)",
		"  files back   3 (1 new, 2 updated)",
		"  total        14.9 s",
	}, "\n")
	if got != want || len(unresolved) != 0 {
		t.Fatalf("got\n%s\nwant\n%s\nunresolved %v", got, want, unresolved)
	}

	// Cancelled, nothing ran remotely, a peer with no known name.
	s = summary{Host: "h", Total: 6200 * time.Millisecond, Err: fmt.Errorf("x: %w", context.Canceled),
		Stderr: receiptPrefix + `{"by_where":{"peer:127.0.0.1:4000":5}}` + "\n"}
	lines, unresolved = summaryLines(s, "h", nil)
	got = strings.Join(lines, "\n")
	if !strings.Contains(got, "ran on       h\n") || !strings.Contains(got, "5 items: 5 on 1 other (127.0.0.1:4000 5)") ||
		!strings.HasSuffix(got, "cancelled after 6.2 s") || len(unresolved) != 1 {
		t.Fatalf("got\n%s\nunresolved %v", got, unresolved)
	}
}

func TestClosureLineCold(t *testing.T) {
	got := closureLine(&daemonctl.UploadResponse{Closure: "full", SentBytes: 206_000_000, SendTime: 4100 * time.Millisecond}, 0, true)
	if got != "sent 206 MB in 4.1 s (cold)" {
		t.Fatalf("got %q", got)
	}
	if got := closureLine(&daemonctl.UploadResponse{Closure: "on-node"}, 0, false); got != "already on the node" {
		t.Fatalf("got %q", got)
	}
}

func TestUnpackagedHonoursPkgFlag(t *testing.T) {
	got := unpackaged([]string{"numpy", "notarealmodule_xyz", "othermissing_xyz"}, []string{"python3Packages.othermissing_xyz"})
	if len(got) != 1 || got[0] != "notarealmodule_xyz" {
		t.Fatalf("got %v", got)
	}
}

func TestMemLimitAlwaysCaps(t *testing.T) {
	if got := memLimitFor(0); got != noEstimateMemLimit || got <= 0 {
		t.Fatalf("no estimate: got %d, want the default cap", got)
	}
	if got := memLimitFor(100 << 20); got != 512<<20 {
		t.Fatalf("small estimate: got %d, want the 512 MiB floor", got)
	}
	if got := memLimitFor(1 << 30); got != 2<<30 {
		t.Fatalf("1 GiB estimate: got %d, want double", got)
	}
}
