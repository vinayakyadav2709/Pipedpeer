package app

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestResolveMapTasksInputs(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.csv"), "1\n")
	writeFile(t, filepath.Join(dir, "b.csv"), "2\n")
	writeFile(t, filepath.Join(dir, "skip.txt"), "x\n")

	tasks, _, err := ResolveMapTasks(MapSpec{
		ScriptPath: "script.py",
		Inputs:     []string{filepath.Join(dir, "*.csv")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("want 2 tasks, got %d", len(tasks))
	}
}

func TestResolveMapTasksArgsFile(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "params.txt")
	writeFile(t, argsFile, "x=1\n\n# comment\nx=2\n")

	tasks, _, err := ResolveMapTasks(MapSpec{ScriptPath: "s.py", ArgsFile: argsFile})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("want 2 tasks, got %d", len(tasks))
	}
	if tasks[0].Args[0] != "x=1" {
		t.Fatalf("want x=1, got %q", tasks[0].Args[0])
	}
}

func TestSplitCSVPreservesHeader(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.csv")
	writeFile(t, src, "a,b\n1,2\n3,4\n5,6\n7,8\n")

	// parts:2 over 4 records → 2 rows per shard
	tasks, shardDir, err := ResolveMapTasks(MapSpec{ScriptPath: "s.py", SplitSource: src, Split: "parts:2"})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(shardDir)

	if len(tasks) != 2 {
		t.Fatalf("want 2 shards, got %d", len(tasks))
	}
	b1, err := os.ReadFile(tasks[0].Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(b1) != "a,b\n1,2\n3,4\n" {
		t.Fatalf("shard 0 wrong:\n%s", b1)
	}
	b2, err := os.ReadFile(tasks[1].Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(b2) != "a,b\n5,6\n7,8\n" {
		t.Fatalf("shard 1 wrong:\n%s", b2)
	}
}

func TestSplitLines(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.txt")
	writeFile(t, src, "l1\nl2\nl3\nl4\nl5\n")

	tasks, shardDir, err := ResolveMapTasks(MapSpec{ScriptPath: "s.py", SplitSource: src, Split: "rows:2"})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(shardDir)

	if len(tasks) != 3 {
		t.Fatalf("want 3 shards, got %d", len(tasks))
	}
	if len(tasks[0].Args) != 1 || tasks[0].Args[0] == "" {
		t.Fatal("shard arg missing")
	}
	if got := ShardEnvs(0, 3); got[0] != "PIPEDPEER_SHARD_ID=0" || got[1] != "PIPEDPEER_NUM_SHARDS=3" {
		t.Fatalf("bad shard envs: %v", got)
	}
}

func TestParseSplit(t *testing.T) {
	cases := []struct {
		in   string
		mode splitMode
		n    int
		err  bool
	}{
		{"", splitByParts, 1, false},
		{"rows:5", splitByRows, 5, false},
		{"parts:auto", splitByParts, 2, false}, // nodeCount=1 → 2
		{"parts:0", 0, 0, true},
		{"banana", 0, 0, true},
	}
	for _, c := range cases {
		m, n, err := parseSplit(c.in, 1)
		if c.err != (err != nil) {
			t.Fatalf("parseSplit(%q) err=%v, want err=%v", c.in, err, c.err)
		}
		if err == nil && (m != c.mode || n != c.n) {
			t.Fatalf("parseSplit(%q) = %v,%d want %v,%d", c.in, m, n, c.mode, c.n)
		}
	}
}

func TestSplitRefusesUnknownFormat(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.parquet")
	writeFile(t, src, "junk")
	_, _, err := ResolveMapTasks(MapSpec{ScriptPath: "s.py", SplitSource: src, Split: "rows:1"})
	if err == nil {
		t.Fatal("expected error for unsupported format")
	}
}
