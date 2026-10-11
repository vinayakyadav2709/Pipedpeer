package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestCheckOnlyScanSpreads(t *testing.T) {
	src := strings.Join([]string{
		"from multiprocessing import Pool", // import, no call
		"import numpy as np",
		"# with Pool(4) as p:",           // comment
		"with ThreadPool(2) as tp: pass", // not a process pool
		"with Pool(4) as p:",             // line 5
		"    df = pd.read_csv('x.csv')",  // line 6
		"    a = np.linalg.inv(m)",       // line 7
		"    b = Pool (2)",               // repeat, first line wins
	}, "\n")
	got := scanSpreads(src)
	want := []spreadHit{
		{"multiprocessing.Pool", 5},
		{"pandas read_csv/read_parquet", 6},
		{"numpy linalg/dot/matmul", 7},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scanSpreads = %v, want %v", got, want)
	}
	if hits := scanSpreads("print('ok')\n"); len(hits) != 0 {
		t.Fatalf("plain script: got %v, want none", hits)
	}
}

func TestCheckOnlyUnpackaged(t *testing.T) {
	got := unpackagedImports([]string{"numpy", "notarealpkg", "otherthing"}, []string{"python3Packages.otherthing"})
	if !reflect.DeepEqual(got, []string{"notarealpkg"}) {
		t.Fatalf("unpackagedImports = %v, want [notarealpkg]", got)
	}
}

func TestCheckOnlyFit(t *testing.T) {
	if s := fit("short"); s != "short" {
		t.Fatalf("fit changed a short line: %q", s)
	}
	long := strings.Repeat("x", 100)
	if s := fit(long); len([]rune(s)) != checkLineWidth || !strings.HasSuffix(s, "…") {
		t.Fatalf("fit(long) = %q", s)
	}
}

func TestCheckOnlyHost(t *testing.T) {
	for in, want := range map[string]string{
		"root@10.0.0.2:22":  "10.0.0.2",
		"10.0.0.3:22":       "10.0.0.3",
		"root@[fe80::1]:22": "fe80::1",
		"pc-3":              "pc-3",
	} {
		if got := checkHost(in); got != want {
			t.Errorf("checkHost(%q) = %q, want %q", in, got, want)
		}
	}
}
