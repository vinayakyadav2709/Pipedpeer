package phone

import (
	"encoding/hex"
	"reflect"
	"testing"
)

// Fixtures from CPython 3.13: protocol 2 (GLOBAL), protocol 5 (STACK_GLOBAL
// with memo gets), and cloudpickle.dumps of `def f(x): return json.dumps(x)`
// defined in __main__ (json arrives through subimport).
const (
	pickleP2    = "800263636f6c6c656374696f6e730a4f726465726564446963740a710029527101580100000061710263646563696d616c0a446563696d616c0a71035801000000317104857105527106732e"
	pickleP5    = "80059558000000000000005d94288c0b636f6c6c656374696f6e73948c0b4f726465726564446963749493942952948c07646563696d616c948c07446563696d616c9493948c0132948594529468018c07436f756e7465729493947d9485945294652e"
	pickleCloud = "800595e7010000000000008c17636c6f75647069636b6c652e636c6f75647069636b6c65948c0e5f6d616b655f66756e6374696f6e9493942868008c0d5f6275696c74696e5f747970659493948c08436f6465547970659485945294284b014b004b004b014b034b03432e95005b00000000000000000052020000000000000000000000000000000000002200550035010000000000002400944e85948c046a736f6e948c0564756d70739486948c01789485948c083c737472696e673e948c01669468104b05430f80009414971a921a9841931dd00a1e944300942929749452947d94288c0b5f5f7061636b6167655f5f944e8c085f5f6e616d655f5f948c085f5f6d61696e5f5f94754e4e4e7494529468008c125f66756e6374696f6e5f7365747374617465949394681a7d947d9428681768108c0c5f5f7175616c6e616d655f5f9468108c0f5f5f616e6e6f746174696f6e735f5f947d948c0e5f5f6b7764656661756c74735f5f944e8c0c5f5f64656661756c74735f5f944e8c0a5f5f6d6f64756c655f5f9468188c075f5f646f635f5f944e8c0b5f5f636c6f737572655f5f944e8c175f636c6f75647069636b6c655f7375626d6f64756c6573945d948c0b5f5f676c6f62616c735f5f947d94680a68008c09737562696d706f7274949394680a85945294737586948652302e"
)

func unhex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestModules(t *testing.T) {
	src := "import os\nimport numpy as _np, torch.nn as nn\nfrom scipy.linalg import svd\nfrom . import x\n# import fake\ndef run(x):\n    import json\n    return x\n"
	for _, c := range []struct {
		name    string
		src     string
		pickles []string
		want    []string
	}{
		{"source", src, nil, []string{"json", "numpy", "os", "scipy", "torch"}},
		{"protocol 2", "", []string{pickleP2}, []string{"collections", "decimal"}},
		{"protocol 5 memo", "", []string{pickleP5}, []string{"collections", "decimal"}},
		{"cloudpickle subimport", "", []string{pickleCloud}, []string{"cloudpickle", "json"}},
		{"truncated", "", []string{pickleP5[:120]}, []string{"collections"}},
	} {
		var ps [][]byte
		for _, p := range c.pickles {
			ps = append(ps, unhex(t, p))
		}
		got := Modules(c.src, ps...)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestCanRunModules(t *testing.T) {
	stub(t, true, "3.12")
	oldFind := tryImports
	t.Cleanup(func() { tryImports = oldFind; importable = map[string]bool{} })
	importable = map[string]bool{}
	asked := 0
	tryImports = func(_ string, mods []string) (map[string]bool, error) {
		asked++
		got := map[string]bool{}
		for _, m := range mods {
			got[m] = m != "torch"
		}
		return got, nil
	}
	if !CanRun("", Need("3.12", true, "numpy", "os")) {
		t.Fatal("present modules refused")
	}
	if CanRun("", Need("3.12", true, "numpy", "torch")) {
		t.Fatal("missing torch accepted")
	}
	before := asked
	CanRun("", Need("3.12", true, "numpy"))
	if asked != before {
		t.Fatal("a present module was asked again")
	}
	if h := Need("3.12", false); h.Get(ImportsHeader) != "" {
		t.Fatal("no modules must send no header")
	}
}
