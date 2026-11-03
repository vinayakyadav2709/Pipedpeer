package peercache

import (
	"strings"
	"testing"

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
)

func one(t *testing.T, name string) PathInfo {
	t.Helper()
	got, err := ParsePathInfo(fixture(t, name))
	if err != nil || len(got) != 1 {
		t.Fatalf("%s: %v, %v", name, got, err)
	}
	return got[0]
}

func TestParsePathInfo(t *testing.T) {
	bash := "/nix/store/" + bashHash + "-bash-5.3p15"
	want11 := PathInfo{
		Path:    bash,
		NarHash: "sha256:0i3p1ir4vwkdssmrnacn1ybxsa18fvp7avhs4ncinwlhr1dfp634",
		NarSize: 1856848,
		References: []string{
			"/nix/store/0d8g8n0a11v6f5m2h416ajyxmnkwc3md-glibc-2.42-67", bash},
		Deriver: "/nix/store/yxs8g9wf1nmphw5xspgyykg37hwp8vgf-bash-5.3p15.drv",
		Sigs:    []string{"cache.nixos.org-1:L1O9X5c1uPJAdtUHF5tOCnTl7chbQNAoqw7DBBP6ehZdOCrfFTL1UpKAKHTrHZmVYgBIfJLiBYzPmIrwEsa0Cw=="},
	}
	same := func(a, b PathInfo) bool {
		return a.Path == b.Path && a.NarHash == b.NarHash && a.NarSize == b.NarSize && a.Deriver == b.Deriver &&
			a.CA == b.CA && strings.Join(a.References, ",") == strings.Join(b.References, ",") &&
			strings.Join(a.Sigs, ",") == strings.Join(b.Sigs, ",")
	}
	if got := one(t, "pathinfo-1.1.json"); !same(got, want11) {
		t.Errorf("1.1: %+v", got)
	}
	// 1.3: format 2 has basenames; they come back as full paths.
	if got := one(t, "pathinfo-1.3.json"); !same(got, want11) {
		t.Errorf("1.3: %+v", got)
	}
	wantCA := PathInfo{
		Path:    patchPath,
		NarHash: "sha256:10w02iccr1pnkrfnhxb0ghfizq42pq6nxgplrdgl9f0bfazxdli2",
		NarSize: 1656,
		CA:      "fixed:r:sha256:10w02iccr1pnkrfnhxb0ghfizq42pq6nxgplrdgl9f0bfazxdli2",
	}
	if got := one(t, "pathinfo-1.2.json"); !same(got, wantCA) || got.Deriver != "" || len(got.References) != 0 {
		t.Errorf("1.2: %+v", got)
	}
	// 1.4: format 2 CA object gives the same string as 1.2.
	if got := one(t, "pathinfo-1.4.json"); got.CA != wantCA.CA || got.Path != patchPath {
		t.Errorf("1.4: %+v", got)
	}
	// 1.5: array form with a nix32 narHash.
	arr := `[{"path":"` + bash + `","narHash":"sha256:0i3p1ir4vwkdssmrnacn1ybxsa18fvp7avhs4ncinwlhr1dfp634","narSize":1856848,"references":[],"deriver":null,"signatures":[]}]`
	if got, err := ParsePathInfo([]byte(arr)); err != nil || len(got) != 1 || got[0].NarHash != want11.NarHash || got[0].Path != bash {
		t.Errorf("1.5: %+v %v", got, err)
	}
}

func TestParsePathInfoSkipsBad(t *testing.T) {
	bash := "/nix/store/" + bashHash + "-bash-5.3p15"
	good := `"` + bash + `":{"narHash":"sha256-ZJjrWsiQchuZJRpude52KCjdlw+WKZur1m3yTXIMd0Q=","narSize":5,"references":[],"signatures":[]}`
	cases := []struct {
		name, in string
		want     int
	}{
		{"1.7 narSize 0", `{"` + bash + `":{"narHash":"sha256-ZJjrWsiQchuZJRpude52KCjdlw+WKZur1m3yTXIMd0Q=","narSize":0}}`, 0},
		{"1.7 narSize missing", `{"` + bash + `":{"narHash":"sha256-ZJjrWsiQchuZJRpude52KCjdlw+WKZur1m3yTXIMd0Q="}}`, 0},
		{"1.7 bad hash", `{"` + bash + `":{"narHash":"sha256-nope","narSize":5}}`, 0},
		{"1.4 unknown method", `{"info":{"x":{"narHash":"sha256-ZJjrWsiQchuZJRpude52KCjdlw+WKZur1m3yTXIMd0Q=","narSize":5,"ca":{"hash":"sha256-ZJjrWsiQchuZJRpude52KCjdlw+WKZur1m3yTXIMd0Q=","method":"git"}}}}`, 0},
		{"bad entry beside a good one", `{"/nix/store/bad":{"narSize":0},` + good + `}`, 1},
	}
	for _, c := range cases {
		got, err := ParsePathInfo([]byte(c.in))
		if err != nil || len(got) != c.want {
			t.Errorf("%s: %d entries, err %v, want %d", c.name, len(got), err, c.want)
		}
	}
	for _, in := range []string{"", "not json", "{"} {
		if _, err := ParsePathInfo([]byte(in)); err == nil {
			t.Errorf("%q: want an error", in)
		}
	}
}

func TestNarinfo(t *testing.T) {
	pi := one(t, "pathinfo-1.1.json")
	ni, err := Narinfo(pi)
	if err != nil {
		t.Fatal(err)
	}
	text := ni.String()
	if !strings.Contains(text, "URL: nar/"+bashHash+".nar.zst\n") || !strings.Contains(text, "Compression: zstd\n") ||
		strings.Contains(text, "FileHash") || strings.Contains(text, "FileSize") {
		t.Errorf("rendering:\n%s", text)
	}
	back, err := narinfo.Parse(strings.NewReader(text))
	if err != nil {
		t.Fatalf("round trip: %v\n%s", err, text)
	}
	real21 := parseNI(t, string(fixture(t, "narinfo-2.1.txt")))
	if back.StorePath != real21.StorePath || back.NarHash.String() != real21.NarHash.String() ||
		back.NarSize != real21.NarSize || back.Deriver != real21.Deriver ||
		strings.Join(back.References, " ") != strings.Join(real21.References, " ") ||
		len(back.Signatures) != 1 || back.Signatures[0].String() != real21.Signatures[0].String() {
		t.Errorf("differs from cache.nixos.org:\n%s", text)
	}
	// The real signature verifies over our fingerprint: the cache serves what nix can check.
	if err := Accept(back, bashHash, []signature.PublicKey{key(t, nixosKey)}); err != nil {
		t.Errorf("Accept of the rendered narinfo: %v", err)
	}
	// CA path (2.2): carried through, no Sig.
	cai, err := Narinfo(one(t, "pathinfo-1.2.json"))
	if err != nil || !strings.Contains(cai.String(), "CA: fixed:r:sha256:10w0") || strings.Contains(cai.String(), "Sig:") {
		t.Errorf("CA narinfo: %v\n%s", err, cai.String())
	}
}
