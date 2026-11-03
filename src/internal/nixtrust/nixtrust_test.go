package nixtrust

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/nixsign"
)

func newKey(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return nixsign.PublicKeyFor(pub)
}

// The helper runs as root on arguments any user can pass, and what it accepts
// lands in nix.conf. Only a key in exactly the form pipedpeer makes gets in.
func TestOnlyAWellFormedPipedpeerKeyIsAccepted(t *testing.T) {
	good := newKey(t)
	if err := ValidKey(good); err != nil {
		t.Fatalf("a real key was refused: %v", err)
	}
	other := newKey(t)
	name, b64, _ := strings.Cut(good, ":")
	_, otherB64, _ := strings.Cut(other, ":")
	bad := map[string]string{
		"newline":             good + "\nrequire-sigs = false",
		"embedded newline":    name + ":" + b64[:20] + "\n" + b64[20:],
		"extra option":        good + " require-sigs = false",
		"second key":          good + " " + other,
		"leading space":       " " + good,
		"empty":               "",
		"no name":             b64,
		"short key":           name + ":" + b64[:40] + "=",
		"long key":            name + ":" + b64[:43] + "AAAA=",
		"bad charset in name": "pipedpeer-ABCDEF0123456789:" + b64,
		"foreign name":        "cache.nixos.org-1:" + b64,
		"name of another key": name + ":" + otherB64,
		"bad base64 char":     name + ":" + b64[:42] + "-=",
		"nul":                 good + "\x00",
		"option-looking":      "--option",
	}
	for why, k := range bad {
		if err := ValidKey(k); err == nil {
			t.Errorf("%s: accepted %q", why, k)
		}
	}
}

func TestTheKeysFileIsRewrittenDedupedAndRestartsOnlyOnChange(t *testing.T) {
	file := filepath.Join(t.TempDir(), "keys.conf")
	a, b := newKey(t), newKey(t)
	restarts := 0
	restart := func() error { restarts++; return nil }
	step := func(args ...string) {
		t.Helper()
		if err := run(args, file, restart); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	read := func() string {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}

	step("add", a, a, b)
	if got, want := read(), FileContent([]string{a, b}); got != want {
		t.Fatalf("file:\n%s\nwant:\n%s", got, want)
	}
	if restarts != 1 {
		t.Fatalf("restarts = %d, want 1", restarts)
	}
	step("add", b)
	if restarts != 1 {
		t.Errorf("re-adding a known key restarted nix-daemon")
	}
	step("remove", a)
	if got := read(); strings.Contains(got, a) || !strings.Contains(got, b) || restarts != 2 {
		t.Errorf("after remove: restarts=%d file:\n%s", restarts, got)
	}
	step("remove", b)
	if got := read(); strings.Contains(got, keysSetting) || restarts != 3 {
		t.Errorf("an empty set should leave no setting line: restarts=%d file:\n%s", restarts, got)
	}

	// A hand-edited file loses what the helper would not have written.
	if err := os.WriteFile(file, []byte(keysSetting+" = "+a+" junk\nrequire-sigs = false\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	step("add", a)
	if got := read(); got != FileContent([]string{a}) {
		t.Errorf("foreign lines survived:\n%s", got)
	}

	for _, args := range [][]string{nil, {"add"}, {"set", a}, {"add", a, "--option"}} {
		if err := run(args, file, restart); err == nil {
			t.Errorf("%q was accepted", args)
		}
	}
}

func TestTheSudoersRuleNamesOnlyTheHelper(t *testing.T) {
	if got, want := SudoersRule("alice"), "alice ALL=(root) NOPASSWD: /usr/local/libexec/pipedpeer-nix-trust *\n"; got != want {
		t.Errorf("rule %q, want %q", got, want)
	}
	for _, u := range []string{"alice", "bob_2", "svc-user"} {
		if err := ValidUser(u); err != nil {
			t.Errorf("%s: %v", u, err)
		}
	}
	for _, u := range []string{"", "ALL", "a b", "a,b", "%wheel", "a\nroot ALL=(ALL) ALL", "#0"} {
		if err := ValidUser(u); err == nil {
			t.Errorf("%q accepted into sudoers", u)
		}
	}
}

func TestCallsGoThroughSudoWithoutAPrompt(t *testing.T) {
	ge, ex := geteuid, execute
	t.Cleanup(func() { geteuid, execute = ge, ex })
	var ran [][]string
	execute = func(a []string) ([]byte, error) { ran = append(ran, a); return nil, nil }

	geteuid = func() int { return 1000 }
	k := newKey(t)
	if err := Add([]string{k}); err != nil {
		t.Fatal(err)
	}
	geteuid = func() int { return 0 }
	if err := Remove([]string{k}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"sudo -n " + HelperPath + " add " + k,
		HelperPath + " remove " + k,
	}
	for i, w := range want {
		if i >= len(ran) || strings.Join(ran[i], " ") != w {
			t.Errorf("call %d = %v, want %s", i, ran, w)
		}
	}
}

func TestFileContentCarriesTheSubstituterAndParseKeepsIt(t *testing.T) {
	k := newKey(t)
	body := FileContent([]string{k})
	if !strings.Contains(body, keysSetting+" = "+k) || !strings.Contains(body, "extra-trusted-substituters = http://127.0.0.1:4242/nix-cache") {
		t.Fatalf("content:\n%s", body)
	}
	if got := parseKeys(body); !slices.Equal(got, []string{k}) {
		t.Errorf("parseKeys = %v", got)
	}
	old := "# x\n" + keysSetting + " = " + k + "\n"
	if got := parseKeys(old); !slices.Equal(got, []string{k}) {
		t.Errorf("old file: parseKeys = %v", got)
	}
}

func TestSyncRewritesOnlyWhenContentDiffers(t *testing.T) {
	file := filepath.Join(t.TempDir(), "keys.conf")
	k1, k2 := newKey(t), newKey(t)
	restarts := 0
	restart := func() error { restarts++; return nil }
	if err := os.WriteFile(file, []byte(keysSetting+" = "+k1+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"sync"}, file, restart); err != nil {
		t.Fatal(err)
	}
	body, _ := os.ReadFile(file)
	if string(body) != FileContent([]string{k1}) || restarts != 1 {
		t.Fatalf("after sync of old file: restarts=%d\n%s", restarts, body)
	}
	if err := run([]string{"sync"}, file, restart); err != nil || restarts != 1 {
		t.Errorf("sync of a current file: err=%v restarts=%d", err, restarts)
	}
	if err := run([]string{"add", k2}, file, restart); err != nil || restarts != 2 {
		t.Fatalf("add: err=%v restarts=%d", err, restarts)
	}
	body, _ = os.ReadFile(file)
	if string(body) != FileContent([]string{k1, k2}) {
		t.Errorf("after add:\n%s", body)
	}
}

func TestNoVerbIsAUsageErrorThatMentionsSync(t *testing.T) {
	err := run(nil, filepath.Join(t.TempDir(), "k"), func() error { return nil })
	if !errors.Is(err, errUsage) || !strings.Contains(err.Error(), "| sync") {
		t.Fatalf("err = %v", err)
	}
	if err := run([]string{"sync", "x"}, filepath.Join(t.TempDir(), "k"), func() error { return nil }); err == nil {
		t.Error("sync with arguments was accepted")
	}
}

func TestSyncMapsAnOldHelperToErrOldHelper(t *testing.T) {
	ge, ex := geteuid, execute
	t.Cleanup(func() { geteuid, execute = ge, ex })
	geteuid = func() int { return 0 }
	execute = func(a []string) ([]byte, error) {
		return []byte("pipedpeer-nix-trust: usage: pipedpeer-nix-trust add|remove <key> [key ...]\n"), errors.New("exit status 2")
	}
	if err := Sync(); !errors.Is(err, ErrOldHelper) {
		t.Errorf("old helper: %v", err)
	}
	execute = func(a []string) ([]byte, error) {
		return []byte("usage: pipedpeer-nix-trust add|remove <key> [key ...] | sync"), errors.New("exit status 2")
	}
	if err := Sync(); err == nil || errors.Is(err, ErrOldHelper) {
		t.Errorf("new helper failing: %v", err)
	}
	var ran []string
	execute = func(a []string) ([]byte, error) { ran = a; return nil, nil }
	if err := Sync(); err != nil || strings.Join(ran, " ") != HelperPath+" sync" {
		t.Errorf("ok sync: %v %v", err, ran)
	}
}

func TestSubstituterListedReadsTheKeysFile(t *testing.T) {
	kf := keysFile
	t.Cleanup(func() { keysFile = kf })
	keysFile = filepath.Join(t.TempDir(), "keys.conf")
	if SubstituterListed() {
		t.Error("missing file reported listed")
	}
	os.WriteFile(keysFile, []byte(keysSetting+" = "+newKey(t)+"\n"), 0o644)
	if SubstituterListed() {
		t.Error("old file reported listed")
	}
	os.WriteFile(keysFile, []byte(FileContent(nil)), 0o644)
	if !SubstituterListed() {
		t.Error("current file not listed")
	}
}
