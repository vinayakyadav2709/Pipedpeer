// Package narpack moves a closure between peers in a form that keeps its
// signatures.
//
// # Why this exists
//
// Every transfer here used to be `nix-store --export`, and that format does
// not serialise signatures: they live in the store's own metadata, and the
// classic export stream carries only the archive and the reference list. So a
// sender could sign a path correctly, the receiver could trust the signing
// key, and the import would still be refused - measured on two machines with
// all 26 paths of a closure signed by a trusted key and the import failing
// with "lacks a signature by a trusted key". The only way to receive work was
// `require-sigs = false`, which switches signature checking off for
// EVERYTHING on that machine rather than for pipedpeer.
//
// A binary cache does carry them. `nix copy --to file://...` writes a
// `.narinfo` per path and the signature is a field in it. Copying out of such
// a cache is an ordinary substitution, so nix checks the signature against
// the receiver's own trusted keys and refuses what it does not recognise -
// which is the behaviour that was wanted from the start.
//
// # One thing that is exempt, and misleads a test
//
// A CONTENT-addressed path - one whose name is a hash of its contents, which
// is what `nix store add-path` and every fixed-output derivation produce -
// never needs a signature. Nix can verify it by hashing it, so a signature
// would add nothing, and such a path imports cleanly into a store that trusts
// nobody at all. Real closures here are input-addressed and do need one. This
// is written down because the obvious way to write a fixture for these tests
// is `nix store add-path`, and doing that produces a negative test that
// passes without the check existing.
//
// # What travels
//
// Not the whole cache. The peer has already said which paths it lacks, so the
// archive carries the .narinfo for every path in the closure - a few hundred
// bytes each, and they are what make the receiver self-contained - and the
// NAR only for the paths it asked for. That keeps the property the old path
// had: a peer already holding numpy is sent nothing for numpy.
package narpack

import (
	"archive/tar"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
)

// Format is the version of the archive layout, carried in the manifest so a
// receiver refuses one it does not understand rather than misreading it.
const Format = 1

// ManifestName is the archive entry describing what the archive is for.
const ManifestName = "pipedpeer-closure.json"

// Manifest is the archive's own description of itself.
type Manifest struct {
	Format int      `json:"format"`
	Roots  []string `json:"roots"`
}

// Narinfo is the part of a .narinfo file this package needs.
type Narinfo struct {
	// StorePath is the path the entry describes.
	StorePath string
	// URL is the archive's location inside the cache, relative to its root.
	// Read rather than derived: the suffix depends on the compression the
	// cache was written with, and guessing it produces an archive whose
	// narinfos point at files that are not in it.
	URL string
	// Signed reports whether any signature is present. A path with none can
	// still be legitimate - nix does not require one on a path the receiver
	// built itself - so this is for reporting, not for refusing.
	Signed bool
	// Name is the file's own name in the cache, e.g. "abc123....narinfo".
	Name string
}

// ParseNarinfo reads the fields this package needs from a narinfo file.
func ParseNarinfo(name string, r io.Reader) (Narinfo, error) {
	out := Narinfo{Name: name}
	sc := bufio.NewScanner(r)
	// References on a large closure is one very long line, and the scanner's
	// default 64KiB limit would end the read in the middle of the file - so
	// StorePath would be found and URL, further down, would not.
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		field, value, ok := strings.Cut(sc.Text(), ": ")
		if !ok {
			continue
		}
		switch field {
		case "StorePath":
			out.StorePath = strings.TrimSpace(value)
		case "URL":
			out.URL = strings.TrimSpace(value)
		case "Sig":
			out.Signed = true
		}
	}
	if err := sc.Err(); err != nil {
		return out, err
	}
	if out.StorePath == "" {
		return out, fmt.Errorf("%s has no StorePath", name)
	}
	if out.URL == "" {
		return out, fmt.Errorf("%s has no URL", name)
	}
	return out, nil
}

// cacheURI is the store URI for a directory used as a binary cache.
//
// zstd because the archives are what the transfer is made of. The old path
// gzipped the whole export stream, so leaving these uncompressed would be a
// regression measured in gigabytes on a torch closure.
func cacheURI(dir string) string {
	return "file://" + dir + "?compression=zstd"
}

// Publish writes these store paths, and everything they depend on, into a
// directory used as a binary cache.
//
// Incremental: nix skips a path already present, so one directory can back
// every transfer this daemon makes and each pays only for what is new.
func Publish(ctx context.Context, cacheDir string, roots []string) error {
	if len(roots) == 0 {
		return fmt.Errorf("nothing to publish")
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return err
	}
	argv := append([]string{"nix", "copy", "--to", cacheURI(cacheDir)}, roots...)
	cmd, done, err := nixstore.Cmd("", argv...)
	if err != nil {
		return err
	}
	defer done()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nix copy to the local cache: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Index reads every narinfo in a cache directory, keyed by store path.
func Index(cacheDir string) (map[string]Narinfo, error) {
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Narinfo, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".narinfo") {
			continue
		}
		f, err := os.Open(filepath.Join(cacheDir, e.Name()))
		if err != nil {
			return nil, err
		}
		ni, err := ParseNarinfo(e.Name(), f)
		f.Close()
		if err != nil {
			return nil, err
		}
		out[ni.StorePath] = ni
	}
	return out, nil
}

// Pack writes an archive carrying the whole closure's metadata and the
// archives for only the paths the receiver asked for.
//
// want is what the peer said it lacks. nil means it lacks everything, which
// is what a peer that could not answer the question has to be assumed to
// mean: sending too much is slow, sending too little is a broken import.
func Pack(cacheDir string, roots, closure, want []string, w io.Writer) error {
	if len(roots) == 0 {
		return fmt.Errorf("no roots to pack")
	}
	index, err := Index(cacheDir)
	if err != nil {
		return err
	}
	send := map[string]bool{}
	if want == nil {
		for _, p := range closure {
			send[p] = true
		}
	} else {
		for _, p := range want {
			send[p] = true
		}
	}

	tw := tar.NewWriter(w)
	body, err := json.Marshal(Manifest{Format: Format, Roots: roots})
	if err != nil {
		return err
	}
	if err := writeEntry(tw, ManifestName, body); err != nil {
		return err
	}

	// Sorted, so the same closure produces the same archive twice: an
	// unstable order makes two transfers of identical content look different
	// to anything that hashes or compares them.
	sorted := append([]string(nil), closure...)
	sort.Strings(sorted)

	for _, p := range sorted {
		ni, ok := index[p]
		if !ok {
			return fmt.Errorf("no narinfo for %s in %s: it was never published", p, cacheDir)
		}
		// Every narinfo travels, not only the ones whose archive does. They
		// cost a few hundred bytes each and they are what lets the receiver
		// resolve the closure against its own store without asking anything
		// further.
		raw, err := os.ReadFile(filepath.Join(cacheDir, ni.Name))
		if err != nil {
			return err
		}
		if err := writeEntry(tw, ni.Name, raw); err != nil {
			return err
		}
		if !send[p] {
			continue
		}
		nar, err := os.ReadFile(filepath.Join(cacheDir, filepath.FromSlash(ni.URL)))
		if err != nil {
			return fmt.Errorf("reading the archive for %s: %w", p, err)
		}
		if err := writeEntry(tw, ni.URL, nar); err != nil {
			return err
		}
	}
	return tw.Close()
}

func writeEntry(tw *tar.Writer, name string, body []byte) error {
	if err := tw.WriteHeader(&tar.Header{
		Name: name, Mode: 0o644, Size: int64(len(body)),
	}); err != nil {
		return err
	}
	_, err := tw.Write(body)
	return err
}

// Unpack writes an archive's contents into a directory usable as a binary
// cache, and returns what the sender said the roots are.
func Unpack(r io.Reader, dir string) (Manifest, error) {
	var m Manifest
	seenManifest := false
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return m, err
		}
		if hdr.Typeflag == tar.TypeDir {
			continue
		}
		name, err := safeName(hdr.Name)
		if err != nil {
			return m, err
		}
		if name == ManifestName {
			body, err := io.ReadAll(tr)
			if err != nil {
				return m, err
			}
			if err := json.Unmarshal(body, &m); err != nil {
				return m, fmt.Errorf("closure manifest: %w", err)
			}
			seenManifest = true
			continue
		}
		dst := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return m, err
		}
		f, err := os.Create(dst)
		if err != nil {
			return m, err
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return m, err
		}
		if err := f.Close(); err != nil {
			return m, err
		}
	}
	if !seenManifest {
		return m, fmt.Errorf("closure archive has no %s", ManifestName)
	}
	if m.Format != Format {
		return m, fmt.Errorf("closure archive is format %d, this node speaks %d", m.Format, Format)
	}
	if len(m.Roots) == 0 {
		return m, fmt.Errorf("closure archive names no roots")
	}
	return m, nil
}

// safeName rejects an archive entry that would write outside the directory.
//
// The archive came from another machine. A name like "../../../etc/nix/nix.conf"
// is not a transfer, it is an overwrite, and it has to be refused before
// anything is created rather than noticed afterwards.
func safeName(name string) (string, error) {
	clean := path.Clean(strings.ReplaceAll(name, `\`, "/"))
	if clean == "" || clean == "." {
		return "", fmt.Errorf("archive entry has an empty name")
	}
	if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("archive entry %q would write outside the directory", name)
	}
	return clean, nil
}

// importSeam is extra nix arguments, empty in production.
//
// It exists so a test can import into a scratch store with its own
// require-sigs and trusted-public-keys settings. Asserting that an unsigned
// closure is refused is the whole point of this package, and the only other
// way to assert it would be to change the machine's own nix configuration -
// which is both destructive and exactly the thing that broke a working node
// the last time signatures were tried.
var importSeam []string

// Import copies the roots out of an unpacked cache into this node's store.
//
// This is where the signature is checked. It is an ordinary substitution, so
// nix applies the receiving store's own rules: with require-sigs on, a path
// no trusted key signed is refused and the error says so. There is
// deliberately no --no-check-sigs here. A machine that wants to accept
// unsigned paths says so in its own configuration, which is the only place
// its owner can see the decision.
func Import(ctx context.Context, cacheDir string, roots []string) error {
	if len(roots) == 0 {
		return fmt.Errorf("nothing to import")
	}
	argv := append([]string{"nix", "copy", "--from", cacheURI(cacheDir)}, importSeam...)
	argv = append(argv, roots...)
	cmd, done, err := nixstore.Cmd("", argv...)
	if err != nil {
		return err
	}
	defer done()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nix copy from a peer's closure: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}
