package daemonctl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// seedClosure gives a node the store paths it lacks, and reports how many of
// the closure's paths had to cross.
//
// The whole-closure archive is keyed on the top-level store path, so two
// environments differing by one package share none of it: adding pandas to a
// numpy script re-sent python, numpy and the entire interpreter tree. The
// broadcast path used for pool spill has asked peers what they lack since it
// was written; the ordinary `run --remote` path did not, so the common case -
// one person, one machine, a script that grows a dependency - was the one
// paying full price. Measured on a 43-path closure whose peer already held
// 36 of them.
//
// Store paths are content-addressed, which is what makes this safe: a path
// the node already has under that name is that path, and there is no version
// of it that could be stale.
//
// The archive is the signed format (narpack), never `nix-store --export`:
// the export stream drops every signature, so a multi-user nix daemon - every
// real Fedora or Ubuntu receiver - refused it with "lacks a signature by a
// trusted key" even when the sender's key was trusted. A node that shares
// nothing gets the whole closure the same way.
//
// Returns ok=false when the signed send is not possible (an older node, a
// local packing failure, a transport error), and the caller sends the archive
// whole the old way. A node that understood the archive and refused it sets
// r.err instead: resending unsigned cannot satisfy a node that checks
// signatures, and would cost the whole closure again.
func seedClosure(ctx context.Context, host string, port int, storePath string) (seedResult, bool) {
	return seed(ctx, host, port, storePath, false)
}

// seedWhole is seedClosure for a cold run: every path travels, whatever the
// node already has.
func seedWhole(ctx context.Context, host string, port int, storePath string) (seedResult, bool) {
	return seed(ctx, host, port, storePath, true)
}

func seed(ctx context.Context, host string, port int, storePath string, whole bool) (seedResult, bool) {
	paths, err := closurePathsLocal(ctx, storePath)
	if ctx.Err() != nil {
		// Cancelled (Ctrl+C): stop here, not fall back to the whole archive.
		return seedResult{err: ctx.Err()}, false
	}
	if err != nil || len(paths) == 0 {
		fmt.Fprintf(os.Stderr, "      warning: cannot list the closure of %s (%v); sending it whole\n", storePath, err)
		return seedResult{}, false
	}
	r := seedResult{total: len(paths)}
	var missing []string // nil: every path
	if !whole {
		m, ok := askMissing(host, port, paths)
		if !ok {
			// An older daemon does not know the question. Sending the whole
			// closure is what it expects.
			return r, false
		}
		if len(m) == 0 {
			// Every path is already there. Nothing to send, and nothing to do:
			// the caller's own cache check decides whether to upload a NAR.
			return r, true
		}
		if len(m) < len(paths) {
			missing = m
		}
	}

	// On disk, not in $TMPDIR: a partial closure can be gigabytes, and /tmp
	// is often a tmpfs charged against the RAM the job is about to need.
	dir, err := userdir.Scratch("closurediff-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "      warning: no temp file for the closure (%v); sending it whole\n", err)
		return r, false
	}
	defer os.RemoveAll(dir)
	archive := filepath.Join(dir, "closure.tar")

	t0 := time.Now()
	if err := packSigned(ctx, storePath, paths, missing, archive); err != nil {
		if ctx.Err() != nil {
			r.err = ctx.Err()
			return r, false
		}
		fmt.Fprintf(os.Stderr, "      warning: packing the closure with its signatures failed (%v); sending it whole unsigned\n", err)
		return r, false
	}
	r.bytes = fileSize(archive)
	fmt.Printf("      Packed closure: %s in %s\n", humanize.Bytes(uint64(r.bytes)), secs(time.Since(t0)))
	t0 = time.Now()
	if err := postPartial(host, port, storePath, archive); err != nil {
		var refused *ClosureRefusedError
		if errors.As(err, &refused) {
			r.err = err
			return r, false
		}
		fmt.Fprintf(os.Stderr, "      warning: sending the closure failed (%v); sending it whole\n", err)
		return r, false
	}
	r.sent, r.sendTime = len(missing), time.Since(t0)
	if missing == nil {
		r.sent = len(paths)
	}
	return r, true
}

// seedResult is what seedClosure sent. err is a refusal no resend can fix.
type seedResult struct {
	sent, total int
	bytes       int64
	sendTime    time.Duration
	err         error
}

// ClosureRefusedError is a node that understood the signed closure and
// refused it - most often a key it does not trust. Sending it again, signed
// or not, or placing the job on that node again, repeats the refusal.
type ClosureRefusedError struct{ Reason string }

func (e *ClosureRefusedError) Error() string {
	return "the node refused the closure: " + e.Reason
}

// refusalMarkers are what a node that unpacked a signed closure says when it
// turns it down (daemonapi importSignedClosure, narpack.Import). Anything
// else - an older node that cannot read the archive - is not a refusal.
var refusalMarkers = []string{
	"lacks a signature by a trusted key",
	"nix copy from a peer's closure",
	"refusing a closure whose signer key",
	"closure archive is format",
}

func isRefusal(msg string) bool {
	for _, m := range refusalMarkers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

// ClosureStatus reports how much of a closure a node already holds, for a
// dry run: "on-node" (runnable there), "partial" (missing paths of total),
// "none" or "unknown" (no local nix, or the node cannot say).
func ClosureStatus(host string, port int, storePath string) (state string, missing, total int) {
	cached := storeCached(host, port, storePath)
	paths, err := closurePathsLocal(context.Background(), storePath)
	if err != nil {
		if cached {
			return "on-node", 0, 0
		}
		fmt.Fprintf(os.Stderr, "      warning: cannot list the closure of %s: %v\n", storePath, err)
		return "unknown", 0, 0
	}
	if cached {
		return "on-node", 0, len(paths)
	}
	miss, ok := askMissing(host, port, paths)
	if !ok {
		fmt.Fprintf(os.Stderr, "      warning: %s:%d did not say which store paths it lacks\n", host, port)
		return "unknown", 0, len(paths)
	}
	if len(miss) == len(paths) {
		return "none", len(miss), len(paths)
	}
	// Includes 0 missing on a node whose copy is not runnable: a run would
	// still send the archive.
	return "partial", len(miss), len(paths)
}

// closurePathsLocal lists every store path the closure needs.
func closurePathsLocal(ctx context.Context, storePath string) ([]string, error) {
	cmd, cleanup, err := nixstore.CmdContext(ctx, "", "nix-store", "-qR", storePath)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nix-store -qR %s: %w: %s", storePath, err,
			strings.TrimSpace(stderr.String()))
	}
	return strings.Fields(string(out)), nil
}

// packSigned writes a signed closure archive (narpack) of storePath carrying
// the NARs of want only (nil: all of paths). The paths are signed with this
// node's key and published into the same cache directory the daemon sends
// from (daemonapi closureCacheDir), so a closure both have sent is
// compressed once.
func packSigned(ctx context.Context, storePath string, paths, want []string, dest string) error {
	// Signed before it is published: the signature is written into the
	// cache's metadata at publish time.
	if err := nixsign.EnsureSigned(ctx, paths); err != nil {
		fmt.Fprintf(os.Stderr, "      warning: could not sign the closure (%v); the node may refuse it\n", err)
	}
	cacheDir := filepath.Join(userdir.Data(), "closure-cache")
	if err := narpack.Publish(ctx, cacheDir, []string{storePath}); err != nil {
		return err
	}
	// Named so a node that has not polled this one can still check the
	// signature.
	var m narpack.Manifest
	if self, err := identity.Key(); err == nil {
		m.Signer, m.SignerKey = self.Fingerprint(), nixsign.PublicKey(self)
	} else {
		fmt.Fprintf(os.Stderr, "      warning: no node key to name in the closure (%v); a node that has not polled this one will refuse it\n", err)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	if err := narpack.PackSigned(m, cacheDir, []string{storePath}, paths, want, f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	narpack.Touch(cacheDir, paths)
	if _, err := narpack.Prune(cacheDir, narpack.MaxBytes()); err != nil {
		fmt.Fprintf(os.Stderr, "      warning: could not bound the closure cache %s: %v\n", cacheDir, err)
	}
	return nil
}

// askMissing asks a node which of these paths it does not have. A node that
// cannot answer yields ok=false rather than an empty list, so "no answer" is
// never mistaken for "has everything".
func askMissing(host string, port int, paths []string) ([]string, bool) {
	body, err := json.Marshal(map[string]any{"paths": paths})
	if err != nil {
		return nil, false
	}
	url := fmt.Sprintf("http://%s:%d/v1/store/missing", host, port)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var out struct {
		Missing []string `json:"missing"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return nil, false
	}
	return out.Missing, true
}

// jsonError is the "error" field of a daemon's JSON answer, or the body.
func jsonError(body []byte) string {
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return e.Error
	}
	return strings.TrimSpace(string(body))
}

// postPartial hands the signed archive to the node's store.
//
// partial=1 matters: the receiving side must not file a fragment away as the
// cache entry for this closure, or it would later forward it to a third node
// as though it were the whole thing.
func postPartial(host string, port int, storePath, narPath string) error {
	f, err := os.Open(narPath)
	if err != nil {
		return err
	}
	defer f.Close()

	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)
	go func() {
		mp.WriteField("store_path", storePath)
		mp.WriteField("partial", "1")
		part, err := mp.CreateFormFile("nar", "closure.tar")
		if err == nil {
			r, done := withProgress(f, fileSize(narPath), "      Sending missing paths")
			_, err = io.Copy(part, r)
			done()
		}
		if err == nil {
			err = mp.Close()
		}
		pw.CloseWithError(err)
	}()

	url := fmt.Sprintf("http://%s:%d/v1/store/import", host, port)
	resp, err := http.Post(url, mp.FormDataContentType(), pr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		if isRefusal(string(msg)) {
			return &ClosureRefusedError{Reason: jsonError(msg)}
		}
		return fmt.Errorf("closure import rejected: %s", strings.TrimSpace(string(msg)))
	}
	return nil
}
