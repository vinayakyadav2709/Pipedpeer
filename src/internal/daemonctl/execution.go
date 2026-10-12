package daemonctl

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/tarcodec"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gorilla/websocket"
)

// ExecConfig is sent to the daemon WebSocket to start execution.
type ExecConfig struct {
	ScriptPath string   `json:"script_path"`
	Args       []string `json:"args"`
	Envs       []string `json:"envs"`
	Isolate    bool     `json:"isolate"`
	StorePath  string   `json:"store_path"`
	GPU        bool     `json:"gpu,omitempty"`
	GPUDevices string   `json:"gpu_devices,omitempty"`
	// Intercept enables the sitecustomize shim on the node.
	Intercept bool `json:"intercept,omitempty"`
	// MemLimitBytes caps the sandbox's memory via cgroups. Everything else
	// about memory here is an estimate checked at admission; this is the only
	// part the kernel enforces, and without it a job that outgrows its
	// estimate takes the whole machine down rather than itself.
	MemLimitBytes int64 `json:"mem_limit_bytes,omitempty"`
	// LeaseID binds the node's lease to this job, so the lease lives exactly
	// as long as the job does, even when nobody is attached to renew it.
	LeaseID string `json:"lease_id,omitempty"`
}

type outputMessage struct {
	// Cancelled marks the done frame of a job stopped by a cancel frame.
	Cancelled     bool   `json:"cancelled,omitempty"`
	O             string `json:"o,omitempty"`
	E             string `json:"e,omitempty"`
	Error         string `json:"error,omitempty"`
	Done          bool   `json:"done,omitempty"`
	ExitCode      int    `json:"exit_code"`
	OOMKilled     bool   `json:"oom_killed,omitempty"`
	MemLimitBytes int64  `json:"mem_limit_bytes,omitempty"`
	// PeakMemBytes mirrors the daemon's field so the client can learn the
	// job's real footprint and record it for the historical estimation tier.
	PeakMemBytes int64 `json:"peak_mem_bytes,omitempty"`
	// Timing is the node's own split of the execution; absent from a daemon
	// too old to send it.
	Timing *ExecTiming `json:"timing,omitempty"`
	// Receipt is the shim's account of the run, when it wrote one.
	Receipt json.RawMessage `json:"receipt,omitempty"`
}

// ExecTiming mirrors the daemon's account of one execution, in milliseconds.
type ExecTiming struct {
	ImportMs int64 `json:"import_ms"`
	SetupMs  int64 `json:"setup_ms"`
	RunMs    int64 `json:"run_ms"`
}

// ExecResult is what StreamExecute learned about a run.
type ExecResult struct {
	// PeakBytes is the peak memory the job used, as the daemon measured it.
	PeakBytes int64
	Stdout    string
	Stderr    string
	// Timing is nil when the daemon did not report one.
	Timing *ExecTiming
	// Receipt is the shim's raw JSON receipt, nil when there was none.
	Receipt json.RawMessage
}

// UploadOptions tunes one upload.
type UploadOptions struct {
	// SkipBroadcast tells the daemon not to forward the closure to its peers.
	SkipBroadcast bool
	// Cold sends the whole closure even when the node already has it, to time
	// a first run. It does not undo the local build or the node's import.
	Cold bool
	// LeaseID names the lease this job runs under, so a node whose upload
	// breaks (the submitter was killed mid-transfer) frees the slot at once
	// instead of after the running-lease TTL. Older daemons ignore the field.
	LeaseID string
}

// UploadResponse is the daemon's answer plus what this side measured about
// the closure transfer, for the run summary.
type UploadResponse struct {
	JobID     string   `json:"job_id"`
	StorePath string   `json:"store_path"`
	Warnings  []string `json:"warnings,omitempty"`
	// SeedMs is the part of the upload the daemon spent giving the closure to
	// its peers.
	SeedMs int64 `json:"seed_ms,omitempty"`

	// Closure is "on-node" (nothing sent), "partial" (only missing paths)
	// or "full" (the whole archive).
	Closure    string        `json:"-"`
	SentPaths  int           `json:"-"`
	TotalPaths int           `json:"-"`
	SentBytes  int64         `json:"-"`
	SendTime   time.Duration `json:"-"`
}

// UploadJob sends workspace tarball + NAR closure to the daemon.
// If the daemon already has the store path cached (a prior task in the fan-out
// shipped the same closure), the NAR is omitted and only the workspace travels.
//
// A cancelled ctx returns at once with an error wrapping context.Canceled.
// Exporting and sending a multi-GB closure takes minutes, and Ctrl+C used to
// wait for all of it (seen: 45 s to 6 min) while the node held the lease.
func UploadJob(ctx context.Context, host string, port int, workspacePath, narPath, storePath, scriptPath string, o UploadOptions) (*UploadResponse, error) {
	type outcome struct {
		resp *UploadResponse
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		r, err := uploadJob(ctx, host, port, workspacePath, narPath, storePath, scriptPath, o)
		done <- outcome{r, err}
	}()
	select {
	case out := <-done:
		return out.resp, out.err
	case <-ctx.Done():
		// ponytail: the closure diff and export helpers take no ctx, so a
		// cancel mid-export leaves that goroutine to finish on its own. The
		// CLI exits right after a cancel, and their nix-store children write
		// into a pipe of this process, so they die with it. Thread ctx into
		// seedClosure/ExportNAR if a long-lived caller ever cancels uploads.
		fmt.Println("      Upload stopped: cancelled")
		return nil, fmt.Errorf("upload cancelled: %w", ctx.Err())
	}
}

func uploadJob(ctx context.Context, host string, port int, workspacePath, narPath, storePath, scriptPath string, o UploadOptions) (*UploadResponse, error) {
	// Export the NAR only when the target daemon lacks the closure: a gzip
	// export of a multi-GB store is seconds-to-minutes of wasted work when a
	// shared store (or a prior fan-out task) already gave every node a copy.
	//
	// When it does lack it, try to send only the store paths it does not
	// already have. A closure that differs from one the node holds by a
	// single package shares almost all of its paths with it, and without this
	// every one of them crosses again.
	//
	// Each of the three outcomes says so. Without that there is no way to
	// tell a run that shipped a whole environment from one that shipped
	// nothing: both print "Uploading to daemon" and differ only in how long
	// they take.
	result := UploadResponse{Closure: "on-node"}
	var sendNAR bool
	switch {
	case narPath == "":
		// Nothing to ship: the caller relies on a store the node shares.
	case !o.Cold && storeCached(host, port, storePath):
		fmt.Println("      Closure already on the node; nothing to send")
	default:
		sendNAR = true
		// sent > 0, not merely ok: the node reporting no missing paths while
		// its cache check said the closure was not runnable means it holds a
		// store path that is there but incomplete. Sending the whole archive
		// repairs that; sending nothing would leave the job to fail on a
		// missing bin/run.
		seedFn := seedClosure
		if o.Cold {
			fmt.Println("      Cold run: sending the whole closure even if the node has it")
			seedFn = seedWhole
		}
		sr, ok := seedFn(ctx, host, port, storePath)
		if sr.err != nil {
			// The node read the signed closure and refused it: the unsigned
			// whole archive would be refused too (FX-SIGN).
			return nil, sr.err
		}
		if ok && sr.sent > 0 {
			// The node now has every path, so the job needs no archive.
			fmt.Printf("      Sent %d of %d store paths: %s in %s\n",
				sr.sent, sr.total, humanize.Bytes(uint64(sr.bytes)), secs(sr.sendTime))
			sendNAR = false
			result.Closure, result.SentPaths, result.TotalPaths = "partial", sr.sent, sr.total
			if sr.sent == sr.total {
				result.Closure = "full"
			}
			result.SentBytes, result.SendTime = sr.bytes, sr.sendTime
		} else {
			fmt.Println("      Sending the whole closure (the node shares none of it)")
		}
	}
	var narSize int64
	if sendNAR {
		t0 := time.Now()
		if err := ExportNAR(ctx, storePath, narPath); err != nil {
			return nil, fmt.Errorf("nix store export failed: %v", err)
		}
		narSize = fileSize(narPath)
		fmt.Printf("      Packed closure: %s in %s\n", humanize.Bytes(uint64(narSize)), secs(time.Since(t0)))
	}

	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)
	var narTime time.Duration
	written := make(chan struct{})

	go func() {
		defer close(written)
		err := func() error {
			// First, so the node knows which lease to free if what follows
			// never arrives.
			if o.LeaseID != "" {
				mp.WriteField("lease_id", o.LeaseID)
			}
			if err := addFile(mp, "workspace", "workspace.tar", workspacePath, nil); err != nil {
				return err
			}
			mp.WriteField("store_path", storePath)
			mp.WriteField("script_path", scriptPath)
			if o.SkipBroadcast {
				mp.WriteField("skip_broadcast", "1")
			}
			if sendNAR {
				t0 := time.Now()
				wrap := func(r io.Reader) (io.Reader, func()) {
					return withProgress(r, narSize, "      Sending closure")
				}
				if err := addFile(mp, "nar", "closure.nar", narPath, wrap); err != nil {
					return err
				}
				narTime = time.Since(t0)
			}
			return mp.Close()
		}()
		pw.CloseWithError(err)
	}()

	url := fmt.Sprintf("http://%s:%d/v1/jobs/upload", host, port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, pr)
	if err != nil {
		pr.CloseWithError(err)
		<-written
		return nil, fmt.Errorf("upload failed: %w", err)
	}
	req.Header.Set("Content-Type", mp.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	// Unblock the writer if the daemon answered without reading everything,
	// then wait so narTime is settled.
	pr.Close()
	<-written
	if err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upload rejected: %s", string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("invalid upload response: %w", err)
	}
	if sendNAR {
		result.Closure, result.SentBytes, result.SendTime = "full", narSize, narTime
		fmt.Printf("      Sent closure: %s in %s (%s)\n",
			humanize.Bytes(uint64(narSize)), secs(narTime), rate(narSize, narTime))
	}
	// Peers that refused the environment: without this the run just stays
	// local and the reason sits in a daemon log on another machine.
	for _, w := range result.Warnings {
		fmt.Fprintf(os.Stderr, "      Warning: %s\n", w)
	}
	return &result, nil
}

// storeCached asks the daemon whether it already has a closure for storePath.
func storeCached(host string, port int, storePath string) bool {
	url := fmt.Sprintf("http://%s:%d/v1/store?path=%s&runnable=1", host, port, url.QueryEscape(storePath))
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "      warning: closure cache check on %s:%d failed (%v); sending it\n", host, port, err)
		return false
	}
	defer resp.Body.Close()
	var r struct {
		Cached bool `json:"cached"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		fmt.Fprintf(os.Stderr, "      warning: closure cache check on %s:%d unreadable (%v); sending it\n", host, port, err)
		return false
	}
	return r.Cached
}

// cancelWait is how long a cancelled run waits for the daemon's done frame.
const cancelWait = 5 * time.Second

// closeDetach is the websocket close code that means "stop watching, keep
// the job running" (C-BG).
const closeDetach = 4001

// StreamExecute connects to the daemon WebSocket and streams job output to stdout/stderr.
// Blocks until the job completes. The ExecResult holds whatever was read,
// error or not: peak memory, output, and the node's timing and receipt from
// the done frame.
//
// ctx cancelled: the daemon is told to stop the job ({"cancel":true}) and the
// returned error wraps context.Canceled. detach closed: the connection closes
// with code 4001 and no cancel, the job keeps running, and the error wraps
// ErrDetached. A nil detach never fires. A script that exits non-zero returns
// *RemoteExitError.
func StreamExecute(ctx context.Context, host string, port int, jobID string, cfg ExecConfig, detach <-chan struct{}) (ExecResult, error) {
	conn, err := dialJobSocket(ctx, host, port, "/v1/jobs/"+url.PathEscape(jobID)+"/exec")
	if err != nil {
		return ExecResult{}, err
	}
	defer conn.Close()

	// Written before streamJob starts its writer goroutine, so the two
	// writes never overlap (gorilla allows one writer at a time).
	if err := conn.WriteJSON(cfg); err != nil {
		return ExecResult{}, fmt.Errorf("send config: %w", err)
	}
	return streamJob(ctx, conn, detach)
}

// dialJobSocket opens an authenticated websocket to a daemon path.
func dialJobSocket(ctx context.Context, host string, port int, path string) (*websocket.Conn, error) {
	u := fmt.Sprintf("ws://%s:%d%s", host, port, path)

	// The websocket dialer does not go through http.DefaultTransport, so the
	// shared secret has to be attached by hand here; without it the upgrade
	// is refused and the job fails with a bare "bad handshake".
	var wsHeaders http.Header
	if tok := authtoken.Current(); tok != "" {
		// Same rule as the transport: the token goes only to a host that has
		// shown it already holds it.
		if err := authtoken.Prove(ctx, net.JoinHostPort(host, strconv.Itoa(port))); err != nil {
			return nil, err
		}
		wsHeaders = http.Header{authtoken.Header: []string{tok}}
	}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, u, wsHeaders)
	if err != nil {
		if resp != nil {
			// A refused upgrade carries the reason (unknown job, bad token)
			// in its body; "bad handshake" alone says nothing.
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return nil, fmt.Errorf("websocket dial %s: %w (HTTP %d: %s)", path, err, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return nil, fmt.Errorf("websocket dial: %w", err)
	}
	return conn, nil
}

type cancelFrame struct {
	Cancel bool `json:"cancel"`
}

// streamJob prints a job's output frames until the done frame, handling
// cancel (ctx) and detach. Shared by exec and attach.
func streamJob(ctx context.Context, conn *websocket.Conn, detach <-chan struct{}) (ExecResult, error) {
	var cancelled, detached atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-stop:
		case <-ctx.Done():
			cancelled.Store(true)
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := conn.WriteJSON(cancelFrame{Cancel: true}); err != nil {
				fmt.Fprintf(os.Stderr, "warning: could not tell the daemon to stop the job: %v\n", err)
			}
			// Keep reading for the done frame, but not forever.
			_ = conn.SetReadDeadline(time.Now().Add(cancelWait))
		case <-detach:
			detached.Store(true)
			msg := websocket.FormatCloseMessage(closeDetach, "detach")
			if err := conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(2*time.Second)); err != nil {
				fmt.Fprintf(os.Stderr, "warning: detach close frame not sent (%v); the job keeps running anyway\n", err)
			}
			_ = conn.SetReadDeadline(time.Now())
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	var res ExecResult
	stdoutBuf := new(strings.Builder)
	stderrBuf := new(strings.Builder)
	result := func() ExecResult {
		res.Stdout, res.Stderr = stdoutBuf.String(), stderrBuf.String()
		return res
	}
	stdoutW, stderrW := io.MultiWriter(os.Stdout, stdoutBuf), io.MultiWriter(os.Stderr, stderrBuf)
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			switch {
			case detached.Load():
				return result(), fmt.Errorf("job keeps running on the node: %w", ErrDetached)
			case cancelled.Load():
				return result(), fmt.Errorf(
					"cancelled; the daemon did not confirm the job stopped within %s (%v): %w", cancelWait, err, context.Canceled)
			case websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway):
				return result(), nil
			}
			return result(), fmt.Errorf("%w: %w", ErrConnectionLost, err)
		}

		var out outputMessage
		if err := json.Unmarshal(msg, &out); err != nil {
			fmt.Fprintf(os.Stderr, "warning: skipping an unreadable frame from the daemon: %v\n", err)
			continue
		}

		if done, err := applyFrame(out, &res, stdoutW, stderrW, cancelled.Load()); done {
			return result(), err
		}
	}
}

// DownloadResults fetches output files from the daemon and saves them to outDir.
func DownloadResults(host string, port int, jobID, outDir string) (*ResultManifest, error) {
	url := fmt.Sprintf("http://%s:%d/v1/jobs/%s/results", host, port, jobID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("download results: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("download rejected: %s", string(body))
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, err
	}

	return extractResults(resp.Body, outDir)
}

// ResultManifest records what a job sent back, so a run can report what it
// touched instead of silently overwriting files in the submitter's project.
type ResultManifest struct {
	JobID   string   `json:"job_id,omitempty"`
	OutDir  string   `json:"out_dir"`
	New     []string `json:"new,omitempty"`
	Updated []string `json:"updated,omitempty"`
	Deleted []string `json:"deleted,omitempty"`
}

// deletedManifestName is the tar entry that carries the deletion list. Must
// match the daemon's writer (internal/daemonapi/execution.go).
const deletedManifestName = ".pipedpeer-deleted.json"

// Count is the total number of files received.
func (m *ResultManifest) Count() int {
	if m == nil {
		return 0
	}
	return len(m.New) + len(m.Updated)
}

// extractResults unpacks a results tar into outDir, classifying each entry as
// new or updated relative to what is already on disk. Entries that would
// escape outDir are refused: the archive comes from another machine.
//
// A .pipedpeer-deleted.json entry lists files the job deleted on the node;
// they are removed locally after everything else is extracted. The list is
// computed daemon-side from exactly what this job uploaded, so only job
// artifacts are ever removed.
func extractResults(body io.Reader, outDir string) (*ResultManifest, error) {
	manifest := &ResultManifest{OutDir: outDir}
	root, err := filepath.Abs(outDir)
	if err != nil {
		return nil, err
	}

	// Sniff, so a daemon on an older build that answers gzip or plain still
	// works: a cluster is rarely upgraded all at once, and it is upgraded in
	// both directions. Go's http client already unwraps gzip when it
	// negotiated the encoding itself, but this response declares it outright,
	// so handle it here.
	rd, closeRd, err := tarcodec.Reader(body)
	if err != nil {
		return nil, err
	}
	defer closeRd()
	tr := tar.NewReader(rd)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return manifest, fmt.Errorf("read results: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		if hdr.Name == deletedManifestName {
			payload, rerr := io.ReadAll(tr)
			if rerr != nil {
				return manifest, fmt.Errorf("read deletion manifest: %w", rerr)
			}
			_ = json.Unmarshal(payload, &manifest.Deleted) // empty/garbled = no deletions
			continue
		}

		target := filepath.Join(root, filepath.FromSlash(hdr.Name))
		if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
			return manifest, fmt.Errorf("refusing result path outside %s: %s", outDir, hdr.Name)
		}

		_, statErr := os.Stat(target)
		existed := statErr == nil

		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return manifest, err
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
		if err != nil {
			return manifest, err
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return manifest, err
		}
		f.Close()

		if existed {
			manifest.Updated = append(manifest.Updated, hdr.Name)
		} else {
			manifest.New = append(manifest.New, hdr.Name)
		}
	}

	for _, rel := range manifest.Deleted {
		target := filepath.Join(root, filepath.FromSlash(rel))
		if target == root || !strings.HasPrefix(target, root+string(os.PathSeparator)) {
			return manifest, fmt.Errorf("refusing deletion outside %s: %s", outDir, rel)
		}
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return manifest, err
		}
	}
	return manifest, nil
}

// ExportNAR exports the full runtime closure of a store path to a NAR file.
// Uses the nix multicall binary under the nix-store name (argv[0] set).
func ExportNAR(ctx context.Context, storePath, destPath string) error {
	query, qcleanup, err := nixstore.CmdContext(ctx, "", "nix-store", "-qR", storePath)
	if err != nil {
		return err
	}
	defer qcleanup()
	var qerr bytes.Buffer
	query.Stderr = &qerr
	closureOut, err := query.Output()
	if err != nil {
		// Carry nix's own words: "exit status 1" alone sends you looking in
		// the wrong place.
		return fmt.Errorf("nix-store -qR %s: %w: %s", storePath, err, strings.TrimSpace(qerr.String()))
	}

	paths := strings.Fields(string(closureOut))
	if len(paths) == 0 {
		return fmt.Errorf("empty closure for %s", storePath)
	}

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	// Signed before it leaves, so the far side can require a signature
	// instead of being configured to stop asking for one.
	if err := nixsign.EnsureSigned(ctx, paths); err != nil {
		fmt.Fprintf(os.Stderr, "      warning: could not sign the closure (%v); the node may refuse it\n", err)
	}
	export, ecleanup, err := nixstore.CmdContext(ctx, "", append([]string{"nix-store", "--export"}, paths...)...)
	if err != nil {
		return err
	}
	defer ecleanup()
	export.Stdout = gz
	export.Stderr = os.Stderr
	return export.Run()
}

// ignoreExcludes turns one .pipedpeerignore line into tar exclude flags.
//
// The file is written gitignore-style, and tar does not read gitignore. A
// directory written "data/" never matched, because tar compares the pattern
// against member names, which carry no trailing slash - so every "dir/" line
// excluded nothing, `pipedpeer init`'s template included, and the demo's
// 1.1 GB CSV went into the upload of every script beside it. A leading slash
// anchors the pattern at the project root, as in gitignore; tar needs that
// spelled as the member name, "./data", and matched anchored.
func ignoreExcludes(line string) []string {
	if line == "" || strings.HasPrefix(line, "#") {
		return nil
	}
	line = strings.TrimRight(line, "/")
	if line == "" {
		return nil
	}
	if strings.HasPrefix(line, "/") {
		return []string{"--anchored", "--exclude=." + line, "--no-anchored"}
	}
	return []string{"--exclude=" + line}
}

// CreateWorkspaceTar creates a tarball of the project directory at destPath.
// It writes a sitecustomize.py into .pipedpeer/shim/ inside the archive, so a
// node that enables the interception shim can put that dir on PYTHONPATH and
// Python auto-imports it before the user's first line. shimContent empty means
// no shim is embedded.
func CreateWorkspaceTar(projectDir, destPath string, shimContent string) error {
	ignoreFile := filepath.Join(projectDir, ".pipedpeerignore")

	args := []string{"--exclude=.git", "--exclude=__pycache__", "--exclude=.venv",
		"--exclude=venv", "--exclude=env", "--exclude=node_modules"}

	if content, err := os.ReadFile(ignoreFile); err == nil {
		for _, line := range strings.Split(string(content), "\n") {
			args = append(args, ignoreExcludes(strings.TrimSpace(line))...)
		}
	}

	args = append(args, "-cf", destPath, "-C", projectDir, ".")
	if err := exec.Command("tar", args...).Run(); err != nil {
		return err
	}

	if shimContent != "" {
		if err := appendShim(destPath, shimContent); err != nil {
			return fmt.Errorf("append shim: %w", err)
		}
	}

	// Compressed last, not at creation: the shim is added with `tar --append`,
	// which cannot open a compressed archive. A workspace is mostly source
	// and crosses the wire on every run, so a second pass over a file already
	// on disk is worth it.
	return tarcodec.CompressFile(destPath, tarcodec.CompressFloor)
}

// appendShim adds .pipedpeer/shim/sitecustomize.py as a member of an existing
// tar archive. It stages the file in a temp dir and uses GNU tar's transform
// to re-home it, so the user's project directory is never touched.
func appendShim(destPath, content string) error {
	stageDir, err := userdir.Scratch("shim-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stageDir)

	stage := filepath.Join(stageDir, "sitecustomize.py")
	if err := os.WriteFile(stage, []byte(content), 0755); err != nil {
		return err
	}

	cmd := exec.Command("tar", "--append", "--file="+destPath,
		"--transform=s,.*,.pipedpeer/shim/sitecustomize.py,", "-C", stageDir, "sitecustomize.py")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// addFile copies a file into the form. wrap, if set, wraps the file reader
// (a progress bar) and returns a func to call once the copy ends.
func addFile(mp *multipart.Writer, fieldName, fileName, filePath string, wrap func(io.Reader) (io.Reader, func())) error {
	w, err := mp.CreateFormFile(fieldName, fileName)
	if err != nil {
		return fmt.Errorf("form field %s: %w", fieldName, err)
	}
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open %s: %w", fieldName, err)
	}
	defer f.Close()
	var r io.Reader = f
	if wrap != nil {
		var done func()
		r, done = wrap(f)
		defer done()
	}
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("send %s: %w", fieldName, err)
	}
	return nil
}

// humanBytes renders a byte count the way the --mem flag accepts it.
func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0fM", float64(n)/float64(1<<20))
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

// RemoteExitError is a script that ran to completion on its node and exited
// non-zero. Unlike a lost connection or a dead node, it says nothing about
// where the script ran.
type RemoteExitError struct {
	Code int
}

func (e *RemoteExitError) Error() string {
	return fmt.Sprintf("remote job exited with code %d", e.Code)
}
