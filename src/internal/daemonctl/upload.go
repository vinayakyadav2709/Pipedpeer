package daemonctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/runrec"
	"github.com/pipedpeer/pipedpeer/internal/say"

	"github.com/dustin/go-humanize"
)

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
		say.Println("      Upload stopped: cancelled")
		return nil, fmt.Errorf("upload cancelled: %w", ctx.Err())
	}
}

func uploadJob(ctx context.Context, host string, port int, workspacePath, narPath, storePath, scriptPath string, o UploadOptions) (*UploadResponse, error) {
	// Export the NAR only when the target daemon lacks the closure: an
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
	started := time.Now()
	switch {
	case narPath == "":
		// Nothing to ship: the caller relies on a store the node shares.
	case !o.Cold && storeCached(host, port, storePath):
		say.Println("      Closure already on the node; nothing to send")
	default:
		sendNAR = true
		// sent > 0, not merely ok: the node reporting no missing paths while
		// its cache check said the closure was not runnable means it holds a
		// store path that is there but incomplete. Sending the whole archive
		// repairs that; sending nothing would leave the job to fail on a
		// missing bin/run.
		seedFn := seedClosure
		if o.Cold {
			say.Println("      Cold run: sending the whole closure even if the node has it")
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
			say.Printf("      Sent %d of %d store paths: %s in %s\n",
				sr.sent, sr.total, humanize.Bytes(uint64(sr.bytes)), secs(sr.sendTime))
			sendNAR = false
			result.Closure, result.SentPaths, result.TotalPaths = "partial", sr.sent, sr.total
			if sr.sent == sr.total {
				result.Closure = "full"
			}
			result.SentBytes, result.SendTime = sr.bytes, sr.sendTime
		} else {
			say.Println("      Sending the whole closure (the node shares none of it)")
		}
	}
	var narSize int64
	if sendNAR {
		t0 := time.Now()
		if err := ExportNAR(ctx, storePath, narPath); err != nil {
			return nil, fmt.Errorf("nix store export failed: %v", err)
		}
		narSize = fileSize(narPath)
		say.Printf("      Packed closure: %s in %s\n", humanize.Bytes(uint64(narSize)), secs(time.Since(t0)))
	}

	wsSend, fromCAS, wsCleanup := workspaceDelta(host, port, workspacePath)
	defer wsCleanup()

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
			if len(fromCAS) > 0 {
				b, err := json.Marshal(fromCAS)
				if err != nil {
					return err
				}
				mp.WriteField("workspace_cas", string(b))
			}
			if err := addFile(mp, "workspace", "workspace.tar", wsSend, nil); err != nil {
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
		say.Printf("      Sent closure: %s in %s (%s)\n",
			humanize.Bytes(uint64(narSize)), secs(narTime), rate(narSize, narTime))
	}
	// Peers that refused the environment: without this the run just stays
	// local and the reason sits in a daemon log on another machine.
	for _, w := range result.Warnings {
		fmt.Fprintf(os.Stderr, "      Warning: %s\n", w)
	}
	recordEnvSend(host, port, result.NodeID, started, result.SentBytes+fileSize(wsSend))
	return &result, nil
}

// recordEnvSend adds the run record's env_send step: closure and workspace
// from this machine to the node at host:port.
func recordEnvSend(host string, port int, nodeID string, started time.Time, bytes int64) {
	if nodeID == "" {
		say.Printf("      %s:%d did not name its node id (older build); env_send has no receiver\n", host, port)
	}
	machine := ""
	if me, err := identity.GetOrCreate(); err == nil {
		machine = me.NodeID
	} else {
		fmt.Fprintf(os.Stderr, "      warning: env_send step has no machine id: %v\n", err)
	}
	runrec.Add(runrec.Step{Machine: machine, Node: nodeID, Chunk: -1, Name: "env_send",
		StartNS: started.UnixNano(), DurNS: time.Since(started).Nanoseconds(), Bytes: bytes})
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
