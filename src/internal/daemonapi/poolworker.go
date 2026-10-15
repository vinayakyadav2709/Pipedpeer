package daemonapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"

	"github.com/pipedpeer/pipedpeer/internal/phone"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// spawn starts a warm worker for a store path. bin/run warm_worker.py reads
// JSON-lines from stdin and writes results to stdout.
func (pm *poolManager) spawn(runPath, storePath string) (*poolWorker, error) {
	dir, err := userdir.Scratch("warm-*")
	if err != nil {
		return nil, err
	}
	workerScript := filepath.Join(dir, "warm_worker.py")
	if err := os.WriteFile(workerScript, []byte(warmWorkerScript), 0755); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	cmd := termuxexec.Command(runPath, workerScript)
	// Own process group, so a cancel kills whatever the user's function forked.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	w := &poolWorker{
		cmd:    cmd,
		stdin:  bufio.NewWriter(stdin),
		stdout: bufio.NewScanner(stdout),
	}
	w.stdout.Buffer(make([]byte, 0, 64<<10), 16<<20)
	return w, nil
}

func (w *poolWorker) dead() bool {
	return w == nil || w.cmd == nil || w.killed.Load()
}

// kill stops the worker and its process group once; later calls do nothing.
func (w *poolWorker) kill() { w.killOnce() }

// killOnce is kill reporting whether this call stopped the worker, so exactly
// one caller reaps it.
func (w *poolWorker) killOnce() bool {
	if w.killed.Swap(true) || w.cmd == nil || w.cmd.Process == nil {
		return false
	}
	if err := killProcessGroup(w.cmd.Process); err != nil {
		log.Printf("[pool] stopping warm worker pid %d: %v", w.cmd.Process.Pid, err)
	}
	return true
}

// submit sends one task over the pipes and waits for its result file. The
// payload (pickled items can be ~100s of MB) is written to a temp file and the
// worker is told the path, so the full blob never passes through the pipe or
// lives in daemon RAM. Frames chunks ship as a header line + length-prefixed
// raw pickle frames; legacy chunks keep the base64 JSON encoding.
func (w *poolWorker) submit(ch *chunk) ([]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	ctx := ch.context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if w.killed.Load() {
		return nil, fmt.Errorf("warm worker was stopped")
	}
	w.id++

	// Holds the chunk payload: hundreds of megabytes is normal, and a
	// tmpfs would charge the job's own memory for it.
	dir, err := userdir.Scratch("warm-task-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	inPath := filepath.Join(dir, "in.bin")
	outPath := filepath.Join(dir, "out.bin")

	var body []byte
	if ch.frames {
		hdr, _ := json.Marshal(map[string]any{
			"id": w.id, "func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName,
			"starmap": ch.starmap, "items_frames": len(ch.items), "globals": ch.globals != nil,
			"required_mem": ch.requiredMem, "cache_keys": ch.cacheKeys, "item_keys": ch.itemKeys,
			"chunk_dir": ch.chunkDir, "items_zlib": ch.itemsZlib,
		})
		body = buildFrames(hdr, ch.globals, ch.items)
	} else {
		req := map[string]any{"id": w.id, "func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName, "extra_b64": ch.extraB64, "items": ch.items, "starmap": ch.starmap, "items_b64": ch.itemsB64, "chunk_dir": ch.chunkDir}
		if len(ch.cacheKeys) > 0 {
			req["cache_keys"] = ch.cacheKeys
		}
		if len(ch.itemKeys) > 0 {
			req["item_keys"] = ch.itemKeys
		}
		body, err = json.Marshal(req)
		if err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(inPath, body, 0644); err != nil {
		return nil, err
	}

	// A cancel while this chunk runs kills the worker; the read below then
	// fails and the caller sees the cancel. Reap it so it leaves no zombie.
	stop := context.AfterFunc(ctx, w.kill)
	defer func() {
		if !stop() {
			err := w.cmd.Wait()
			log.Printf("[pool] warm worker pid %d stopped for a cancelled request: %v", w.cmd.Process.Pid, err)
		}
	}()

	msg := map[string]any{"id": w.id, "in_file": inPath, "out_file": outPath}
	line, _ := json.Marshal(msg)
	if _, err := w.stdin.WriteString(string(line) + "\n"); err != nil {
		return nil, err
	}
	if err := w.stdin.Flush(); err != nil {
		return nil, err
	}

	// Read until the ack matching our id arrives. Scan() returns false on EOF
	// (worker exited), which surfaces as an error and lets the caller fall back
	// to a cold run.
	for {
		if !w.stdout.Scan() {
			return nil, fmt.Errorf("warm worker closed: %v", w.stdout.Err())
		}
		var ack struct {
			ID    int64  `json:"id"`
			Done  bool   `json:"done"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(w.stdout.Bytes(), &ack); err != nil {
			continue
		}
		if ack.ID != w.id {
			continue // stale line from a concurrent write; ignore
		}
		if ack.Error != "" {
			return nil, fmt.Errorf("worker: %s", ack.Error)
		}
		if !ack.Done {
			return nil, fmt.Errorf("worker: no done ack")
		}
		outBytes, err := os.ReadFile(outPath)
		if err != nil {
			return nil, err
		}
		if ch.frames {
			nl := bytes.IndexByte(outBytes, '\n')
			if nl < 0 {
				return nil, fmt.Errorf("worker: bad frames output")
			}
			var out struct {
				ResultsFrames int `json:"results_frames"`
			}
			if err := json.Unmarshal(outBytes[:nl], &out); err != nil {
				return nil, err
			}
			rest := outBytes[nl+1:]
			results := make([]any, 0, out.ResultsFrames)
			for i := 0; i < out.ResultsFrames; i++ {
				var f []byte
				f, rest, err = readFrame(rest)
				if err != nil {
					return nil, err
				}
				results = append(results, map[string]string{"pickle": string(f)})
			}
			return results, nil
		}
		var out struct {
			Results []string `json:"results"`
		}
		if err := json.Unmarshal(outBytes, &out); err != nil {
			return nil, err
		}
		results := make([]any, 0, len(out.Results))
		for _, r := range out.Results {
			if _, err := base64.StdEncoding.DecodeString(r); err != nil {
				return nil, err
			}
			results = append(results, map[string]string{"pickle": r})
		}
		return results, nil
	}
}

// stopAll terminates every warm worker; used on server shutdown.
func (pm *poolManager) stopAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, w := range pm.workers {
		if w != nil {
			w.kill()
		}
	}
	pm.workers = map[string]*poolWorker{}
}

// runPoolChunk is the cold path: one closure spawn per chunk. Kept for the
// fallback when warm workers are unavailable.
func runPoolChunk(runPath string, ch *chunk) ([]any, error) {
	if ch.chunkDir != "" {
		if err := os.MkdirAll(ch.chunkDir, 0755); err != nil {
			return nil, err
		}
	}
	dir, err := userdir.Scratch("pool-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	runnerPath := filepath.Join(dir, "pool_runner.py")
	if err := os.WriteFile(runnerPath, []byte(poolRunner), 0755); err != nil {
		return nil, err
	}

	inPath := filepath.Join(dir, "in.bin")
	outPath := filepath.Join(dir, "out.bin")
	var body []byte
	if ch.frames {
		hdr, _ := json.Marshal(map[string]any{
			"func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName,
			"starmap": ch.starmap, "items_frames": len(ch.items), "globals": ch.globals != nil,
			"required_mem": ch.requiredMem, "cache_keys": ch.cacheKeys, "item_keys": ch.itemKeys,
			"chunk_dir": ch.chunkDir, "items_zlib": ch.itemsZlib,
		})
		body = buildFrames(hdr, ch.globals, ch.items)
	} else {
		payload := map[string]any{"func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName, "extra_b64": ch.extraB64, "items": ch.items, "starmap": ch.starmap, "items_b64": ch.itemsB64, "chunk_dir": ch.chunkDir}
		if len(ch.cacheKeys) > 0 {
			payload["cache_keys"] = ch.cacheKeys
		}
		if len(ch.itemKeys) > 0 {
			payload["item_keys"] = ch.itemKeys
		}
		body, _ = json.Marshal(payload)
	}
	if err := os.WriteFile(inPath, body, 0644); err != nil {
		return nil, err
	}

	ctx := ch.context()
	cmd := termuxexec.CommandContext(ctx, runPath, runnerPath, inPath, outPath)
	// Own process group, killed whole on cancel (see killProcessGroup).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return killProcessGroup(cmd.Process) }
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("pool run failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		return nil, err
	}
	if ch.frames {
		nl := bytes.IndexByte(outBytes, '\n')
		if nl < 0 {
			return nil, fmt.Errorf("pool run: bad frames output")
		}
		var out struct {
			ResultsFrames int `json:"results_frames"`
		}
		if err := json.Unmarshal(outBytes[:nl], &out); err != nil {
			return nil, err
		}
		rest := outBytes[nl+1:]
		results := make([]any, 0, out.ResultsFrames)
		for i := 0; i < out.ResultsFrames; i++ {
			var f []byte
			f, rest, err = readFrame(rest)
			if err != nil {
				return nil, err
			}
			results = append(results, map[string]string{"pickle": string(f)})
		}
		return results, nil
	}
	var out struct {
		Results []string `json:"results"` // base64-pickled results
	}
	if err := json.Unmarshal(outBytes, &out); err != nil {
		return nil, err
	}

	results := make([]any, 0, len(out.Results))
	for _, r := range out.Results {
		if _, err := base64.StdEncoding.DecodeString(r); err != nil {
			return nil, err
		}
		// Ship the pickled blob back base64-encoded; the shim unpickles it.
		results = append(results, map[string]string{"pickle": r})
	}
	return results, nil
}

// chunkModules lists the modules a pool request needs, from its func_src and
// pickles (function, globals, and the first few items: items are uniform, and
// scanning gigabytes per request is not worth it). Compressed items are
// skipped.
func chunkModules(req *poolRequest, globals []byte, items []json.RawMessage, frames bool) []string {
	var pickles [][]byte
	for _, b64 := range []string{req.Func, req.ExtraB64} {
		if b64 == "" {
			continue
		}
		b, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			log.Printf("[pool] module scan: undecodable base64 pickle skipped: %v", err)
			continue
		}
		pickles = append(pickles, b)
	}
	pickles = append(pickles, globals)
	for i, it := range items {
		if i == 4 {
			break
		}
		switch {
		case frames && !req.ItemsZlib:
			pickles = append(pickles, it)
		case !frames && req.ItemsB64:
			var s string
			if json.Unmarshal(it, &s) != nil {
				continue
			}
			if b, err := base64.StdEncoding.DecodeString(s); err == nil {
				pickles = append(pickles, b)
			}
		}
	}
	return phone.Modules(req.FuncSrc, pickles...)
}
