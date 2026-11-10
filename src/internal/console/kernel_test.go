package console

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

// frame is any server frame, decoded loosely so tests can assert on the wire
// shape the frontend sees.
type frame map[string]any

func (f frame) str(key string) string {
	s, _ := f[key].(string)
	return s
}

// kernelConn is a WebSocket client for one notebook's kernel.
type kernelConn struct {
	t    *testing.T
	conn *websocket.Conn
}

func requirePython(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not found")
	}
}

func dialKernel(t *testing.T, ts *testServer, notebook string) *kernelConn {
	t.Helper()
	u := "ws://" + ts.Host + "/api/kernel/ws?notebook=" + url.QueryEscape(notebook)
	header := http.Header{"Authorization": {"Bearer " + ts.Token()}}
	conn, resp, err := websocket.DefaultDialer.Dial(u, header)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial kernel: %v (status %d)", err, status)
	}
	t.Cleanup(func() { conn.Close() })
	return &kernelConn{t: t, conn: conn}
}

func (k *kernelConn) send(v any) {
	k.t.Helper()
	if err := k.conn.WriteJSON(v); err != nil {
		k.t.Fatalf("send: %v", err)
	}
}

func (k *kernelConn) execute(cellID, code string) {
	k.t.Helper()
	k.send(map[string]any{"type": "execute", "cell_id": cellID, "code": code, "target": "local"})
}

// until reads frames until stop returns true and returns every frame read,
// the matching one last.
func (k *kernelConn) until(what string, stop func(frame) bool) []frame {
	k.t.Helper()
	var seen []frame
	_ = k.conn.SetReadDeadline(time.Now().Add(testTimeout))
	for {
		_, raw, err := k.conn.ReadMessage()
		if err != nil {
			k.t.Fatalf("waiting for %s: %v; frames so far: %v", what, err, seen)
		}
		var f frame
		if err := json.Unmarshal(raw, &f); err != nil {
			k.t.Fatalf("frame is not JSON: %s", raw)
		}
		seen = append(seen, f)
		if stop(f) {
			return seen
		}
	}
}

func (k *kernelConn) untilState(state string) []frame {
	k.t.Helper()
	return k.until("status "+state, func(f frame) bool {
		return f.str("type") == "status" && f.str("state") == state
	})
}

// run executes a cell and returns its frames up to and including done.
func (k *kernelConn) run(cellID, code string) []frame {
	k.t.Helper()
	k.execute(cellID, code)
	return k.untilDone(cellID)
}

func (k *kernelConn) untilDone(cellID string) []frame {
	k.t.Helper()
	return k.until("done "+cellID, func(f frame) bool {
		return f.str("type") == "done" && f.str("cell_id") == cellID
	})
}

func ofType(frames []frame, typ, cellID string) []frame {
	var out []frame
	for _, f := range frames {
		if f.str("type") == typ && f.str("cell_id") == cellID {
			out = append(out, f)
		}
	}
	return out
}

func streamText(frames []frame, cellID, name string) string {
	var b strings.Builder
	for _, f := range ofType(frames, "stream", cellID) {
		if f.str("name") == name {
			b.WriteString(f.str("text"))
		}
	}
	return b.String()
}

func tracebackText(errFrame frame) string {
	var b strings.Builder
	lines, _ := errFrame["traceback"].([]any)
	for _, line := range lines {
		b.WriteString(line.(string))
		b.WriteByte('\n')
	}
	return b.String()
}

func doneStatus(t *testing.T, frames []frame) string {
	t.Helper()
	last := frames[len(frames)-1]
	if last.str("type") != "done" {
		t.Fatalf("last frame is %v, want done", last)
	}
	return last.str("status")
}

func resultText(t *testing.T, frames []frame, cellID string) string {
	t.Helper()
	results := ofType(frames, "result", cellID)
	if len(results) != 1 {
		t.Fatalf("got %d result frames for %s, want 1: %v", len(results), cellID, frames)
	}
	data, _ := results[0]["data"].(map[string]any)
	text, _ := data["text/plain"].(string)
	return text
}

func startKernel(t *testing.T) (*testServer, *kernelConn) {
	t.Helper()
	requirePython(t)
	ts := newTestServer(t, Options{Env: os.Environ()})
	if err := os.WriteFile(filepath.Join(ts.root, "nb.ipynb"), []byte(emptyNotebook), 0o644); err != nil {
		t.Fatal(err)
	}
	k := dialKernel(t, ts, "nb.ipynb")
	frames := k.untilState(stateIdle)
	if frames[0].str("type") != "status" {
		t.Fatalf("first frame on connect is %v, want a status", frames[0])
	}
	return ts, k
}

func TestKernelSharesStateAcrossCells(t *testing.T) {
	_, k := startKernel(t)

	if got := doneStatus(t, k.run("c1", "x = 20\ny = 22")); got != statusOK {
		t.Fatalf("c1 status = %q", got)
	}
	frames := k.run("c2", "x + y")
	if got := resultText(t, frames, "c2"); got != "42" {
		t.Errorf("c2 result = %q, want 42", got)
	}
	done := frames[len(frames)-1]
	if done["execution_count"] != float64(2) {
		t.Errorf("c2 execution_count = %v, want 2", done["execution_count"])
	}
	if _, ok := done["duration_ms"].(float64); !ok {
		t.Errorf("done frame has no numeric duration_ms: %v", done)
	}
}

func TestKernelStatusFrameShape(t *testing.T) {
	_, k := startKernel(t)
	k.send(map[string]any{"type": "execute", "cell_id": "c1", "code": "1", "target": "local"})
	frames := k.untilDone("c1")
	frames = append(frames, k.untilState(stateIdle)...)

	var busy frame
	for _, f := range frames {
		if f.str("type") == "status" && f.str("state") == stateBusy {
			busy = f
		}
	}
	if busy == nil {
		t.Fatalf("no busy status while c1 ran: %v", frames)
	}
	if busy.str("running") != "c1" {
		t.Errorf("busy status running = %v, want c1", busy["running"])
	}
	if busy["interception"] != false {
		t.Errorf("interception = %v, want false: a local kernel cannot reach the cluster pool", busy["interception"])
	}
	if !strings.HasPrefix(busy.str("python"), "3.") {
		t.Errorf("python = %q, want a 3.x version", busy.str("python"))
	}
	if _, ok := busy["queued"].([]any); !ok {
		t.Errorf("queued = %v, want an array", busy["queued"])
	}
}

func TestKernelStreamsStdoutLive(t *testing.T) {
	_, k := startKernel(t)

	// The cell blocks after printing; seeing the text before interrupting
	// proves it was streamed and not delivered at the end.
	k.execute("c1", "import time\nprint('early')\ntime.sleep(60)")
	k.until("early output", func(f frame) bool {
		return f.str("type") == "stream" && f.str("cell_id") == "c1" && strings.Contains(f.str("text"), "early")
	})
	k.send(map[string]any{"type": "interrupt"})
	if got := doneStatus(t, k.untilDone("c1")); got != statusError {
		t.Errorf("interrupted cell status = %q, want error", got)
	}

	frames := k.run("c2", "import sys\nprint('out')\nprint('err', file=sys.stderr)")
	if got := streamText(frames, "c2", "stdout"); got != "out\n" {
		t.Errorf("stdout = %q", got)
	}
	if got := streamText(frames, "c2", "stderr"); got != "err\n" {
		t.Errorf("stderr = %q", got)
	}
}

func TestKernelLastExpression(t *testing.T) {
	_, k := startKernel(t)

	if got := resultText(t, k.run("c1", "a = 3\na * 2"), "c1"); got != "6" {
		t.Errorf("result = %q, want 6", got)
	}
	for name, code := range map[string]string{
		"assignment":   "b = 5",
		"None value":   "None",
		"semicolon":    "a * 2;",
		"print":        "print(a)",
		"empty source": "",
	} {
		frames := k.run("quiet", code)
		if n := len(ofType(frames, "result", "quiet")); n != 0 {
			t.Errorf("%s: got %d result frames, want none", name, n)
		}
	}

	frames := k.run("rich", strings.Join([]string{
		"class Rich:",
		"    def _repr_html_(self): return '<b>hi</b>'",
		"    def _repr_json_(self): return {'k': [1, 2]}",
		"Rich()",
	}, "\n"))
	data, _ := ofType(frames, "result", "rich")[0]["data"].(map[string]any)
	if data["text/html"] != "<b>hi</b>" {
		t.Errorf("text/html = %v", data["text/html"])
	}
	if _, ok := data["text/plain"].(string); !ok {
		t.Errorf("result data has no text/plain: %v", data)
	}
	if obj, _ := data["application/json"].(map[string]any); obj["k"] == nil {
		t.Errorf("application/json = %v", data["application/json"])
	}

	frames = k.run("disp", "display('shown')\nprint('after')")
	displays := ofType(frames, "display", "disp")
	if len(displays) != 1 {
		t.Fatalf("got %d display frames, want 1", len(displays))
	}
	if data, _ := displays[0]["data"].(map[string]any); data["text/plain"] != "'shown'" {
		t.Errorf("display text/plain = %v", data["text/plain"])
	}
}

func TestKernelExceptionGivesErrorThenDone(t *testing.T) {
	_, k := startKernel(t)

	frames := k.run("c1", "def boom():\n    raise ValueError('bad value')\nprint('before')\nboom()")
	if got := doneStatus(t, frames); got != statusError {
		t.Fatalf("status = %q, want error", got)
	}
	errs := ofType(frames, "error", "c1")
	if len(errs) != 1 {
		t.Fatalf("got %d error frames, want 1", len(errs))
	}
	e := errs[0]
	if e.str("ename") != "ValueError" || e.str("evalue") != "bad value" {
		t.Errorf("ename/evalue = %q/%q", e.str("ename"), e.str("evalue"))
	}
	text := tracebackText(e)
	if !strings.Contains(text, "<cell 1>") {
		t.Errorf("traceback does not name the cell as the file:\n%s", text)
	}
	if !strings.Contains(text, "raise ValueError('bad value')") {
		t.Errorf("traceback does not show the cell's source line:\n%s", text)
	}
	if strings.Contains(text, "pipedpeer_kernel_driver") {
		t.Errorf("traceback shows driver frames:\n%s", text)
	}
	if got := streamText(frames, "c1", "stdout"); got != "before\n" {
		t.Errorf("stdout before the error = %q", got)
	}

	// The kernel survives and keeps its state.
	if got := resultText(t, k.run("c2", "boom.__name__"), "c2"); got != "'boom'" {
		t.Errorf("after error, result = %q", got)
	}

	frames = k.run("c3", "def broken(:\n    pass")
	if got := doneStatus(t, frames); got != statusError {
		t.Errorf("syntax error status = %q", got)
	}
	if errs := ofType(frames, "error", "c3"); len(errs) != 1 || errs[0].str("ename") != "SyntaxError" {
		t.Errorf("syntax error frames = %v", errs)
	}
}

func TestKernelInterrupt(t *testing.T) {
	_, k := startKernel(t)

	k.execute("c1", "print('spinning')\nwhile True: pass")
	k.until("cell output", func(f frame) bool { return f.str("type") == "stream" })
	k.send(map[string]any{"type": "interrupt"})

	frames := k.untilDone("c1")
	if got := doneStatus(t, frames); got != statusError {
		t.Fatalf("status = %q, want error", got)
	}
	errs := ofType(frames, "error", "c1")
	if len(errs) != 1 || errs[0].str("ename") != "KeyboardInterrupt" {
		t.Fatalf("error frames = %v, want one KeyboardInterrupt", errs)
	}

	if tb := tracebackText(errs[0]); strings.Contains(tb, "pipedpeer_kernel_driver") || !strings.Contains(tb, "while True: pass") {
		t.Errorf("interrupt traceback should show the cell's line and no driver frames:\n%s", tb)
	}

	// Interrupting must not cost the kernel its state.
	k.run("c2", "kept = 7")
	if got := resultText(t, k.run("c3", "kept"), "c3"); got != "7" {
		t.Errorf("after interrupt, result = %q", got)
	}
}

func TestKernelRestartClearsState(t *testing.T) {
	_, k := startKernel(t)

	k.run("c1", "secret = 1")
	k.send(map[string]any{"type": "restart"})
	k.untilState(stateStarting)
	k.untilState(stateIdle)

	frames := k.run("c2", "secret")
	errs := ofType(frames, "error", "c2")
	if len(errs) != 1 || errs[0].str("ename") != "NameError" {
		t.Fatalf("after restart, frames = %v; want a NameError", frames)
	}
	if count := frames[len(frames)-1]["execution_count"]; count != float64(1) {
		t.Errorf("execution_count after restart = %v, want 1", count)
	}
}

func TestKernelRestartAbortsQueue(t *testing.T) {
	_, k := startKernel(t)

	k.execute("running", "import time\nprint('go')\ntime.sleep(60)")
	k.until("cell output", func(f frame) bool { return f.str("type") == "stream" })
	k.execute("waiting", "1")
	k.until("queued status", func(f frame) bool {
		q, _ := f["queued"].([]any)
		return f.str("type") == "status" && len(q) == 1 && q[0] == "waiting"
	})
	k.send(map[string]any{"type": "restart"})

	frames := k.untilState(stateIdle)
	for _, cell := range []string{"running", "waiting"} {
		done := ofType(frames, "done", cell)
		if len(done) != 1 || done[0].str("status") != statusAborted {
			t.Errorf("%s: done frames = %v, want one aborted", cell, done)
		}
	}
}

func TestKernelCapturesSubprocessOutput(t *testing.T) {
	_, k := startKernel(t)

	frames := k.run("c1", strings.Join([]string{
		"import subprocess, sys, os",
		"subprocess.run([sys.executable, '-c', 'print(\"from child\")'])",
		"os.system('echo from shell')",
		"os.write(1, b'raw fd write\\n')",
	}, "\n"))
	out := streamText(frames, "c1", "stdout")
	for _, want := range []string{"from child\n", "from shell\n", "raw fd write\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout %q is missing %q", out, want)
		}
	}
}

func TestKernelQueueRunsInOrder(t *testing.T) {
	_, k := startKernel(t)

	k.execute("a", "import time\ntime.sleep(0.3)\norder = ['a']")
	k.execute("b", "order.append('b')")
	k.execute("c", "order")
	frames := k.untilDone("c")
	if got := resultText(t, frames, "c"); got != "['a', 'b']" {
		t.Errorf("order = %s", got)
	}
}

func TestKernelErrorAbortsQueuedCells(t *testing.T) {
	_, k := startKernel(t)

	k.execute("a", "import time\ntime.sleep(0.3)\nraise RuntimeError('stop')")
	k.execute("b", "print('must not run')")
	frames := k.untilDone("b")
	if got := doneStatus(t, frames); got != statusAborted {
		t.Errorf("cell queued behind a failure: status = %q, want aborted", got)
	}
	if out := streamText(frames, "b", "stdout"); out != "" {
		t.Errorf("aborted cell produced output %q", out)
	}
}

func TestKernelReplaysRunningCellOnReconnect(t *testing.T) {
	ts, k := startKernel(t)

	k.execute("c1", "import time\nprint('first half')\ntime.sleep(60)")
	k.until("cell output", func(f frame) bool { return f.str("type") == "stream" })
	k.conn.Close()

	again := dialKernel(t, ts, "nb.ipynb")
	frames := again.until("status after replay", func(f frame) bool { return f.str("type") == "status" })
	if got := streamText(frames, "c1", "stdout"); got != "first half\n" {
		t.Errorf("replayed stdout = %q, want the output from before the reconnect", got)
	}
	status := frames[len(frames)-1]
	if status.str("state") != stateBusy || status.str("running") != "c1" {
		t.Errorf("status after replay = %v, want busy running c1", status)
	}
	again.send(map[string]any{"type": "interrupt"})
	again.untilDone("c1")
}

func TestKernelCompleteAndInspect(t *testing.T) {
	_, k := startKernel(t)
	k.run("c1", "alpha_value = [1, 2, 3]\nimport os")

	k.send(map[string]any{"type": "complete", "req_id": "r1", "code": "alpha_v", "cursor": 7})
	reply := k.until("complete_reply", func(f frame) bool { return f.str("type") == "complete_reply" })
	got := reply[len(reply)-1]
	if got.str("req_id") != "r1" || got["cursor_start"] != float64(0) || got["cursor_end"] != float64(7) {
		t.Errorf("complete_reply = %v", got)
	}
	if matches, _ := got["matches"].([]any); len(matches) != 1 || matches[0] != "alpha_value" {
		t.Errorf("matches = %v, want [alpha_value]", got["matches"])
	}

	k.send(map[string]any{"type": "complete", "req_id": "r2", "code": "x = os.path.joi", "cursor": 15})
	reply = k.until("complete_reply", func(f frame) bool { return f.str("req_id") == "r2" })
	got = reply[len(reply)-1]
	if matches, _ := got["matches"].([]any); len(matches) != 1 || matches[0] != "join" {
		t.Errorf("attribute matches = %v, want [join]", got["matches"])
	}
	if got["cursor_start"] != float64(12) {
		t.Errorf("attribute cursor_start = %v, want 12", got["cursor_start"])
	}

	k.send(map[string]any{"type": "inspect_vars", "req_id": "r3"})
	reply = k.until("vars_reply", func(f frame) bool { return f.str("type") == "vars_reply" })
	vars, _ := reply[len(reply)-1]["vars"].([]any)
	if len(vars) != 1 {
		t.Fatalf("vars = %v, want only alpha_value (modules are not listed)", vars)
	}
	v := vars[0].(map[string]any)
	if v["name"] != "alpha_value" || v["type"] != "list" || v["repr"] != "[1, 2, 3]" {
		t.Errorf("var = %v", v)
	}
	if size, _ := v["size_bytes"].(float64); size <= 0 {
		t.Errorf("size_bytes = %v, want > 0", v["size_bytes"])
	}
}

func TestKernelShellAndAwait(t *testing.T) {
	_, k := startKernel(t)

	frames := k.run("c1", "!echo from bang\nx = 1")
	if got := streamText(frames, "c1", "stdout"); got != "from bang\n" {
		t.Errorf("!shell stdout = %q", got)
	}
	frames = k.run("c2", "import asyncio\nawait asyncio.sleep(0)\n41 + x")
	if got := resultText(t, frames, "c2"); got != "42" {
		t.Errorf("top-level await result = %q", got)
	}
	frames = k.run("c3", "%time sum(range(10))")
	if got := resultText(t, frames, "c3"); got != "45" {
		t.Errorf("%%time result = %q, want the expression's value", got)
	}
	if out := streamText(frames, "c3", "stdout"); !strings.Contains(out, "Wall time: ") {
		t.Errorf("%%time printed %q, want a wall time", out)
	}
	// A cell that is valid Python is never rewritten, even if a line inside a
	// string starts with "!".
	frames = k.run("c4", "text = \"\"\"\n!not a command\n\"\"\"\ntext")
	if got := resultText(t, frames, "c4"); got != `'\n!not a command\n'` {
		t.Errorf("string containing a ! line = %s", got)
	}
}

func TestKernelMultiprocessingUsesCellFunctions(t *testing.T) {
	_, k := startKernel(t)

	frames := k.run("c1", strings.Join([]string{
		"import multiprocessing",
		"def square(n): return n * n",
		"with multiprocessing.Pool(2) as pool:",
		"    out = pool.map(square, range(5))",
		"out",
	}, "\n"))
	if got := doneStatus(t, frames); got != statusOK {
		t.Fatalf("status = %q: %v", got, ofType(frames, "error", "c1"))
	}
	if got := resultText(t, frames, "c1"); got != "[0, 1, 4, 9, 16]" {
		t.Errorf("pool.map result = %s", got)
	}
}

func TestKernelCapturesMatplotlibFigures(t *testing.T) {
	requirePython(t)
	if exec.Command("python3", "-c", "import matplotlib").Run() != nil {
		t.Skip("matplotlib is not installed")
	}
	_, k := startKernel(t)

	frames := k.run("c1", strings.Join([]string{
		"import matplotlib.pyplot as plt",
		"plt.plot([1, 2, 3])",
		"plt.show()",
		"print('after show')",
		"plt.figure(); plt.plot([3, 2, 1])",
	}, "\n"))
	if got := doneStatus(t, frames); got != statusOK {
		t.Fatalf("status = %q: %s", got, tracebackText(ofType(frames, "error", "c1")[0]))
	}
	displays := ofType(frames, "display", "c1")
	if len(displays) != 2 {
		t.Fatalf("got %d display frames, want one for show() and one for the figure left open", len(displays))
	}
	for _, d := range displays {
		data, _ := d["data"].(map[string]any)
		png, _ := data["image/png"].(string)
		raw, err := base64.StdEncoding.DecodeString(png)
		if err != nil || !strings.HasPrefix(string(raw), "\x89PNG") {
			t.Errorf("image/png is not a base64 PNG (%d chars, err %v)", len(png), err)
		}
		if _, ok := data["text/plain"].(string); !ok {
			t.Errorf("figure display has no text/plain: %v", data)
		}
	}
	if errOut := streamText(frames, "c1", "stderr"); errOut != "" {
		t.Errorf("stderr = %q, want no backend warning", errOut)
	}

	// Figures were closed after being shown, so the next cell emits none.
	if n := len(ofType(k.run("c2", "1"), "display", "c2")); n != 0 {
		t.Errorf("next cell re-emitted %d figures", n)
	}
}

func TestKernelDeathIsReported(t *testing.T) {
	_, k := startKernel(t)

	k.execute("c1", "import os\nos._exit(3)")
	frames := k.untilState(stateDead)
	errs := ofType(frames, "error", "c1")
	if len(errs) != 1 || errs[0].str("ename") != "KernelDied" {
		t.Fatalf("frames = %v, want a KernelDied error for c1", frames)
	}
	if done := ofType(frames, "done", "c1"); len(done) != 1 || done[0].str("status") != statusError {
		t.Errorf("done frames = %v, want one with status error", done)
	}

	// The next execute brings up a fresh kernel.
	if got := resultText(t, k.run("c2", "1 + 1"), "c2"); got != "2" {
		t.Errorf("after kernel death, result = %q", got)
	}
}

func TestKernelMissingPythonIsFatal(t *testing.T) {
	ts := newTestServer(t, Options{Python: filepath.Join(t.TempDir(), "no-such-python")})
	if err := os.WriteFile(filepath.Join(ts.root, "nb.ipynb"), []byte(emptyNotebook), 0o644); err != nil {
		t.Fatal(err)
	}
	k := dialKernel(t, ts, "nb.ipynb")
	frames := k.untilState(stateDead)
	var fatal frame
	for _, f := range frames {
		if f.str("type") == "error_fatal" {
			fatal = f
		}
	}
	if fatal == nil || !strings.Contains(fatal.str("message"), "kernel could not start") {
		t.Fatalf("frames = %v, want an error_fatal saying the kernel could not start", frames)
	}

	frames = k.run("c1", "1")
	if got := doneStatus(t, frames); got != statusAborted {
		t.Errorf("execute with no interpreter: status = %q, want aborted", got)
	}
}

func TestKernelWebSocketRejectsBadNotebookPath(t *testing.T) {
	ts := newTestServer(t, Options{})
	for _, path := range []string{"", "../escape.ipynb", "notes.txt"} {
		resp, body := ts.do(t, http.MethodGet, "/api/kernel/ws?notebook="+url.QueryEscape(path), "", nil)
		wantStatus(t, resp, body, http.StatusBadRequest)
		wantErrorBody(t, body)
	}
}

func TestKernelShutdownThenExecuteStartsFresh(t *testing.T) {
	_, k := startKernel(t)
	k.run("c1", "kept = 1")

	k.send(map[string]any{"type": "shutdown"})
	k.untilState(stateDead)

	frames := k.run("c2", "kept")
	if errs := ofType(frames, "error", "c2"); len(errs) != 1 || errs[0].str("ename") != "NameError" {
		t.Errorf("after shutdown, frames = %v; want a fresh kernel and a NameError", frames)
	}
}

func TestKernelReapedOnlyWhenUnused(t *testing.T) {
	ts, k := startKernel(t)
	sess := ts.kernels.session("nb.ipynb", ts.root)
	longAgo := time.Now().Add(-2 * kernelIdleTimeout)
	setLastUsed := func() {
		sess.mu.Lock()
		sess.lastUsed = longAgo
		sess.mu.Unlock()
	}
	alive := func() bool {
		ts.kernels.mu.Lock()
		defer ts.kernels.mu.Unlock()
		return ts.kernels.sessions["nb.ipynb"] == sess
	}

	// A connected client keeps the kernel however long it has sat idle.
	setLastUsed()
	ts.kernels.reap(time.Now())
	if !alive() {
		t.Fatal("kernel reaped while a client was connected")
	}

	// So does a running cell after the client has gone.
	k.execute("c1", "import time\nprint('running')\ntime.sleep(60)")
	k.until("cell output", func(f frame) bool { return f.str("type") == "stream" })
	k.conn.Close()
	waitFor(t, "client to detach", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return len(sess.clients) == 0
	})
	setLastUsed()
	ts.kernels.reap(time.Now())
	if !alive() {
		t.Fatal("kernel reaped while a cell was running")
	}

	sess.interrupt()
	waitFor(t, "cell to stop", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.running == nil
	})
	sess.mu.Lock()
	pid := sess.proc.cmd.Process.Pid
	sess.mu.Unlock()
	setLastUsed()
	ts.kernels.reap(time.Now())
	if alive() {
		t.Fatal("idle kernel with no client was not reaped")
	}
	waitFor(t, "kernel process to exit", func() bool {
		return syscall.Kill(pid, 0) != nil
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// fakePipedpeer writes a stand-in for the pipedpeer binary that handles
// `run <script>` the way the tests need.
func fakePipedpeer(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipedpeer")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func dialClusterKernel(t *testing.T, executable string) *kernelConn {
	t.Helper()
	// An interpreter that cannot start proves cluster cells do not need the
	// local kernel.
	ts := newTestServer(t, Options{
		Python:     filepath.Join(t.TempDir(), "no-such-python"),
		Executable: executable,
		Env:        os.Environ(),
	})
	k := dialKernel(t, ts, "nb.ipynb")
	k.untilState(stateDead)
	return k
}

func (k *kernelConn) runCluster(cellID, code string) []frame {
	k.t.Helper()
	k.send(map[string]any{"type": "execute", "cell_id": cellID, "code": code, "target": "cluster"})
	return k.untilDone(cellID)
}

func TestClusterCellSubmitsThroughRun(t *testing.T) {
	exe := fakePipedpeer(t, `echo "args: $# $1 $(basename "$2")"; echo "--- script"; cat "$2"; echo; echo "progress" >&2`)
	k := dialClusterKernel(t, exe)

	frames := k.runCluster("c1", "print('héllo from the cluster')")
	if got := doneStatus(t, frames); got != statusOK {
		t.Fatalf("status = %q: %v", got, frames)
	}
	out := streamText(frames, "c1", "stdout")
	if !strings.Contains(out, "args: 2 run notebook_cell.py\n") {
		t.Errorf("run was not invoked as `run <script>`: %q", out)
	}
	if !strings.Contains(out, "print('héllo from the cluster')") {
		t.Errorf("the submitted script is not the cell's code: %q", out)
	}
	if got := streamText(frames, "c1", "stderr"); got != "progress\n" {
		t.Errorf("stderr = %q", got)
	}
	if done := frames[len(frames)-1]; done["execution_count"] != float64(1) {
		t.Errorf("execution_count = %v, want 1", done["execution_count"])
	}
}

func TestClusterCellFailureIsAnError(t *testing.T) {
	k := dialClusterKernel(t, fakePipedpeer(t, `echo "nix build failed" >&2; exit 7`))

	frames := k.runCluster("c1", "1")
	if got := doneStatus(t, frames); got != statusError {
		t.Fatalf("status = %q, want error", got)
	}
	errs := ofType(frames, "error", "c1")
	if len(errs) != 1 || errs[0].str("ename") != "ClusterJobFailed" || !strings.Contains(errs[0].str("evalue"), "exit status 7") {
		t.Errorf("error frames = %v", errs)
	}
	if got := streamText(frames, "c1", "stderr"); got != "nix build failed\n" {
		t.Errorf("stderr = %q", got)
	}
}

func TestClusterCellInterrupt(t *testing.T) {
	k := dialClusterKernel(t, fakePipedpeer(t, `echo started; exec sleep 300`))

	k.send(map[string]any{"type": "execute", "cell_id": "c1", "code": "1", "target": "cluster"})
	k.until("job output", func(f frame) bool { return f.str("type") == "stream" })
	k.send(map[string]any{"type": "interrupt"})

	frames := k.untilDone("c1")
	if got := doneStatus(t, frames); got != statusError {
		t.Fatalf("status = %q, want error", got)
	}
	if errs := ofType(frames, "error", "c1"); len(errs) != 1 || errs[0].str("ename") != "KeyboardInterrupt" {
		t.Errorf("error frames = %v, want one KeyboardInterrupt", errs)
	}
}

func TestClusterCellWithoutBinary(t *testing.T) {
	k := dialClusterKernel(t, "")
	frames := k.runCluster("c1", "1")
	if got := doneStatus(t, frames); got != statusError {
		t.Fatalf("status = %q, want error", got)
	}
	if errs := ofType(frames, "error", "c1"); len(errs) != 1 || errs[0].str("ename") != "ClusterUnavailable" {
		t.Errorf("error frames = %v", errs)
	}
}

func TestSplitValidUTF8(t *testing.T) {
	whole := "aé€😀"
	for cut := 0; cut <= len(whole); cut++ {
		text, rest := splitValidUTF8([]byte(whole[:cut]))
		text2, rest2 := splitValidUTF8(append(rest, whole[cut:]...))
		if got := text + text2; got != whole || len(rest2) != 0 {
			t.Errorf("cut at %d: reassembled %q with %d bytes left over, want %q", cut, got, len(rest2), whole)
		}
		if !utf8.ValidString(text) {
			t.Errorf("cut at %d: emitted invalid UTF-8 %q", cut, text)
		}
	}
	if text, rest := splitValidUTF8([]byte("ok\xff")); text != "ok\xff" || rest != nil {
		t.Errorf("an invalid byte must pass through, got %q rest %q", text, rest)
	}
}
