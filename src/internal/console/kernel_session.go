package console

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	// A kernel with no client and no running cell is stopped after this long.
	kernelIdleTimeout = 30 * time.Minute
	reapInterval      = time.Minute
	// Output of the running cell kept for a client that reconnects mid-run.
	replayLimitBytes = 4 << 20
	// Frames a slow client may fall behind by before it is disconnected. It
	// reconnects and is brought up to date from the replay buffer.
	clientBacklog = 4096
)

// kernelConfig is what every kernel session needs from the server.
type kernelConfig struct {
	Python     string
	Env        []string
	Executable string
	Log        zerolog.Logger
}

// kernelManager owns the kernel sessions, one per notebook path.
type kernelManager struct {
	cfg kernelConfig

	mu       sync.Mutex
	sessions map[string]*session

	driverMu   sync.Mutex
	driverDir  string
	driverPath string
}

func newKernelManager(cfg kernelConfig) *kernelManager {
	return &kernelManager{cfg: cfg, sessions: map[string]*session{}}
}

// driver writes the embedded driver to disk on first use: Python needs a file
// to run, and tracebacks identify driver frames by its path.
func (m *kernelManager) driver() (string, error) {
	m.driverMu.Lock()
	defer m.driverMu.Unlock()
	if m.driverPath != "" {
		return m.driverPath, nil
	}
	dir, err := os.MkdirTemp("", "pipedpeer-kernel-")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "pipedpeer_kernel_driver.py")
	if err := os.WriteFile(path, driverSource, 0o600); err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	m.driverDir, m.driverPath = dir, path
	return path, nil
}

// session returns the kernel session for a notebook, creating it on first use.
// notebook is the notebook's path relative to the root; dir is where its
// kernel runs.
func (m *kernelManager) session(notebook, dir string) *session {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[notebook]; ok {
		return s
	}
	s := &session{
		mgr:      m,
		dir:      dir,
		state:    stateDead,
		clients:  map[*client]struct{}{},
		lastUsed: time.Now(),
	}
	m.sessions[notebook] = s
	return s
}

func (m *kernelManager) shutdown(notebook string) {
	m.mu.Lock()
	s := m.sessions[notebook]
	delete(m.sessions, notebook)
	m.mu.Unlock()
	if s != nil {
		s.close()
	}
}

// rename keeps a notebook's kernel, and so its state, across a file rename.
func (m *kernelManager) rename(from, to string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[from]
	if !ok {
		return
	}
	if _, taken := m.sessions[to]; taken {
		return
	}
	delete(m.sessions, from)
	m.sessions[to] = s
}

func (m *kernelManager) shutdownAll() {
	m.mu.Lock()
	sessions := m.sessions
	m.sessions = map[string]*session{}
	m.mu.Unlock()

	for _, s := range sessions {
		s.close()
	}

	m.driverMu.Lock()
	defer m.driverMu.Unlock()
	if m.driverDir != "" {
		os.RemoveAll(m.driverDir)
		m.driverDir, m.driverPath = "", ""
	}
}

func (m *kernelManager) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.reap(now)
		}
	}
}

func (m *kernelManager) reap(now time.Time) {
	m.mu.Lock()
	var expired []*session
	for name, s := range m.sessions {
		if s.idleSince(now) >= kernelIdleTimeout {
			expired = append(expired, s)
			delete(m.sessions, name)
		}
	}
	m.mu.Unlock()
	for _, s := range expired {
		s.close()
	}
}

// execution is one queued or running cell.
type execution struct {
	cellID  string
	code    string
	target  string
	count   int
	started time.Time
	// cancel stops a cluster execution. Nil for local ones.
	cancel context.CancelFunc
}

// client is one connected WebSocket. Frames are queued on out and written by
// the connection's own goroutine, so a slow browser never blocks the kernel.
type client struct {
	out  chan []byte
	once sync.Once
	gone chan struct{}
}

func newClient() *client {
	return &client{out: make(chan []byte, clientBacklog), gone: make(chan struct{})}
}

func (c *client) drop() { c.once.Do(func() { close(c.gone) }) }

// session is the kernel for one notebook: a Python process, the queue of
// cells waiting for it, and the clients watching.
type session struct {
	mgr *kernelManager
	dir string

	mu     sync.Mutex
	state  string
	python string
	proc   *kernelProc
	// gen identifies the current process, so callbacks from one that has been
	// replaced are ignored.
	gen      int
	count    int
	queue    []*execution
	running  *execution
	replay   [][]byte
	replaySz int
	clients  map[*client]struct{}
	lastUsed time.Time
	closed   bool
}

func (s *session) idleSince(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.clients) > 0 || s.running != nil || len(s.queue) > 0 {
		return 0
	}
	return now.Sub(s.lastUsed)
}

// attach registers a client, replays the running cell's output to it, and
// starts the kernel if this is the first use.
func (s *session) attach(c *client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[c] = struct{}{}
	s.lastUsed = time.Now()
	for _, frame := range s.replay {
		s.sendTo(c, frame)
	}
	if s.proc == nil && s.state == stateDead && !s.closed {
		s.startLocked()
		return
	}
	s.sendTo(c, s.statusLocked())
}

func (s *session) detach(c *client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, c)
	s.lastUsed = time.Now()
}

func (s *session) sendTo(c *client, frame []byte) {
	select {
	case c.out <- frame:
	default:
		delete(s.clients, c)
		c.drop()
	}
}

func (s *session) broadcast(v any) {
	frame, err := json.Marshal(v)
	if err != nil {
		return
	}
	s.broadcastRaw(frame)
}

func (s *session) broadcastRaw(frame []byte) {
	for c := range s.clients {
		s.sendTo(c, frame)
	}
}

// output broadcasts a frame belonging to the running cell and keeps it for
// clients that reconnect before the cell finishes.
func (s *session) output(v any) {
	frame, err := json.Marshal(v)
	if err != nil {
		return
	}
	// Output that arrives with nothing running is a finished cell's child
	// process still writing; it is delivered but not worth replaying.
	if s.running != nil {
		s.replay = append(s.replay, frame)
		s.replaySz += len(frame)
		for s.replaySz > replayLimitBytes && len(s.replay) > 1 {
			s.replaySz -= len(s.replay[0])
			s.replay = s.replay[1:]
		}
	}
	s.broadcastRaw(frame)
}

func (s *session) statusLocked() []byte {
	queued := make([]string, 0, len(s.queue))
	for _, e := range s.queue {
		queued = append(queued, e.cellID)
	}
	running := ""
	if s.running != nil {
		running = s.running.cellID
	}
	frame, _ := json.Marshal(statusFrame{
		Type:           "status",
		State:          s.state,
		Python:         s.python,
		Interception:   false,
		ExecutionCount: s.count,
		Queued:         queued,
		Running:        running,
	})
	return frame
}

func (s *session) setState(state string) {
	s.state = state
	s.broadcastRaw(s.statusLocked())
}

// startLocked launches a kernel process. The session becomes idle when the
// driver reports ready.
func (s *session) startLocked() {
	s.state = stateStarting
	s.python = ""
	s.broadcastRaw(s.statusLocked())

	driverPath, err := s.mgr.driver()
	if err != nil {
		s.failStart("kernel could not start: " + err.Error())
		return
	}
	s.gen++
	gen := s.gen
	proc, err := startKernelProc(s.mgr.cfg.Python, driverPath, s.dir, kernelEnv(s.mgr.cfg.Env),
		func(ev driverEvent) { s.onEvent(gen, ev) },
		func(waitErr error) { s.onExit(gen, waitErr) },
	)
	if err != nil {
		s.failStart("kernel could not start: " + err.Error())
		return
	}
	s.proc = proc
	s.mgr.cfg.Log.Info().Str("dir", s.dir).Int("pid", proc.cmd.Process.Pid).Msg("kernel started")
}

func (s *session) failStart(message string) {
	s.broadcast(fatalFrame{Type: "error_fatal", Message: message})
	s.abortLocalLocked()
	s.setState(stateDead)
	s.pumpLocked()
}

// kernelEnv is the kernel's environment: the base one plus settings that make
// a headless, piped interpreter behave like an interactive one.
func kernelEnv(base []string) []string {
	env := slices.Clone(base)
	env = append(env, "PYTHONUNBUFFERED=1", "PYTHONIOENCODING=utf-8")
	if !envHas(base, "MPLBACKEND") {
		env = append(env, "MPLBACKEND=Agg")
	}
	return env
}

func envHas(env []string, key string) bool {
	prefix := key + "="
	for _, kv := range env {
		if len(kv) >= len(prefix) && kv[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

func (s *session) onEvent(gen int, ev driverEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gen != s.gen || s.proc == nil {
		return
	}
	switch ev.Type {
	case "ready":
		s.python = ev.Python
		s.setStateForQueue()
		s.pumpLocked()
	case "stream":
		s.output(streamFrame{Type: "stream", CellID: ev.ID, Name: ev.Name, Text: ev.Text})
	case "display":
		s.output(displayFrame{Type: "display", CellID: ev.ID, Data: ev.Data, DisplayID: ev.DisplayID})
	case "result":
		s.output(resultFrame{Type: "result", CellID: ev.ID, ExecutionCount: ev.ExecutionCount, Data: ev.Data})
	case "error":
		s.output(errorFrame{Type: "error", CellID: ev.ID, Ename: ev.Ename, Evalue: ev.Evalue, Traceback: ev.Traceback})
	case "done":
		if s.running != nil && s.running.cellID == ev.ID && s.running.target == targetLocal {
			s.finishLocked(ev.Status)
		}
	case "complete_reply":
		matches := ev.Matches
		if matches == nil {
			matches = []string{}
		}
		s.broadcast(completeReplyFrame{Type: "complete_reply", ReqID: ev.ReqID, Matches: matches,
			CursorStart: ev.CursorStart, CursorEnd: ev.CursorEnd})
	case "vars_reply":
		s.broadcast(varsReplyFrame{Type: "vars_reply", ReqID: ev.ReqID, Vars: ev.Vars})
	}
}

func (s *session) onExit(gen int, waitErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gen != s.gen || s.proc == nil {
		return
	}
	message := s.proc.describeExit(waitErr)
	s.mgr.cfg.Log.Warn().Str("dir", s.dir).Str("reason", message).Msg("kernel exited")
	neverReady := s.python == ""
	s.proc = nil
	s.python = ""

	switch {
	case neverReady:
		// Queued local cells would only start it again, and it would fail
		// again.
		s.broadcast(fatalFrame{Type: "error_fatal", Message: "kernel could not start: " + message})
		s.abortLocalLocked()
	case s.running != nil && s.running.target == targetLocal:
		s.output(errorFrame{Type: "error", CellID: s.running.cellID, Ename: "KernelDied",
			Evalue: message, Traceback: []string{message}})
		s.finishRunningLocked(statusError)
		s.abortQueueLocked(func(*execution) bool { return true })
	}
	if s.running == nil {
		s.count = 0
	}
	s.setStateForQueue()
	s.pumpLocked()
}

// setStateForQueue derives the state from what is running and whether a
// kernel process exists.
func (s *session) setStateForQueue() {
	switch {
	case s.running != nil:
		s.setState(stateBusy)
	case s.proc == nil:
		s.setState(stateDead)
	case s.python == "":
		s.setState(stateStarting)
	default:
		s.setState(stateIdle)
	}
}

// finishRunningLocked ends the running execution with a done frame.
func (s *session) finishRunningLocked(status string) {
	e := s.running
	if e == nil {
		return
	}
	s.running = nil
	s.replay, s.replaySz = nil, 0
	s.lastUsed = time.Now()
	s.broadcast(doneFrame{Type: "done", CellID: e.cellID, ExecutionCount: e.count,
		Status: status, DurationMs: time.Since(e.started).Milliseconds()})
}

// finishLocked ends the running execution and moves on. As in Jupyter, a cell
// that fails stops the cells queued behind it.
func (s *session) finishLocked(status string) {
	s.finishRunningLocked(status)
	if status != statusOK {
		s.abortQueueLocked(func(*execution) bool { return true })
	}
	s.setStateForQueue()
	s.pumpLocked()
}

func (s *session) abortQueueLocked(match func(*execution) bool) {
	kept := s.queue[:0]
	for _, e := range s.queue {
		if !match(e) {
			kept = append(kept, e)
			continue
		}
		s.broadcast(doneFrame{Type: "done", CellID: e.cellID, Status: statusAborted})
	}
	s.queue = kept
}

func (s *session) abortLocalLocked() {
	s.abortQueueLocked(func(e *execution) bool { return e.target == targetLocal })
}

// pumpLocked starts the next queued cell if nothing is running.
func (s *session) pumpLocked() {
	if s.running != nil || len(s.queue) == 0 || s.closed {
		return
	}
	next := s.queue[0]
	if next.target == targetLocal {
		if s.proc == nil {
			s.startLocked()
			return
		}
		if s.python == "" {
			return
		}
	}
	s.queue = s.queue[1:]
	s.count++
	next.count = s.count
	next.started = time.Now()
	s.running = next
	s.replay, s.replaySz = nil, 0
	s.setState(stateBusy)

	if next.target == targetCluster {
		ctx, cancel := context.WithCancel(context.Background())
		next.cancel = cancel
		go s.runCluster(ctx, next)
		return
	}
	err := s.proc.send(driverCommand{Type: "execute", ID: next.cellID, Code: next.code, ExecutionCount: next.count})
	if err != nil {
		s.output(errorFrame{Type: "error", CellID: next.cellID, Ename: "KernelError",
			Evalue: err.Error(), Traceback: []string{err.Error()}})
		s.finishLocked(statusError)
	}
}

// ---- client commands ----

func (s *session) execute(cellID, code, target string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	e := &execution{cellID: cellID, code: code, target: target}
	s.queue = append(s.queue, e)
	s.lastUsed = time.Now()
	s.pumpLocked()
	// Starting the cell already announced it; otherwise say it is waiting.
	if slices.Contains(s.queue, e) {
		s.broadcastRaw(s.statusLocked())
	}
}

func (s *session) interrupt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running == nil {
		return
	}
	if s.running.target == targetCluster {
		s.running.cancel()
		return
	}
	if s.proc != nil {
		s.proc.interrupt()
	}
}

// restart replaces the kernel process. The running cell and everything queued
// end as aborted, and the namespace starts empty.
func (s *session) restart() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.stopLocked()
	s.startLocked()
}

// stop ends the kernel process and leaves the session dead until the next
// execute or restart.
func (s *session) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopLocked()
	s.setState(stateDead)
}

func (s *session) stopLocked() {
	if s.running != nil && s.running.cancel != nil {
		s.running.cancel()
	}
	s.finishRunningLocked(statusAborted)
	s.abortQueueLocked(func(*execution) bool { return true })
	if s.proc != nil {
		s.proc.kill()
		s.proc = nil
		s.gen++
	}
	s.python = ""
	s.count = 0
}

// close stops the kernel for good and disconnects its clients.
func (s *session) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.stopLocked()
	s.setState(stateDead)
	s.closed = true
	for c := range s.clients {
		c.drop()
	}
	s.clients = map[*client]struct{}{}
}

func (s *session) complete(reqID, code string, cursor *int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc == nil || s.python == "" ||
		s.proc.send(driverCommand{Type: "complete", ReqID: reqID, Code: code, Cursor: cursor}) != nil {
		end := len(code)
		if cursor != nil {
			end = *cursor
		}
		s.broadcast(completeReplyFrame{Type: "complete_reply", ReqID: reqID, Matches: []string{},
			CursorStart: end, CursorEnd: end})
	}
}

func (s *session) inspectVars(reqID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc == nil || s.python == "" ||
		s.proc.send(driverCommand{Type: "inspect_vars", ReqID: reqID}) != nil {
		s.broadcast(varsReplyFrame{Type: "vars_reply", ReqID: reqID, Vars: json.RawMessage("[]")})
	}
}
