// Package console serves the local web console: cluster analytics and a
// notebook whose cells run in a persistent Python kernel on this machine.
//
// The server executes arbitrary code on behalf of whoever can reach it, so it
// binds loopback only and every API request must carry the start-up token and
// a Host header naming the loopback address it was started on.
package console

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

const (
	defaultSampleInterval = 5 * time.Second
	shutdownGrace         = 3 * time.Second
	readHeaderTimeout     = 10 * time.Second
)

// Options configures a console server. Everything environment-derived is
// resolved by the caller and passed in here.
type Options struct {
	// Port is the loopback TCP port to listen on. 0 picks a free one.
	Port int
	// DaemonPort is the local daemon's API port.
	DaemonPort int
	// Dir is the notebook root. Notebook paths never resolve outside it.
	Dir string
	// Python is the interpreter the kernel runs.
	Python string
	// DevOrigin additionally allows one Origin, for a dev server that proxies
	// /api to this process.
	DevOrigin string
	// SelfNodeID and SelfHostname identify this machine in the analytics view.
	SelfNodeID   string
	SelfHostname string
	// HistoryDir is the job history directory. Empty means the default store.
	HistoryDir string
	// Executable is the pipedpeer binary used to submit cluster cells.
	Executable string
	// Env is the environment kernels and cluster submissions start with.
	Env []string
	// SampleInterval is how often node load is sampled into the metrics
	// buffer. Zero means the default.
	SampleInterval time.Duration
	// Ready, when set, is called once the listener is bound, with the URL
	// (token included) that opens the console.
	Ready func(url string)
	Log   zerolog.Logger
}

// Server is a configured console. Create one with New.
type Server struct {
	opts    Options
	token   string
	root    string
	metrics *metricsBuffer
	history *historyStore
	kernels *kernelManager
}

// New validates opts and builds a server that is not yet listening.
func New(opts Options) (*Server, error) {
	if opts.Dir == "" {
		opts.Dir = "."
	}
	abs, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, err
	}
	root, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("notebook directory: %w", err)
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("notebook directory %s is not a directory", root)
	}
	if opts.Python == "" {
		opts.Python = "python3"
	}
	if opts.HistoryDir == "" {
		opts.HistoryDir = jobhistory.BaseDir()
	}
	if opts.SampleInterval <= 0 {
		opts.SampleInterval = defaultSampleInterval
	}
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	s := &Server{
		opts:    opts,
		token:   token,
		root:    root,
		metrics: newMetricsBuffer(opts.SampleInterval),
		history: newHistoryStore(opts.HistoryDir, opts.SelfHostname),
	}
	s.kernels = newKernelManager(kernelConfig{
		Python:     opts.Python,
		Env:        opts.Env,
		Executable: opts.Executable,
		Log:        opts.Log,
	})
	return s, nil
}

// Token is the secret a client must present.
func (s *Server) Token() string { return s.token }

// Run builds a server from opts and serves until ctx is cancelled.
func Run(ctx context.Context, opts Options) error {
	s, err := New(opts)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp4", net.JoinHostPort(loopbackIP, fmt.Sprint(opts.Port)))
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve serves on ln until ctx is cancelled, then stops every kernel. ln must
// be a loopback listener.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || !addr.IP.IsLoopback() {
		ln.Close()
		return fmt.Errorf("console refuses to serve on non-loopback address %s", ln.Addr())
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go s.sampleLoop(ctx)
	go s.kernels.reapLoop(ctx)
	defer s.kernels.shutdownAll()

	srv := &http.Server{
		Handler:           s.Handler(addr.Port),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	if s.opts.Ready != nil {
		s.opts.Ready(fmt.Sprintf("http://%s:%d/?token=%s", loopbackIP, addr.Port, s.token))
	}

	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// WebSocket connections are hijacked and never become idle.
		srv.Close()
	}
	if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Handler returns the console's HTTP handler for a server reachable on port.
func (s *Server) Handler(port int) http.Handler {
	guard := newGuard(s.token, port, s.opts.DevOrigin)

	r := chi.NewRouter()
	r.Use(guard.checkHost)

	r.Route("/api", func(api chi.Router) {
		api.Use(guard.requireToken)
		api.NotFound(func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusNotFound, "not found")
		})
		api.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		})

		api.Get("/overview", s.handleOverview)
		api.Get("/nodes", s.handleNodes)
		api.Get("/metrics", s.handleMetrics)
		api.Get("/tasks", s.handleTasks)
		api.Get("/jobs", s.handleJobs)
		api.Get("/jobs/{id}", s.handleJob)
		api.Get("/analytics", s.handleAnalytics)

		api.Get("/notebooks", s.handleNotebookList)
		api.Post("/notebooks", s.handleNotebookCreate)
		api.Get("/notebooks/content", s.handleNotebookGet)
		api.Put("/notebooks/content", s.handleNotebookPut)
		api.Delete("/notebooks/content", s.handleNotebookDelete)
		api.Post("/notebooks/rename", s.handleNotebookRename)

		api.With(guard.checkOrigin).Get("/kernel/ws", s.handleKernelWS)
	})

	r.NotFound(guard.exchangeToken(staticHandler()).ServeHTTP)
	return r
}
