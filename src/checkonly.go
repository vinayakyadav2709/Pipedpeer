package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/app"
	"github.com/pipedpeer/pipedpeer/internal/coordinator"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/pythondeps"
	"github.com/pipedpeer/pipedpeer/internal/resourceest"
)

// checkOnlyOptions is everything `run --check-only` already knows when it
// hands over: the run flags plus the memory estimate it printed.
type checkOnlyOptions struct {
	ScriptPath     string
	DaemonHost     string // --host as typed, "" = auto
	DaemonPort     int
	Remote         bool
	RequireGPU     bool
	PreferGPU      bool
	RequiredGPUMem int64
	RequiredCores  int
	DDPNodes       int
	Pkgs           []string
	PythonVersion  string
	RegistryURL    string
	Strategy       string
	JobName        string
	NodeID         identity.NodeIdentity
	EstMemBytes    int64
	EstTier        string
}

// checkLineWidth keeps every line readable on a laptop held up to an audience.
const checkLineWidth = 72

// spreadPattern is one call the shim (or DDP) can spread across the cluster.
type spreadPattern struct {
	label string
	re    *regexp.Regexp
}

// DECIDED: hand-rolled regex scan, not a Python AST: the submitter has no
// Python outside the nix closure. ponytail: a call split over two lines or
// hidden behind an alias is missed; an AST pass inside the closure fixes that.
var spreadPatterns = []spreadPattern{
	{"multiprocessing.Pool", regexp.MustCompile(`\bPool\s*\(`)},
	{"ProcessPoolExecutor", regexp.MustCompile(`\bProcessPoolExecutor\s*\(`)},
	{"n_jobs= (sklearn/joblib)", regexp.MustCompile(`\bn_jobs\s*=`)},
	{"joblib", regexp.MustCompile(`\bjoblib\b`)},
	{"pandas read_csv/read_parquet", regexp.MustCompile(`\.read_(csv|parquet)\s*\(`)},
	{"pandas groupby", regexp.MustCompile(`\.groupby\s*\(`)},
	{"numpy linalg/dot/matmul", regexp.MustCompile(`\b(np|numpy)\.(linalg|dot|matmul)\b`)},
	{"torch (GPU/DDP)", regexp.MustCompile(`\btorch\b`)},
}

type spreadHit struct {
	Label string
	Line  int
}

// scanSpreads returns the first line of each interceptable call in src, in
// pattern order. Comment lines are skipped.
func scanSpreads(src string) []spreadHit {
	first := map[string]int{}
	for i, line := range strings.Split(src, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		for _, p := range spreadPatterns {
			if _, seen := first[p.label]; !seen && p.re.MatchString(t) {
				first[p.label] = i + 1
			}
		}
	}
	var hits []spreadHit
	for _, p := range spreadPatterns {
		if n, ok := first[p.label]; ok {
			hits = append(hits, spreadHit{p.label, n})
		}
	}
	return hits
}

// fit cuts s to checkLineWidth runes, marking the cut.
func fit(s string) string {
	r := []rune(s)
	if len(r) <= checkLineWidth {
		return s
	}
	return string(r[:checkLineWidth-1]) + "…"
}

type checkReport struct {
	color    bool
	blockers []string
}

func (c *checkReport) paint(code, s string) string {
	if !c.color {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}

func (c *checkReport) line(label, text string) {
	fmt.Println(fit(fmt.Sprintf("  %-12s %s", label, text)))
}

func (c *checkReport) warn(label, text string) {
	fmt.Println(c.paint("33", fit(fmt.Sprintf("  %-12s %s", label, text))))
}

func (c *checkReport) block(label, text string) {
	fmt.Println(c.paint("31", fit(fmt.Sprintf("  %-12s %s", label, text))))
	c.blockers = append(c.blockers, text)
}

// checkHost reduces an ssh_endpoint ("root@10.0.0.2:22") to its host.
func checkHost(endpoint string) string {
	if i := strings.Index(endpoint, "@"); i >= 0 {
		endpoint = endpoint[i+1:]
	}
	if h, _, err := net.SplitHostPort(endpoint); err == nil {
		return h
	}
	return endpoint
}

func nodeHasGPU(n clusterNode) bool {
	if g := n.Capabilities["gpu"]; g != "" && g != "none" {
		return true
	}
	return n.Load.GPUModel != "" || len(n.Load.GPUs) > 0
}

func nodeLabel(n clusterNode) string {
	if h := n.Capabilities["hostname"]; h != "" {
		return h
	}
	return n.NodeID[:min(8, len(n.NodeID))]
}

// nixAvailable reports why no environment can be built here, or nil.
func nixAvailable() error {
	if nixstore.Private() {
		_, err := nixstore.NixBinDir()
		return err
	}
	_, err := nixstore.SystemNix()
	return err
}

// runCheckOnly is `run --check-only`: everything a run would decide, built
// for real where it can be, with nothing executed. It returns an error only
// on a finding that would stop the run.
func runCheckOnly(o checkOnlyOptions) error {
	fi, statErr := os.Stdout.Stat()
	c := &checkReport{color: statErr == nil && fi.Mode()&os.ModeCharDevice != 0}

	absScript, err := filepath.Abs(o.ScriptPath)
	if err != nil {
		return fmt.Errorf("script path %s: %w", o.ScriptPath, err)
	}
	src, err := os.ReadFile(absScript)
	if err != nil {
		return fmt.Errorf("cannot read script: %w", err)
	}
	fmt.Println(c.paint("1", fit(fmt.Sprintf("CHECK %s — nothing will run", filepath.Base(absScript)))))

	// Imports → nix packages.
	scan := pythondeps.ExtractImportScan(absScript)
	if len(scan.ExternalDeps) == 0 {
		c.line("imports", "standard library only")
	} else {
		c.line("imports", strings.Join(scan.ExternalDeps, ", "))
	}
	// The run's own resolution, requirements.txt beside the script included.
	pkgs, unknown := app.PlanPackages(absScript, scan.ExternalDeps, o.Pkgs, false)
	// Ask nixpkgs for names the table lacks, as the run does (PKG-AUTO).
	var probe func([]string) (map[string]bool, error)
	nixErr := nixAvailable()
	if nixErr == nil {
		probe = app.ProbePythonPackages
	}
	pkgs, unknown, probeErr := resolveUnknown(pkgs, unknown, probe)
	if probeErr != nil {
		fmt.Fprintf(os.Stderr, "check-only: could not look up %s in nixpkgs: %v\n", strings.Join(unknown, ", "), probeErr)
	}
	if len(pkgs) > 0 {
		c.line("packages", strings.Join(pkgs, ", "))
	}
	if len(scan.LocalFiles) > 0 {
		c.line("local", fmt.Sprintf("%d file(s) bundled with the script", len(scan.LocalFiles)))
	}
	for _, name := range unknown {
		if probe != nil && probeErr == nil {
			c.warn("not packaged", name+" (not in nixpkgs; the job will fail to import it — add --pkg <nixpkgs name>)")
		} else {
			c.warn("not packaged", name+" (looked up in nixpkgs at run time; else add --pkg)")
		}
	}

	// What the shim (or DDP) can spread.
	hits := scanSpreads(string(src))
	for _, h := range hits {
		c.line("spreads", fmt.Sprintf("%s (line %d)", h.Label, h.Line))
	}
	if len(hits) == 0 {
		c.line("spreads", "no parallel call found — runs on one node")
	} else {
		c.line("", "split decided at run time by measured cost")
	}

	c.line("memory", fmt.Sprintf("~%s (tier=%s)", resourceest.FormatBytes(o.EstMemBytes), o.EstTier))

	nodes, nodesErr := fetchClusterNodes(o.DaemonPort)
	if nodesErr != nil {
		fmt.Fprintf(os.Stderr, "check-only: cannot list cluster nodes from the daemon on port %d: %v\n", o.DaemonPort, nodesErr)
	}
	names := map[string]string{}
	var gpuNodes []string
	for _, n := range nodes {
		names[n.NodeID] = nodeLabel(n)
		if n.State == "healthy" && nodeHasGPU(n) {
			gpuNodes = append(gpuNodes, nodeLabel(n))
		}
	}
	nameOf := func(id string) string {
		if id == o.NodeID.NodeID {
			return "this machine"
		}
		if n := names[id]; n != "" {
			return n
		}
		return id[:min(8, len(id))]
	}

	// GPU.
	gpuList := "none"
	if len(gpuNodes) > 0 {
		gpuList = strings.Join(gpuNodes, ", ")
	}
	switch {
	case o.RequireGPU && len(gpuNodes) == 0:
		c.block("gpu", "required (--gpu force) but no healthy GPU node")
	case o.RequireGPU:
		c.line("gpu", "required · GPU nodes: "+gpuList)
	case o.PreferGPU:
		c.line("gpu", "preferred · GPU nodes: "+gpuList)
	default:
		c.line("gpu", "not used")
	}

	// DDP: the same gate run uses (main.go), minus the ring measurement.
	if o.DDPNodes > 1 || (o.DDPNodes == 0 && pythondeps.HasDistributedImports(absScript)) {
		eligible := ddpEligibleCount(o.DaemonPort, o.RequireGPU, o.RequiredGPUMem, o.EstMemBytes)
		world := eligible
		if o.DDPNodes > 1 {
			world = min(o.DDPNodes, eligible)
		}
		if world >= 2 {
			c.line("ddp", fmt.Sprintf("up to %d ranks (%d eligible), ring measured at run", world, eligible))
		} else {
			c.warn("ddp", fmt.Sprintf("%d eligible rank(s) — runs on one node", eligible))
		}
	}

	// Placement. targetHost/targetPort are where the closure would go.
	var targetHost, targetName string
	var targetPort int
	// Run places a --gpu force job like --remote when this machine has no
	// GPU (main.go, gpuPlace); the check must name the same node.
	gpuPlace := o.RequireGPU && !o.Remote && o.DaemonHost == "" && gpu.Detect().Vendor == gpu.VendorNone
	if o.Remote || gpuPlace {
		coord := coordinator.New(coordinator.Config{
			RegistryURL:      o.RegistryURL,
			SelfIdentity:     o.NodeID,
			SelfSSH:          fmt.Sprintf("root@%s:22", detectLocalIP()),
			SelfDaemon:       o.DaemonPort,
			SelfLoad:         heartbeat.CollectLoad(0, 0),
			JobName:          o.JobName,
			RequiredMemBytes: o.EstMemBytes,
			RequiredCores:    o.RequiredCores,
			RequiredGPUMem:   o.RequiredGPUMem,
			NoSelf:           o.Remote,
			RequireGPU:       o.RequireGPU,
			PreferGPU:        o.PreferGPU,
			Strategy:         o.Strategy,
			RoundRobinFn:     checkRoundRobin(o.DaemonPort),
		})
		d := coord.FindNode()
		switch {
		case d.ChosenNode.NodeID == "" && gpuPlace:
			c.block("placement", "this machine has no GPU and no GPU node can take this run")
		case d.ChosenNode.NodeID == "":
			c.block("placement", "no eligible node other than this machine")
		default:
			targetHost = checkHost(d.ChosenNode.SSHEndpoint)
			targetPort = d.ChosenNode.DaemonPort
			targetName = nameOf(d.ChosenNode.NodeID)
			why := d.Reason
			if gpuPlace {
				why = "--gpu force, this machine has no GPU; " + why
			}
			c.line("placement", fmt.Sprintf("%s (%s)", targetName, why))
		}
	} else {
		host, port, targetID := "127.0.0.1", o.DaemonPort, o.NodeID.NodeID
		how := "this machine (no --remote)"
		if o.DaemonHost != "" {
			h, p, id, err := resolveExplicitTarget(o.DaemonPort, o.DaemonHost)
			if err != nil {
				c.block("placement", fmt.Sprintf("--host %s: %v", o.DaemonHost, err))
				targetID = ""
			} else {
				host, port, targetID = h, p, id
				how = fmt.Sprintf("%s (--host)", nameOf(id))
			}
		}
		if targetID != "" {
			c.line("placement", how)
			acc, err := daemonctl.CheckRemoteAcceptance(host, port, targetID, o.JobName, o.NodeID.NodeID, o.EstMemBytes)
			if err != nil {
				c.block("lease", fmt.Sprintf("refused: %v", err))
			} else {
				fmt.Println(fit(fmt.Sprintf("  %-12s remote daemon accepted job (lease=%s), cancelled",
					"lease", acc.LeaseID[:min(8, len(acc.LeaseID))])))
				// Same release path a real run takes, so the node's admission
				// sees the lease end the way it always does.
				if err := daemonctl.CommitLease(host, port, acc.LeaseID); err != nil {
					fmt.Fprintf(os.Stderr, "check-only: commit of lease %s failed: %v\n", acc.LeaseID, err)
				}
				if err := daemonctl.CompleteLease(host, port, acc.LeaseID, "cancelled"); err != nil {
					fmt.Fprintf(os.Stderr, "check-only: could not cancel lease %s (it expires on its own): %v\n", acc.LeaseID, err)
				}
				targetHost, targetPort, targetName = host, port, nameOf(targetID)
			}
		}
	}

	// Environment: built for real when nix is here, so a broken flake or a
	// missing package shows now rather than mid-demo.
	var storePath string
	if nixErr != nil {
		c.warn("environment", "skipped (no nix on this machine)")
		fmt.Fprintf(os.Stderr, "check-only: environment build skipped: %v\n", nixErr)
	} else {
		c.line("environment", "building (first build of a new env takes minutes)")
		start := time.Now()
		env, err := buildEnvQuiet(absScript, app.EnvOptions{
			PythonVersion: o.PythonVersion,
			Pkgs:          o.Pkgs,
			Intercept:     true,
			GPU:           o.RequireGPU || o.PreferGPU,
		})
		if err != nil {
			c.block("environment", fmt.Sprintf("build failed: %v", err))
		} else {
			storePath = env.StorePath
			env.Close()
			c.line("environment", fmt.Sprintf("built in %.1f s", time.Since(start).Seconds()))
			c.line("store", storePath)
		}
	}

	// Closure on the target.
	if storePath != "" && targetHost != "" {
		state, missing, total := daemonctl.ClosureStatus(targetHost, targetPort, storePath)
		switch state {
		case "on-node":
			c.line("closure", fmt.Sprintf("already on %s (%d paths)", targetName, total))
		case "partial":
			c.line("closure", fmt.Sprintf("%s lacks %d of %d paths; first run sends them", targetName, missing, total))
		case "none":
			c.line("closure", fmt.Sprintf("not on %s; first run sends all of it", targetName))
		default:
			c.warn("closure", fmt.Sprintf("could not ask %s", targetName))
		}
	}

	if len(c.blockers) > 0 {
		return fmt.Errorf("NOT READY: %s", strings.Join(c.blockers, "; "))
	}
	fmt.Println(c.paint("32", "checks complete"))
	return nil
}

// resolveUnknown moves the unknown names nixpkgs has (per probe) into pkgs,
// the way a run does. A nil probe (no nix here) leaves unknown as is.
func resolveUnknown(pkgs, unknown []string, probe func([]string) (map[string]bool, error)) ([]string, []string, error) {
	if probe == nil || len(unknown) == 0 {
		return pkgs, unknown, nil
	}
	found, missing, err := pythondeps.SplitByExistence(unknown, probe)
	for _, f := range found {
		if !slices.Contains(pkgs, f) {
			pkgs = append(pkgs, f)
		}
	}
	return pkgs, missing, err
}

// buildEnvQuiet builds the environment with BuildEnvironment's step lines
// sent to /dev/null; nix's own progress (stderr) stays visible.
// ponytail: swaps the process-wide os.Stdout, safe only because check-only
// is single-threaded; a writer parameter on BuildEnvironment would remove it.
func buildEnvQuiet(absScript string, opts app.EnvOptions) (*app.Environment, error) {
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-only: cannot open %s, showing build steps: %v\n", os.DevNull, err)
		return app.BuildEnvironment(absScript, opts, nil)
	}
	defer devnull.Close()
	saved := os.Stdout
	os.Stdout = devnull
	defer func() { os.Stdout = saved }()
	return app.BuildEnvironment(absScript, opts, nil)
}

// checkRoundRobin asks the local daemon for the next round-robin index, as
// run --remote does.
func checkRoundRobin(daemonPort int) func(int) int {
	return func(count int) int {
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/roundrobin?count=%d", daemonPort, count))
		if err != nil {
			fmt.Fprintf(os.Stderr, "check-only: round-robin index unavailable, using 0: %v\n", err)
			return 0
		}
		defer resp.Body.Close()
		var result struct{ Index int }
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "check-only: bad round-robin reply, using 0: %v\n", err)
		}
		return result.Index
	}
}
