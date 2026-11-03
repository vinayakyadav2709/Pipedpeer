package daemonctl

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/say"
)

// MaxWorkspaceBytes is the most a workspace may total, before compression,
// for a run to pack it without --allow-large-workspace. A project is source
// and small data; a workspace past this is nearly always a directory run from
// by mistake (a home directory packed to a 151 GB tar once filled a disk).
const MaxWorkspaceBytes int64 = 2 << 30

// WorkspaceEntry is one top-level entry of a workspace and its total size.
type WorkspaceEntry struct {
	Name string
	Size int64
}

// WorkspaceTooLargeError is returned by CheckWorkspaceSize; its text is the
// message shown to the user.
type WorkspaceTooLargeError struct {
	Dir   string
	Total int64
	Limit int64
	Top   []WorkspaceEntry
}

func (e *WorkspaceTooLargeError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "workspace %s is %s, over the %s limit for a run; it was not packed.\nBiggest entries:\n",
		e.Dir, humanBytes(e.Total), humanBytes(e.Limit))
	for _, t := range e.Top {
		fmt.Fprintf(&b, "  %s  %s\n", humanBytes(t.Size), t.Name)
	}
	fmt.Fprintf(&b, "Run from the project directory, or list what to leave out in %s (one name per line, e.g. %q), or re-run with --allow-large-workspace to send it all.",
		filepath.Join(e.Dir, ".pipedpeerignore"), topIgnoreExample(e.Top))
	return b.String()
}

func topIgnoreExample(top []WorkspaceEntry) string {
	if len(top) == 0 {
		return "data/"
	}
	return top[0].Name + "/"
}

// workspacePatterns is the exclude list CreateWorkspaceTar passes to tar:
// the built-in names, then the lines of .pipedpeerignore.
func workspacePatterns(projectDir string) (names []string, anchored []string) {
	names = []string{".git", "__pycache__", ".venv", "venv", "env", "node_modules"}
	content, err := os.ReadFile(filepath.Join(projectDir, ".pipedpeerignore"))
	if err != nil {
		return names, nil
	}
	for _, line := range strings.Split(string(content), "\n") {
		args := ignoreExcludes(strings.TrimSpace(line))
		for _, a := range args {
			switch {
			case strings.HasPrefix(a, "--exclude=./"):
				anchored = append(anchored, strings.TrimPrefix(a, "--exclude=./"))
			case strings.HasPrefix(a, "--exclude="):
				names = append(names, strings.TrimPrefix(a, "--exclude="))
			}
		}
	}
	return names, anchored
}

// excluded mirrors tar's --exclude matching closely enough for a size
// estimate: an unanchored pattern matches the path from any component on
// down, an anchored one only from the root.
func excluded(rel string, names, anchored []string) bool {
	comps := strings.Split(rel, "/")
	for _, p := range anchored {
		if ok, _ := path.Match(p, rel); ok {
			return true
		}
	}
	for i := range comps {
		sub := strings.Join(comps[i:], "/")
		for _, p := range names {
			if ok, _ := path.Match(p, sub); ok {
				return true
			}
		}
	}
	return false
}

// EstimateWorkspace totals the bytes tar would pack from projectDir, applying
// the same ignore rules, without reading file contents. top lists every
// top-level entry, biggest first.
func EstimateWorkspace(projectDir string) (int64, []WorkspaceEntry, error) {
	names, anchored := workspacePatterns(projectDir)
	sizes := map[string]int64{}
	var total int64
	err := filepath.WalkDir(projectDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// Tar would fail or skip this too; one unreadable entry is not
			// a reason to guess the whole workspace is fine or too big.
			say.Printf("      warning: size check cannot read %s: %v\n", p, err)
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(projectDir, p)
		if err != nil || rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if excluded(rel, names, anchored) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			say.Printf("      warning: size check cannot stat %s: %v\n", p, err)
			return nil
		}
		top, _, _ := strings.Cut(rel, "/")
		sizes[top] += info.Size()
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	top := make([]WorkspaceEntry, 0, len(sizes))
	for n, s := range sizes {
		top = append(top, WorkspaceEntry{n, s})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].Size != top[j].Size {
			return top[i].Size > top[j].Size
		}
		return top[i].Name < top[j].Name
	})
	return total, top, nil
}

// CheckWorkspaceSize returns a *WorkspaceTooLargeError when the workspace
// would pack to more than limit bytes.
func CheckWorkspaceSize(projectDir string, limit int64) error {
	total, top, err := EstimateWorkspace(projectDir)
	if err != nil {
		return fmt.Errorf("measure workspace: %w", err)
	}
	if total <= limit {
		return nil
	}
	if len(top) > 5 {
		top = top[:5]
	}
	return &WorkspaceTooLargeError{Dir: projectDir, Total: total, Limit: limit, Top: top}
}
