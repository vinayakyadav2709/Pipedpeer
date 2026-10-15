package nixgen

import (
	"embed"
	"io/fs"
	"os"
	"strings"
)

// ShimFS holds the job shim: shim/sitecustomize.py, which Python imports at
// startup when its dir is on PYTHONPATH, and the shim/pipedpeer_shim package
// it imports. It patches the parallel primitives (multiprocessing.Pool,
// ProcessPoolExecutor, joblib, pandas, numpy, torch DDP) to route through
// the local pipedpeer daemon, with no change to the user's script. Only the
// runtime files are embedded; shim/tests and shim/ruff.toml stay out.
//
//go:embed shim/sitecustomize.py shim/pipedpeer_shim/*.py
var ShimFS embed.FS

// ShimDir is ShimFS rooted at the shim dir: sitecustomize.py at the top,
// pipedpeer_shim/ beside it, the layout PYTHONPATH expects.
func ShimDir() fs.FS {
	sub, err := fs.Sub(ShimFS, "shim")
	if err != nil {
		panic(err) // "shim" is a valid, embedded path: cannot happen
	}
	return sub
}

// WriteShimDir writes the shim into dir (sitecustomize.py plus the
// pipedpeer_shim package), ready to be put on PYTHONPATH. dir may exist but
// must not already hold the shim's files.
func WriteShimDir(dir string) error {
	return os.CopyFS(dir, ShimDir())
}

// ShimSource is every shim file concatenated, in path order, for tests that
// search the shim's code as text.
func ShimSource() string {
	var b strings.Builder
	err := fs.WalkDir(ShimFS, "shim", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		src, err := ShimFS.ReadFile(path)
		b.Write(src)
		return err
	})
	if err != nil {
		panic(err) // reading an embed.FS cannot fail
	}
	return b.String()
}
