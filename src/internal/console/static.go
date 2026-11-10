package console

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// web holds the committed stub page and, when the frontend has been built,
// its output under web/dist.
//
//go:embed all:web
var web embed.FS

const (
	distDir   = "web/dist"
	indexFile = "index.html"
	stubFile  = "web/stub.html"
)

// staticHandler serves built assets, and the app shell for every other GET so
// client-side routes survive a reload.
func staticHandler() http.Handler {
	shell, err := fs.ReadFile(web, path.Join(distDir, indexFile))
	if err != nil {
		shell, _ = fs.ReadFile(web, stubFile)
	}
	dist, _ := fs.Sub(web, distDir)
	assets := http.FileServer(http.FS(dist))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name != "" && name != indexFile {
			if info, err := fs.Stat(dist, name); err == nil && !info.IsDir() {
				assets.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(shell)
	})
}
