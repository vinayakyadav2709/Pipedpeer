package pythondeps

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

type ImportScan struct {
	ExternalDeps []string
	LocalFiles   []string
}

var nixpkgsMapping = map[string]string{
	"numpy":          "numpy",
	"pandas":         "pandas",
	"requests":       "requests",
	"flask":          "flask",
	"django":         "django",
	"scipy":          "scipy",
	"sklearn":        "scikit-learn",
	"scikit-learn":   "scikit-learn",
	"matplotlib":     "matplotlib",
	"pillow":         "Pillow",
	"pyyaml":         "pyyaml",
	"yaml":           "pyyaml",
	"pytest":         "pytest",
	"torch":          "torch",
	"tensorflow":     "tensorflow",
	"keras":          "keras",
	"joblib":         "joblib",
	"opencv":         "opencv-python",
	"beautifulsoup4": "beautifulsoup4",
	"lxml":           "lxml",
	"PIL":            "Pillow",
	"cryptography":   "cryptography",
	"jwt":            "pyjwt",
	"sqlalchemy":     "sqlalchemy",
	"psycopg2":       "psycopg2",
	"redis":          "redis",
	"pymongo":        "pymongo",
	"boto3":          "boto3",
	"aiohttp":        "aiohttp",
	"httpx":          "httpx",
	"celery":         "celery",
	"fastapi":        "fastapi",
	"uvicorn":        "uvicorn",
	"pydantic":       "pydantic",
	"click":          "click",
}

// Python standard library modules — these are always available and should
// never be treated as external dependencies.
var stdlibModules = map[string]bool{
	"abc": true, "argparse": true, "ast": true, "asyncio": true,
	"base64": true, "bisect": true, "builtins": true,
	"calendar": true, "cmath": true, "code": true, "codecs": true,
	"codeop": true, "collections": true, "compileall": true,
	"concurrent": true, "configparser": true, "contextlib": true,
	"copy": true, "copyreg": true, "csv": true, "ctypes": true,
	"dataclasses": true, "datetime": true, "decimal": true,
	"difflib": true, "dis": true, "email": true, "enum": true,
	"errno": true, "faulthandler": true, "fileinput": true,
	"fnmatch": true, "fractions": true, "ftplib": true,
	"functools": true, "gc": true, "getpass": true, "gettext": true,
	"glob": true, "gzip": true, "hashlib": true, "heapq": true,
	"hmac": true, "html": true, "http": true,
	"importlib": true, "inspect": true, "io": true, "ipaddress": true,
	"itertools": true, "json": true, "keyword": true,
	"linecache": true, "locale": true, "logging": true, "lzma": true,
	"math": true, "mimetypes": true, "multiprocessing": true,
	"numbers": true, "operator": true, "os": true,
	"pathlib": true, "pdb": true, "pickle": true, "platform": true,
	"pprint": true, "profile": true,
	"queue": true, "random": true, "re": true, "readline": true,
	"reprlib": true, "runpy": true,
	"sched": true, "secrets": true, "select": true, "shelve": true,
	"shlex": true, "shutil": true, "signal": true, "site": true,
	"smtplib": true, "socket": true, "sqlite3": true, "ssl": true,
	"stat": true, "statistics": true, "string": true, "struct": true,
	"subprocess": true, "sys": true, "sysconfig": true,
	"tarfile": true, "tempfile": true, "textwrap": true,
	"threading": true, "time": true, "timeit": true, "token": true,
	"tokenize": true, "tomllib": true, "trace": true, "traceback": true,
	"tracemalloc": true, "turtle": true, "types": true, "typing": true,
	"unicodedata": true, "unittest": true, "urllib": true, "uuid": true,
	"venv": true, "warnings": true, "wave": true, "weakref": true,
	"webbrowser": true, "xml": true, "xmlrpc": true,
	"zipfile": true, "zipimport": true, "zlib": true,
	"_thread": true, "__future__": true,
}

// GPU-accelerated libraries — scripts importing these likely need GPU access.
var gpuImports = map[string]bool{
	"torch":       true, // PyTorch
	"tensorflow":  true, // TensorFlow
	"tf":          true, // TensorFlow shorthand
	"cupy":        true, // CuPy
	"numba":       true, // Numba (may use CUDA)
	"pycuda":      true, // PyCUDA
	"cuda":        true, // CUDA Python
	"jax":         true, // JAX
	"jaxlib":      true, // JAX lib
	"paddle":      true, // PaddlePaddle
	"onnxruntime": true, // ONNX Runtime
	"tensorrt":    true, // TensorRT
	"keras":       true, // Keras (usually on top of TF)
}

// HasGPUImports checks if the given script imports any GPU-accelerated libraries.
func HasGPUImports(scriptPath string) bool {
	scan := ExtractImportScan(scriptPath)
	for _, imp := range scan.ExternalDeps {
		// Also check for submodules like numba.cuda
		base := strings.SplitN(imp, ".", 2)[0]
		if gpuImports[imp] || gpuImports[base] {
			return true
		}
	}
	return false
}

// HasDistributedImports reports whether the script looks like a distributed
// training script: it must touch torch.distributed or DistributedDataParallel.
// The token scanner only sees top-level modules, so this scans raw source text
// for the DDP markers.
func HasDistributedImports(scriptPath string) bool {
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		return false
	}
	src := string(data)
	return strings.Contains(src, "torch.distributed") ||
		strings.Contains(src, "DistributedDataParallel") ||
		strings.Contains(src, "torch.dist")
}

func ExtractImports(scriptPath string) []string {
	scan := ExtractImportScan(scriptPath)
	return scan.ExternalDeps
}

// importedModules returns every top-level module an import line brings in.
//
// It used to be one regular expression capturing the first name on the line,
// which silently drops everything after a comma: "import numpy, pandas" put
// numpy in the closure and left pandas out, and the job then died on the
// worker with ModuleNotFoundError - an error pointing at the user's script
// rather than at the dependency detection that caused it. Found by a benchmark
// whose fixture happened to use that form.
//
// "from x import a, b" is different and already handled: the dependency is x,
// and what is taken from it does not matter here.
func importedModules(line string) []string {
	// A comment can contain anything that looks like an import.
	if i := strings.Index(line, "#"); i >= 0 {
		line = strings.TrimSpace(line[:i])
	}
	switch {
	case strings.HasPrefix(line, "from "):
		rest := strings.TrimSpace(strings.TrimPrefix(line, "from "))
		if name := topLevelModule(rest); name != "" {
			return []string{name}
		}
	case strings.HasPrefix(line, "import "):
		rest := strings.TrimSpace(strings.TrimPrefix(line, "import "))
		var out []string
		for _, part := range strings.Split(rest, ",") {
			if name := topLevelModule(strings.TrimSpace(part)); name != "" {
				out = append(out, name)
			}
		}
		return out
	}
	return nil
}

// topLevelModule reduces "pkg.sub as alias" to "pkg". A relative import
// ("from . import x") has no top-level name and yields nothing.
func topLevelModule(s string) string {
	if s == "" || strings.HasPrefix(s, ".") {
		return ""
	}
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		s = s[:i] // drop "as alias" and anything after
	}
	if i := strings.Index(s, "."); i >= 0 {
		s = s[:i]
	}
	for _, r := range s {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return ""
		}
	}
	return s
}

func ExtractImportScan(scriptPath string) ImportScan {
	file, err := os.Open(scriptPath)
	if err != nil {
		return ImportScan{}
	}
	defer file.Close()

	scriptDir := filepath.Dir(scriptPath)

	var deps []string
	var localFiles []string
	seenDeps := make(map[string]bool)
	seenLocal := make(map[string]bool)
	reader := bufio.NewReader(file)

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}

		lineStr := strings.TrimSpace(string(line))
		for _, imp := range importedModules(lineStr) {
			localFile := filepath.Join(scriptDir, imp+".py")
			localPackage := filepath.Join(scriptDir, imp, "__init__.py")

			if _, err := os.Stat(localFile); err == nil {
				if !seenLocal[localFile] {
					seenLocal[localFile] = true
					localFiles = append(localFiles, localFile)
				}
				continue
			}

			if _, err := os.Stat(localPackage); err == nil {
				if !seenLocal[localPackage] {
					seenLocal[localPackage] = true
					localFiles = append(localFiles, localPackage)
				}
				continue
			}

			if stdlibModules[imp] {
				continue
			}

			if !seenDeps[imp] {
				seenDeps[imp] = true
				deps = append(deps, imp)
			}
		}
	}

	return ImportScan{ExternalDeps: deps, LocalFiles: localFiles}
}

// ParseUVLock parses a uv.lock file and extracts the names of all packages.
func ParseUVLock(lockPath string) ([]string, error) {
	content, err := os.ReadFile(lockPath)
	if err != nil {
		return nil, err
	}

	// uv.lock has an array of tables named "package"
	// [[package]]
	// name = "pandas"
	// version = "2.0.3"

	// We do a simple string-based parse to avoid complex TOML unmarshaling
	// of the entire schema, since we only need the package names.
	var pkgs []string
	lines := strings.Split(string(content), "\n")
	inPackage := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "[[package]]" {
			inPackage = true
			continue
		}
		if inPackage && strings.HasPrefix(line, "name =") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				name := strings.TrimSpace(parts[1])
				name = strings.Trim(name, `"'`)
				pkgs = append(pkgs, name)
			}
			inPackage = false // wait for next [[package]]
		} else if strings.HasPrefix(line, "[[") {
			inPackage = false
		}
	}

	return pkgs, nil
}

func ResolveNixPackage(pkg string) string {
	if nixpkg, ok := nixpkgsMapping[pkg]; ok {
		return nixpkg
	}
	return ""
}

func ResolvePackages(astImports []string, explicitPkgs []string) []string {
	seen := make(map[string]bool)
	var result []string

	add := func(pkg string) {
		if pkg == "" {
			return
		}
		if !seen[pkg] {
			seen[pkg] = true
			result = append(result, pkg)
		}
	}

	for _, imp := range astImports {
		add(ResolveNixPackage(imp))
	}

	for _, req := range explicitPkgs {
		if strings.HasSuffix(req, ".txt") {
			content, err := os.ReadFile(req)
			if err != nil {
				continue
			}
			lines := strings.Split(string(content), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				pkgName := line
				if idx := strings.IndexAny(line, "=>~<"); idx != -1 {
					pkgName = strings.TrimSpace(line[:idx])
				}
				if mapped := ResolveNixPackage(pkgName); mapped != "" {
					add(mapped)
				} else {
					add(pkgName)
				}
			}
		} else {
			if mapped := ResolveNixPackage(req); mapped != "" {
				add(mapped)
			} else {
				add(req)
			}
		}
	}

	return result
}
