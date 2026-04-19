package pythondeps

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
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
	"matplotlib":     "matplotlib",
	"pillow":         "Pillow",
	"pyyaml":         "pyyaml",
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

func ExtractImports(scriptPath string) []string {
	scan := ExtractImportScan(scriptPath)
	return scan.ExternalDeps
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
	re := regexp.MustCompile(`^\s*(?:import|from)\s+([a-zA-Z0-9_]+)`)

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}

		lineStr := strings.TrimSpace(string(line))
		matches := re.FindStringSubmatch(lineStr)
		if len(matches) >= 2 {
			imp := matches[1]

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
