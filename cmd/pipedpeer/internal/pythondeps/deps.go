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
