package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

type SSHConfig struct {
	User string
	Host string
	Port int
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

func parseRemote(remote string) (*SSHConfig, error) {
	var user, host string
	var port int

	if strings.Contains(remote, "@") {
		parts := strings.SplitN(remote, "@", 2)
		user = parts[0]
		hostPort := parts[1]
		if strings.Contains(hostPort, ":") {
			hP := strings.SplitN(hostPort, ":", 2)
			host = hP[0]
			fmt.Sscanf(hP[1], "%d", &port)
		} else {
			host = hostPort
			port = 22
		}
	} else {
		user = "root"
		if strings.Contains(remote, ":") {
			hP := strings.SplitN(remote, ":", 2)
			host = hP[0]
			fmt.Sscanf(hP[1], "%d", &port)
		} else {
			host = remote
			port = 22
		}
	}

	return &SSHConfig{User: user, Host: host, Port: port}, nil
}

func extractImports(scriptPath string) []string {
	file, err := os.Open(scriptPath)
	if err != nil {
		return nil
	}
	defer file.Close()

	var imports []string
	seen := make(map[string]bool)
	reader := bufio.NewReader(file)

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}

		lineStr := strings.TrimSpace(string(line))
		re := regexp.MustCompile(`^\s*(?:import|from)\s+([a-zA-Z0-9_]+)`)
		matches := re.FindStringSubmatch(lineStr)
		if len(matches) >= 2 {
			imp := matches[1]
			if !seen[imp] {
				seen[imp] = true
				imports = append(imports, imp)
			}
		}
	}

	return imports
}

func resolveNixpkg(pkg string) string {
	if nixpkg, ok := nixpkgsMapping[pkg]; ok {
		return nixpkg
	}
	return ""
}

func main() {
	scriptPath := flag.String("script", "", "Path to Python script")
	remote := flag.String("remote", "", "Remote SSH destination (e.g., root@localhost:2221)")
	flag.Parse()

	if *scriptPath == "" || *remote == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s --script <script.py> --remote <user@host:port>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	sshCfg, err := parseRemote(*remote)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse remote: %v\n", err)
		os.Exit(1)
	}

	if _, err := os.Stat(*scriptPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "File not found: %s\n", *scriptPath)
		os.Exit(1)
	}

	absScriptPath, err := filepath.Abs(*scriptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get absolute path: %v\n", err)
		os.Exit(1)
	}

	scriptName := filepath.Base(absScriptPath)

	fmt.Printf("=== Pipedpeer CLI ===\n")
	fmt.Printf("Script: %s\n", absScriptPath)
	fmt.Printf("Remote: %s@%s:%d\n\n", sshCfg.User, sshCfg.Host, sshCfg.Port)

	fmt.Printf("[1/6] Detecting imports...\n")
	imports := extractImports(absScriptPath)
	if len(imports) > 0 {
		fmt.Printf("      Found imports: %s\n", strings.Join(imports, ", "))
	}

	var nixPkgs []string
	for _, pkg := range imports {
		if nixpkg := resolveNixpkg(pkg); nixpkg != "" {
			fmt.Printf("      %s -> %s\n", pkg, nixpkg)
			nixPkgs = append(nixPkgs, nixpkg)
		}
	}

	tmpDir, err := os.MkdirTemp("", "pipedpeer-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	if err := os.WriteFile(filepath.Join(tmpDir, scriptName), []byte{}, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create placeholder: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n[2/6] Copying script to temp dir...\n")
	input, err := os.ReadFile(absScriptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read script: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, scriptName), input, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write script: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Copied to: %s\n", tmpDir)

	fmt.Printf("\n[3/6] Generating flake.nix...\n")
	var flakeContent string
	if len(nixPkgs) == 0 {
		flakeContent = fmt.Sprintf(`{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux.default =
      let
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
      in
      pkgs.writeShellScriptBin "run" ''
        ${pkgs.python3}/bin/python3 ${./%s}
      '';
  };
}
`, scriptName)
	} else {
		var psPkgs []string
		for _, pkg := range nixPkgs {
			psPkgs = append(psPkgs, "ps."+pkg)
		}
		pkgsList := strings.Join(psPkgs, "\n          ")
		flakeContent = fmt.Sprintf(`{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux.default =
      let
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
        python = pkgs.python3.withPackages (ps: [
          %s
        ]);
      in
      pkgs.writeShellScriptBin "run" ''
        ${python}/bin/python3 ${./%s}
      '';
  };
}
`, pkgsList, scriptName)
	}

	flakePath := filepath.Join(tmpDir, "flake.nix")
	if err := os.WriteFile(flakePath, []byte(flakeContent), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write flake.nix: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Created: %s\n", flakePath)

	fmt.Printf("\n[4/6] Building locally...\n")
	cmd := exec.Command("nix", "build", ".#packages.x86_64-linux.default")
	cmd.Dir = tmpDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "nix build failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Built successfully\n")

	fmt.Printf("\n[5/6] Getting store path...\n")
	resultPath := filepath.Join(tmpDir, "result")
	storePath, err := os.Readlink(resultPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to readlink result: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Store path: %s\n", storePath)

	fmt.Printf("\n[6/6] Copying to remote...\n")
	sshDest := fmt.Sprintf("ssh://%s@%s:%d", sshCfg.User, sshCfg.Host, sshCfg.Port)
	cmd = exec.Command("nix", "copy", "--to", sshDest, storePath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "nix copy failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Copied successfully\n")

	fmt.Printf("\n[7/7] Executing on remote...\n")
	runPath := filepath.Join(storePath, "bin", "run")
	cmd = exec.Command("ssh", "-p", fmt.Sprintf("%d", sshCfg.Port), fmt.Sprintf("%s@%s", sshCfg.User, sshCfg.Host), runPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Execution failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n=== Done ===\n")
}
