package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type WorkerConfig struct {
	Name string
	Port int
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <script.py> [worker]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Workers: worker-1 (2221), worker-2 (2222), worker-3 (2223)\n")
		os.Exit(1)
	}

	scriptPath := os.Args[1]
	workerName := "worker-1"
	if len(os.Args) >= 3 {
		workerName = os.Args[2]
	}

	workers := map[string]WorkerConfig{
		"worker-1": {Name: "worker-1", Port: 2221},
		"worker-2": {Name: "worker-2", Port: 2222},
		"worker-3": {Name: "worker-3", Port: 2223},
	}

	worker, ok := workers[workerName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown worker: %s\n", workerName)
		fmt.Fprintf(os.Stderr, "Available: worker-1, worker-2, worker-3\n")
		os.Exit(1)
	}

	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "File not found: %s\n", scriptPath)
		os.Exit(1)
	}

	fmt.Printf("=== Pipedpeer CLI ===\n")
	fmt.Printf("Script: %s\n", scriptPath)
	fmt.Printf("Worker: %s (port %d)\n\n", worker.Name, worker.Port)

	absPath, err := filepath.Abs(scriptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get absolute path: %v\n", err)
		os.Exit(1)
	}

	dir := filepath.Dir(absPath)
	scriptName := filepath.Base(absPath)

	fmt.Printf("[1/4] Generating flake.nix...\n")
	flakeContent := fmt.Sprintf(`{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }: {
    packages.x86_64-linux.default =
      let
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
      in
      pkgs.writeShellScriptBin "run" ''
        ${pkgs.python3}/bin/python3 ${./%s}
      '';

    apps.x86_64-linux.default = {
      type = "app";
      program = self.packages.x86_64-linux.default;
    };
  };
}
`, scriptName)

	flakePath := filepath.Join(dir, "flake.nix")
	if err := os.WriteFile(flakePath, []byte(flakeContent), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write flake.nix: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Created: %s\n", flakePath)

	fmt.Printf("\n[2/4] Copying to %s...\n", worker.Name)
	cmd := exec.Command("nix", "copy", "--to", fmt.Sprintf("ssh://root@localhost:%d", worker.Port), ".#default")
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "nix copy failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n[3/4] Finding profile...\n")
	cmd = exec.Command("ssh", "-p", fmt.Sprintf("%d", worker.Port), "root@localhost",
		"ls", "-la", "/nix/var/nix/profiles/")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()

	fmt.Printf("\n[4/4] Executing on %s...\n", worker.Name)
	cmd = exec.Command("ssh", "-p", fmt.Sprintf("%d", worker.Port), "root@localhost",
		"/nix/var/nix/profiles/default/bin/run")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Execution failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n=== Done ===\n")
}
