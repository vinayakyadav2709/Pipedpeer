package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type SSHConfig struct {
	User string
	Host string
	Port int
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

	fmt.Printf("=== Pipedpeer CLI ===\n")
	fmt.Printf("Script: %s\n", *scriptPath)
	fmt.Printf("Remote: %s@%s:%d\n\n", sshCfg.User, sshCfg.Host, sshCfg.Port)

	absPath, err := filepath.Abs(*scriptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get absolute path: %v\n", err)
		os.Exit(1)
	}

	dir := filepath.Dir(absPath)
	scriptName := filepath.Base(absPath)

	fmt.Printf("[1/5] Generating flake.nix...\n")
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
  };
}
`, scriptName)

	flakePath := filepath.Join(dir, "flake.nix")
	if err := os.WriteFile(flakePath, []byte(flakeContent), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write flake.nix: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Created: %s\n", flakePath)

	fmt.Printf("\n[2/5] Building locally...\n")
	cmd := exec.Command("nix", "build", ".#packages.x86_64-linux.default")
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "nix build failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Built successfully\n")

	fmt.Printf("\n[3/5] Getting store path...\n")
	resultPath := filepath.Join(dir, "result")
	storePath, err := os.Readlink(resultPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to readlink result: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Store path: %s\n", storePath)

	fmt.Printf("\n[4/5] Copying to remote...\n")
	sshDest := fmt.Sprintf("ssh://%s@%s:%d", sshCfg.User, sshCfg.Host, sshCfg.Port)
	cmd = exec.Command("nix", "copy", "--to", sshDest, storePath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "nix copy failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      Copied successfully\n")

	fmt.Printf("\n[5/5] Executing on remote...\n")
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
