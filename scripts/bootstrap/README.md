# Pipedpeer Bootstrap

Automated dependency installation and setup for Pipedpeer.

## What Gets Installed

- **Nix** (with flakes support) — Reproducible builds and dependency management
- **SSH** — Remote execution (generates ed25519 key if missing)
- **Bubblewrap** — Namespace-based job isolation

Not installed in this phase:
- Docker runtime bootstrap
- GPU runtime bootstrap

## Quick Start

```bash
sudo ./scripts/bootstrap/all.sh
```

This script:
1. Detects your OS (Linux, macOS, WSL2)
2. Installs each dependency
3. Configures Nix with flakes enabled
4. Generates SSH key (if missing)
5. Runs `pipedpeer doctor` to validate setup
6. Detects GPU and logs CPU-only registration (GPU setup is skipped)

## Step-by-Step

### Linux (Debian/Ubuntu)
```bash
# Prerequisites for bootstrap itself
sudo apt-get update
sudo apt-get install -y curl

# Run bootstrap
sudo ./scripts/bootstrap/all.sh
```

### macOS
```bash
# Ensure Homebrew is installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Run bootstrap
sudo ./scripts/bootstrap/all.sh
```

### WSL2 (Ubuntu)
```bash
# Run bootstrap in WSL2 terminal
sudo ./scripts/bootstrap/all.sh
```

## Individual Modules

Run individual setup scripts if needed:

```bash
# Just install Nix
sudo ./scripts/bootstrap/modules/nix.sh

# Just set up SSH
sudo ./scripts/bootstrap/modules/ssh.sh

# Just install Bubblewrap
sudo ./scripts/bootstrap/modules/bwrap.sh
```

## GPU Support

GPU setup is currently **not implemented in bootstrap**. Nodes are treated as CPU-only.

To enable GPU job execution later:
- NVIDIA: Install CUDA drivers
- AMD: Install ROCm drivers
- Intel Arc: Install oneAPI/DPC++ drivers

After runtime support lands, re-run bootstrap to register GPU capability.

## Troubleshooting

### Module failed with reasons
Bootstrap shows:
```
❌ [module] failed
Reason: <specific error>
Possible fixes:
  • Fix 1
  • Fix 2
```

Follow the suggested fixes and re-run.

### Nix flakes not working
If `nix flake --version` fails after installation:
```bash
# Restart your shell
bash
nix flake --version

# OR manually enable flakes in ~/.config/nix/nix.conf:
experimental-features = nix-command flakes
```

### SSH key not generated
Check permissions:
```bash
ls -la ~/.ssh/
# Should show: drwx------
```

If missing, generate manually:
```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ''
```

## After Bootstrap

```bash
# Build Pipedpeer CLI
./scripts/build.sh

# Validate setup
pipedpeer doctor

# Run tests
./scripts/test.sh

# Try it out
./bin/pipedpeer --help
```

## Manual Installation

If bootstrap fails, install dependencies manually:

### Nix
https://nixos.org/download.html

### Bubblewrap
```bash
# Ubuntu/Debian
sudo apt-get install bubblewrap

# macOS
brew install bubblewrap

# Arch
sudo pacman -S bubblewrap
```

### SSH
```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ''
```

## Environment Variables

Bootstrap respects:
- `$NIX_PROFILES` — Nix environment (set automatically)
- `$HOME` — User home directory (for SSH keys)
- `$XDG_CONFIG_HOME` — Nix config location (defaults to ~/.config)

## Testing

Current project test flow:
```bash
./scripts/test.sh
./scripts/test-integration.sh
```

Integration tests continue to use the lab worker containers.
