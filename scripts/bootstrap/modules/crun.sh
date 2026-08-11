#!/bin/bash

# crun (OCI runtime) installation module
# Installs crun for OCI-based job isolation

set -e

source "$(dirname "$0")/../lib/common.sh"

MODULE_NAME="crun"
OS=$(detect_os)

log_info "[$MODULE_NAME] Starting installation..."

# Check if already installed
if command_exists crun; then
    log_success "crun already installed: $(crun --version 2>&1 | head -1)"
    exit 0
fi

# Install based on OS
case "$OS" in
    linux|wsl2)
        DISTRO=$(detect_distro)
        case "$DISTRO" in
            ubuntu|debian)
                log_info "[$MODULE_NAME] Installing crun on Debian/Ubuntu..."
                run_command "apt-get update" "apt-get update"
                run_command "apt-get install -y crun" "apt-get install -y crun"
                ;;
            fedora|rhel|centos)
                log_info "[$MODULE_NAME] Installing crun on RHEL/CentOS..."
                run_command "dnf install -y crun" "dnf install -y crun"
                ;;
            arch)
                log_info "[$MODULE_NAME] Installing crun on Arch..."
                run_command "pacman -S --noconfirm crun" "pacman -S --noconfirm crun"
                ;;
            *)
                log_warn "No crun package for $DISTRO. Installing via Nix..."
                if command_exists nix-env; then
                    run_command "nix-env -iA nixpkgs.crun" "nix-env -iA nixpkgs.crun"
                else
                    error_exit "Unsupported distro: $DISTRO (Nix not found)" \
                        "Install crun manually: https://github.com/containers/crun" \
                        "crun-distro-detection"
                fi
                ;;
        esac
        ;;

    macos)
        log_info "[$MODULE_NAME] Installing crun on macOS..."
        if ! command_exists brew; then
            log_warn "Homebrew not found. Installing crun via Nix..."
            if command_exists nix-env; then
                run_command "nix-env -iA nixpkgs.crun" "nix-env -iA nixpkgs.crun"
            else
                error_exit "No package manager found" \
                    "Install Homebrew or Nix first, then run: brew install crun" \
                    "crun-macos"
            fi
        else
            run_command "brew install crun" "brew install crun"
        fi
        ;;

    *)
        log_warn "Unknown OS: $OS. Attempting Nix install..."
        if command_exists nix-env; then
            run_command "nix-env -iA nixpkgs.crun" "nix-env -iA nixpkgs.crun"
        else
            error_exit "Unsupported OS: $OS" \
                "Install crun manually from: https://github.com/containers/crun" \
                "crun-os-detection"
        fi
        ;;
esac

# Verify installation
if command_exists crun; then
    log_success "crun installed: $(crun --version 2>&1 | head -1)"
else
    error_exit "crun installation failed but no errors detected" \
        "Check crun is in \$PATH" \
        "crun-verification"
fi

exit 0
