#!/bin/bash

# Bubblewrap (bwrap) installation module
# Installs bubblewrap for namespace-based job isolation

set -e

source "$(dirname "$0")/../lib/common.sh"

MODULE_NAME="Bubblewrap"
OS=$(detect_os)

log_info "[$MODULE_NAME] Starting installation..."

# Check if already installed
if command_exists bwrap; then
    log_success "Bubblewrap already installed: $(bwrap --version 2>&1 | head -1)"
    exit 0
fi

# Install based on OS
case "$OS" in
    linux)
        DISTRO=$(detect_distro)
        case "$DISTRO" in
            ubuntu|debian)
                log_info "[$MODULE_NAME] Installing bubblewrap on Debian/Ubuntu..."
                run_command "apt-get update" "apt-get update"
                run_command "apt-get install -y bubblewrap" "apt-get install -y bubblewrap"
                ;;
            fedora|rhel|centos)
                log_info "[$MODULE_NAME] Installing bubblewrap on RHEL/CentOS..."
                run_command "dnf install -y bubblewrap" "dnf install -y bubblewrap"
                ;;
            arch)
                log_info "[$MODULE_NAME] Installing bubblewrap on Arch..."
                run_command "pacman -S --noconfirm bubblewrap" "pacman -S --noconfirm bubblewrap"
                ;;
            *)
                error_exit "Unsupported Linux distro: $DISTRO" \
                    "Install bubblewrap manually: https://github.com/projectatomic/bubblewrap" \
                    "bwrap-distro-detection"
                ;;
        esac
        ;;
    
    macos)
        log_info "[$MODULE_NAME] Installing bubblewrap on macOS..."
        if ! command_exists brew; then
            error_exit "Homebrew not found" \
                "Install Homebrew from: https://brew.sh/" \
                "bwrap-macos-brew"
        fi
        run_command "brew install bubblewrap" "brew install bubblewrap"
        ;;
    
    wsl2)
        log_info "[$MODULE_NAME] Installing bubblewrap on WSL2..."
        DISTRO=$(detect_distro)
        case "$DISTRO" in
            ubuntu|debian)
                run_command "apt-get update" "apt-get update"
                run_command "apt-get install -y bubblewrap" "apt-get install -y bubblewrap"
                ;;
            *)
                error_exit "Unsupported WSL2 distro: $DISTRO" \
                    "Ensure running Ubuntu or Debian in WSL2" \
                    "bwrap-wsl2-distro"
                ;;
        esac
        ;;
    
    *)
        error_exit "Unsupported OS: $OS" \
            "Install bubblewrap manually from: https://github.com/projectatomic/bubblewrap" \
            "bwrap-os-detection"
        ;;
esac

# Verify installation
if command_exists bwrap; then
    log_success "Bubblewrap installed: $(bwrap --version 2>&1 | head -1)"
else
    error_exit "Bubblewrap installation failed but no errors detected" \
        "Check bubblewrap is in \$PATH" \
        "bwrap-verification"
fi

exit 0
