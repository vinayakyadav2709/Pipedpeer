#!/bin/bash

# Common utilities for bootstrap scripts
# Provides logging, error handling, OS detection, and command verification

set -o pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[✗]${NC} $1" >&2
}

# Error exit with reason and fix suggestion
error_exit() {
    local reason="$1"
    local fix="$2"
    local step="${3:-unknown}"
    
    log_error "Bootstrap failed at step: $step"
    log_error "Reason: $reason"
    
    if [[ ! -z "$fix" ]]; then
        echo -e "${YELLOW}Possible fixes:${NC}"
        echo "$fix" | sed 's/^/  • /'
    fi
    
    exit 1
}

# Check if command exists
command_exists() {
    command -v "$1" &> /dev/null
}

# Detect OS
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    elif grep -qi microsoft /proc/version 2>/dev/null; then
        echo "wsl2"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "linux"
    else
        echo "unknown"
    fi
}

# Detect Linux distribution
detect_distro() {
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        echo "$ID"
    else
        echo "unknown"
    fi
}

# Detect GPU type (nvidia, amd, intel, none)
detect_gpu() {
    # Check for NVIDIA
    if command_exists nvidia-smi; then
        echo "nvidia"
        return
    fi
    
    # Check for AMD
    if command_exists rocm-smi; then
        echo "amd"
        return
    fi
    
    # Check for Intel Arc
    if lspci 2>/dev/null | grep -qi "intel.*arc"; then
        echo "intel"
        return
    fi
    
    echo "none"
}

# Verify sudo access
require_sudo() {
    if [[ $EUID -ne 0 ]]; then
        error_exit "This script must be run with sudo" "Run: sudo $0"
    fi
}

# Run command with error handling
run_command() {
    local description="$1"
    local cmd="$2"
    
    log_info "Running: $description"
    
    if ! eval "$cmd"; then
        local exit_code=$?
        error_exit "Command failed: $cmd (exit code: $exit_code)" "Check output above for details" "$description"
    fi
}

# Install package based on OS
install_package() {
    local os="$1"
    local package="$2"
    
    case "$os" in
        linux)
            local distro=$(detect_distro)
            case "$distro" in
                ubuntu|debian)
                    run_command "apt-get update" "apt-get update"
                    run_command "apt-get install -y $package" "apt-get install -y $package"
                    ;;
                fedora|rhel|centos)
                    run_command "dnf install -y $package" "dnf install -y $package"
                    ;;
                arch)
                    run_command "pacman -S --noconfirm $package" "pacman -S --noconfirm $package"
                    ;;
                *)
                    error_exit "Unsupported distro: $distro" "Install $package manually" "distro-detection"
                    ;;
            esac
            ;;
        macos)
            run_command "brew install $package" "brew install $package"
            ;;
        wsl2)
            local distro=$(detect_distro)
            case "$distro" in
                ubuntu|debian)
                    run_command "apt-get update" "apt-get update"
                    run_command "apt-get install -y $package" "apt-get install -y $package"
                    ;;
                *)
                    error_exit "Unsupported WSL2 distro: $distro" "Manual installation required" "wsl2-distro"
                    ;;
            esac
            ;;
    esac
}

# Export for use in module scripts
export -f log_info log_success log_warn log_error error_exit
export -f command_exists detect_os detect_distro detect_gpu require_sudo run_command install_package
