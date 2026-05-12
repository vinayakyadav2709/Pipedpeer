#!/bin/bash

# Nix installation module
# Installs Nix with flakes support for reproducible builds

set -e

source "$(dirname "$0")/../lib/common.sh"

MODULE_NAME="Nix"
OS=$(detect_os)

log_info "[$MODULE_NAME] Starting installation..."

# Check if already installed
if command_exists nix; then
    NIX_VERSION=$(nix --version)
    log_info "[$MODULE_NAME] Nix already installed: $NIX_VERSION"
    
    # Check if flakes are enabled
    if nix flake --version &>/dev/null; then
        log_success "Nix flakes verified"
        exit 0
    else
        log_warn "Nix is installed but flakes not available"
        log_warn "Enable flakes in ~/.config/nix/nix.conf or /etc/nix/nix.conf"
        exit 0
    fi
fi

# Install Nix
log_info "[$MODULE_NAME] Installing Nix..."

case "$OS" in
    linux|wsl2)
        # Use official installer
        # Check if curl is available
        if ! command_exists curl; then
            error_exit "curl not found (required to download Nix installer)" \
                "Install curl and retry" \
                "nix-curl-check"
        fi
        
        log_info "[$MODULE_NAME] Downloading Nix installer..."
        run_command "Download Nix installer" \
            "curl -L https://nixos.org/nix/install -o /tmp/nix-install.sh"
        
        run_command "Install Nix" "bash /tmp/nix-install.sh --daemon"
        
        log_info "[$MODULE_NAME] Nix installed. Reloading shell environment..."
        # Source nix environment
        if [[ -e /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh ]]; then
            source /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh
        fi
        ;;
    
    macos)
        # Use official installer
        if ! command_exists curl; then
            error_exit "curl not found (required to download Nix installer)" \
                "Install curl and retry" \
                "nix-curl-check-macos"
        fi
        
        log_info "[$MODULE_NAME] Downloading Nix installer for macOS..."
        run_command "Download Nix installer" \
            "curl -L https://nixos.org/nix/install -o /tmp/nix-install.sh"
        
        run_command "Install Nix" "bash /tmp/nix-install.sh"
        
        log_info "[$MODULE_NAME] Nix installed. Reloading shell environment..."
        if [[ -e /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh ]]; then
            source /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh
        fi
        ;;
    
    *)
        error_exit "Unsupported OS: $OS" \
            "Install Nix manually from: https://nixos.org/download.html" \
            "nix-os-detection"
        ;;
esac

# Verify installation
if ! command_exists nix; then
    error_exit "Nix installation failed but no errors detected" \
        "Check NIX_PROFILES environment or restart shell" \
        "nix-verification"
fi

log_success "Nix installed: $(nix --version)"

# Enable flakes in nix config
log_info "[$MODULE_NAME] Enabling experimental features (flakes, nix-command)..."

NIX_CONF_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/nix"
mkdir -p "$NIX_CONF_DIR"
NIX_CONF="$NIX_CONF_DIR/nix.conf"

if [[ ! -f "$NIX_CONF" ]]; then
    cat > "$NIX_CONF" <<EOF
experimental-features = nix-command flakes
EOF
    log_success "Created $NIX_CONF with flakes enabled"
else
    # Check if flakes already enabled
    if grep -q "experimental-features" "$NIX_CONF"; then
        if grep -q "flakes" "$NIX_CONF"; then
            log_info "[$MODULE_NAME] Flakes already enabled in nix.conf"
        else
            log_warn "nix.conf exists but flakes not enabled. Please add:"
            log_warn "  experimental-features = nix-command flakes"
        fi
    else
        echo "experimental-features = nix-command flakes" >> "$NIX_CONF"
        log_success "Added flakes to $NIX_CONF"
    fi
fi

# Verify flakes
if nix flake --version &>/dev/null; then
    log_success "Nix flakes verified"
else
    log_warn "Nix installed but flakes test failed"
    log_warn "You may need to restart your shell and try: nix flake --version"
fi

exit 0
