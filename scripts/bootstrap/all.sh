#!/bin/bash

# Pipedpeer Bootstrap Script
# Installs current runtime dependencies: Nix, SSH, crun
# Validates setup with 'pipedpeer setup'

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="$SCRIPT_DIR/lib"
MODULES_DIR="$SCRIPT_DIR/modules"

# Source common utilities
source "$LIB_DIR/common.sh"

# Colors
BOLD='\033[1m'

# Banner
echo -e "${BOLD}=== Pipedpeer Bootstrap ===${NC}"
echo "Installing dependencies: Nix, SSH, crun"
echo ""

# Check sudo
require_sudo

# Detect environment
OS=$(detect_os)
log_info "Detected OS: $OS"

if [[ "$OS" == "unknown" ]]; then
    error_exit "Unable to detect OS" \
        "Run on Linux, macOS, or WSL2" \
        "os-detection"
fi

GPU=$(detect_gpu)
if [[ "$GPU" == "none" ]]; then
    log_info "No GPU detected. Node will be registered CPU-only."
else
    log_info "Detected GPU: $GPU"
    log_info "GPU runtime will be installed."
fi

echo ""

# Modules to install
MODULES=(
    "nix"
    "ssh"
    "crun"
)

# Add GPU module if a GPU was detected
if [[ "$GPU" != "none" ]]; then
    MODULES+=("gpu:$GPU")
fi

FAILED=0

# Run each module
for module in "${MODULES[@]}"; do
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Support "module:arg" syntax for passing arguments
    MODULE_NAME="${module%%:*}"
    MODULE_ARG="${module#*:}"
    if [[ "$MODULE_ARG" == "$MODULE_NAME" ]]; then
        MODULE_ARG=""
    fi
    
    MODULE_SCRIPT="$MODULES_DIR/${MODULE_NAME}.sh"
    
    if [[ ! -f "$MODULE_SCRIPT" ]]; then
        log_error "Module script not found: $MODULE_SCRIPT"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    if [[ -n "$MODULE_ARG" ]]; then
        if bash "$MODULE_SCRIPT" "$MODULE_ARG"; then
            log_success "[$MODULE_NAME] completed successfully"
        else
            EXIT_CODE=$?
            log_error "[$MODULE_NAME] failed with exit code $EXIT_CODE"
            FAILED=$((FAILED + 1))
        fi
    else
        if bash "$MODULE_SCRIPT"; then
            log_success "[$MODULE_NAME] completed successfully"
        else
            EXIT_CODE=$?
            log_error "[$MODULE_NAME] failed with exit code $EXIT_CODE"
            FAILED=$((FAILED + 1))
        fi
    fi
    
    echo ""
done

# Summary
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if [[ $FAILED -eq 0 ]]; then
    log_success "All modules installed successfully!"
    echo ""
    
    # Validate the install if pipedpeer is available
    if command_exists pipedpeer; then
        log_info "Running pipedpeer setup validation..."
        echo ""
        if pipedpeer setup -y --no-install; then
            log_success "System validation passed!"
        else
            log_warn "Some optional checks failed. See above for details."
        fi
    else
        log_info "pipedpeer CLI not yet built. Run: ./scripts/build.sh"
    fi
else
    log_error "Bootstrap failed: $FAILED module(s) did not complete successfully"
    log_error "Review errors above for details"
    exit 1
fi

echo ""
log_success "Bootstrap complete!"
log_info "Next steps:"
echo "  1. Build CLI: ./scripts/build.sh"
echo "  2. Run tests: ./scripts/test.sh"
echo "  3. Try it: ./bin/pipedpeer --help"
echo ""

exit 0
