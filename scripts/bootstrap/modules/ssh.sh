#!/bin/bash

# SSH module
# Generates SSH key if missing (required for remote job execution)

set -e

source "$(dirname "$0")/../lib/common.sh"

MODULE_NAME="SSH"
SSH_KEY_PATH="$HOME/.ssh/id_ed25519"
SSH_KEY_PUB="$SSH_KEY_PATH.pub"

log_info "[$MODULE_NAME] Starting setup..."

# Check if SSH keys already exist
if [[ -f "$SSH_KEY_PATH" ]]; then
    log_success "SSH key already exists: $SSH_KEY_PATH"
    
    # Verify it's an ed25519 key
    if ssh-keygen -l -f "$SSH_KEY_PATH" 2>/dev/null | grep -q "ED25519"; then
        log_success "SSH key is ED25519 (optimal)"
        exit 0
    else
        log_warn "SSH key exists but is not ED25519. Keep it or generate new:"
        log_warn "  ssh-keygen -t ed25519 -f $SSH_KEY_PATH -N ''"
        exit 0
    fi
fi

# Create .ssh directory if needed
SSH_DIR="$HOME/.ssh"
if [[ ! -d "$SSH_DIR" ]]; then
    log_info "[$MODULE_NAME] Creating $SSH_DIR..."
    mkdir -p "$SSH_DIR"
    chmod 700 "$SSH_DIR"
fi

# Generate new SSH key
log_info "[$MODULE_NAME] Generating ED25519 SSH key..."
log_info "[$MODULE_NAME] (No passphrase for automated job execution)"

run_command "Generate SSH key" \
    "ssh-keygen -t ed25519 -f $SSH_KEY_PATH -N '' -C 'pipedpeer@$(hostname)'"

if [[ ! -f "$SSH_KEY_PATH" ]]; then
    error_exit "SSH key generation failed" \
        "Check write permissions to $SSH_DIR" \
        "ssh-keygen"
fi

# Set correct permissions
chmod 600 "$SSH_KEY_PATH"
chmod 644 "$SSH_KEY_PUB"

log_success "SSH key generated: $SSH_KEY_PATH"
log_info "[$MODULE_NAME] Public key: $SSH_KEY_PUB"
log_info "[$MODULE_NAME] Add public key to remote nodes ~/.ssh/authorized_keys"

exit 0
