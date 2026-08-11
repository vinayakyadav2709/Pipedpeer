#!/bin/bash

# NVIDIA GPU runtime installation module
# Installs nvidia-container-toolkit for GPU-accelerated OCI sandboxes
# Requires: GPU detected (called from all.sh when detect_gpu returns "nvidia")

set -e

source "$(dirname "$0")/../lib/common.sh"

MODULE_NAME="NVIDIA-GPU"
OS=$(detect_os)

log_info "[$MODULE_NAME] Starting NVIDIA Container Toolkit installation..."

# Check if already installed
if command_exists nvidia-smi && command_exists nvidia-ctk; then
    GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1)
    log_success "NVIDIA GPU already configured: $GPU_NAME"
    log_success "nvidia-ctk available: $(nvidia-ctk --version 2>&1 | head -1)"
    exit 0
fi

if ! command_exists nvidia-smi; then
    error_exit "NVIDIA driver not detected (nvidia-smi missing)" \
        "Install NVIDIA drivers first: https://www.nvidia.com/download/index.aspx" \
        "nvidia-driver"
fi

# Install nvidia-container-toolkit based on OS
case "$OS" in
    linux|wsl2)
        DISTRO=$(detect_distro)
        case "$DISTRO" in
            ubuntu|debian)
                log_info "[$MODULE_NAME] Installing on Debian/Ubuntu via NVIDIA repo..."
                run_command "add NVIDIA APT repo" \
                    "curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' > /etc/apt/sources.list.d/nvidia-container-toolkit.list && apt-get update"
                run_command "apt-get install -y nvidia-container-toolkit" "apt-get install -y nvidia-container-toolkit"
                ;;
            fedora|rhel|centos)
                log_info "[$MODULE_NAME] Installing on RHEL/CentOS via NVIDIA repo..."
                run_command "add NVIDIA YUM repo" \
                    "curl -s -L https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo > /etc/yum.repos.d/nvidia-container-toolkit.repo"
                run_command "dnf install -y nvidia-container-toolkit" "dnf install -y nvidia-container-toolkit"
                ;;
            arch)
                log_info "[$MODULE_NAME] Installing on Arch..."
                run_command "pacman -S --noconfirm nvidia-container-toolkit" "pacman -S --noconfirm nvidia-container-toolkit"
                ;;
            *)
                log_warn "Unsupported distro: $DISTRO. Trying Nix..."
                if command_exists nix-env; then
                    run_command "nix-env -iA nixpkgs.nvidia-container-toolkit" "nix-env -iA nixpkgs.nvidia-container-toolkit"
                else
                    error_exit "Unsupported distro: $DISTRO" \
                        "Install nvidia-container-toolkit manually: https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html" \
                        "nvidia-distro-detection"
                fi
                ;;
        esac
        ;;

    macos)
        log_warn "NVIDIA GPUs are not supported on macOS. Skipping."
        exit 0
        ;;

    *)
        error_exit "Unsupported OS: $OS" \
            "Install nvidia-container-toolkit manually: https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html" \
            "nvidia-os-detection"
        ;;
esac

# Verify installation
if command_exists nvidia-ctk; then
    log_success "nvidia-container-toolkit installed: $(nvidia-ctk --version 2>&1 | head -1)"
else
    error_exit "nvidia-container-toolkit installation failed" \
        "Check NVIDIA documentation" \
        "nvidia-verification"
fi

# Generate CDI specification for crun
log_info "[$MODULE_NAME] Generating CDI specification for crun..."
run_command "nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml" \
    "nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml"

if [[ -f /etc/cdi/nvidia.yaml ]]; then
    log_success "CDI specification generated at /etc/cdi/nvidia.yaml"
else
    log_warn "CDI generation failed — crun will use device node mounts instead"
fi

# Test GPU access via crun
if command_exists crun; then
    log_info "[$MODULE_NAME] Testing GPU access via crun OCI sandbox..."
    TEST_DIR=$(mktemp -d)
    mkdir -p "$TEST_DIR/rootfs/dev" "$TEST_DIR/rootfs/proc" "$TEST_DIR/rootfs/sys"
    cat > "$TEST_DIR/config.json" << 'CONFIG'
{
  "ociVersion": "1.0.2",
  "process": {
    "terminal": false,
    "user": {"uid": 0, "gid": 0},
    "args": ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
    "env": [
      "PATH=/usr/local/bin:/usr/bin:/bin",
      "NVIDIA_VISIBLE_DEVICES=all",
      "NVIDIA_DRIVER_CAPABILITIES=compute,utility"
    ],
    "cwd": "/"
  },
  "root": {"path": "rootfs", "readonly": true},
  "hostname": "gpu-test",
  "mounts": [
    {"destination": "/usr", "type": "bind", "source": "/usr", "options": ["rbind", "ro"]},
    {"destination": "/bin", "type": "bind", "source": "/bin", "options": ["rbind", "ro"]},
    {"destination": "/lib", "type": "bind", "source": "/lib", "options": ["rbind", "ro"]},
    {"destination": "/lib64", "type": "bind", "source": "/lib64", "options": ["rbind", "ro"]},
    {"destination": "/proc", "type": "proc", "source": "proc"},
    {"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
    {"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"}
  ],
  "linux": {
    "namespaces": [
      {"type": "pid"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"}
    ],
    "devices": [
      {"path": "/dev/nvidia0", "type": "c", "major": 195, "minor": 0, "fileMode": 438, "permissions": "rwm"},
      {"path": "/dev/nvidiactl", "type": "c", "major": 195, "minor": 255, "fileMode": 438, "permissions": "rwm"},
      {"path": "/dev/nvidia-modeset", "type": "c", "major": 195, "minor": 254, "fileMode": 438, "permissions": "rwm"},
      {"path": "/dev/nvidia-uvm", "type": "c", "major": 234, "minor": 0, "fileMode": 438, "permissions": "rwm"},
      {"path": "/dev/nvidia-uvm-tools", "type": "c", "major": 234, "minor": 1, "fileMode": 438, "permissions": "rwm"}
    ]
  }
}
CONFIG

    GPU_NAME=$(crun run --bundle "$TEST_DIR" "gpu-verify" 2>/dev/null || echo "")
    crun delete -f "gpu-verify" 2>/dev/null || true
    rm -rf "$TEST_DIR"

    if [[ -n "$GPU_NAME" ]]; then
        log_success "GPU verified inside crun sandbox: $GPU_NAME"
    else
        log_warn "GPU verification failed — check driver and nvidia-container-toolkit"
    fi
fi

log_success "[$MODULE_NAME] NVIDIA GPU runtime setup complete!"
exit 0
