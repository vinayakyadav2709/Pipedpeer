#!/bin/bash

# GPU runtime installation module
# Detects GPU vendor and installs the appropriate container runtime:
#   - NVIDIA: nvidia-container-toolkit + CDI spec
#   - AMD:    rocm-container-toolkit (if available) + /dev/kfd config
#   - Intel:  intel-compute-runtime + /dev/dri config

set -e

source "$(dirname "$0")/../lib/common.sh"

MODULE_NAME="GPU"
OS=$(detect_os)

# Detect GPU vendor if not passed as argument
# When called from all.sh without args, we auto-detect
if [[ -z "${1:-}" ]]; then
    GPU_VENDOR=$(detect_gpu)
else
    GPU_VENDOR="$1"
fi

log_info "[$MODULE_NAME] Setting up GPU runtime for: $GPU_VENDOR"

if [[ "$GPU_VENDOR" == "none" ]]; then
    log_info "[$MODULE_NAME] No GPU detected. Skipping."
    exit 0
fi

case "$GPU_VENDOR" in
    nvidia)
        log_info "[$MODULE_NAME] Configuring NVIDIA GPU..."

        if command_exists nvidia-smi && command_exists nvidia-ctk; then
            GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1)
            log_success "NVIDIA GPU detected: $GPU_NAME"
        else
            log_info "[$MODULE_NAME] Installing nvidia-container-toolkit..."
            case "$OS" in
                linux|wsl2)
                    DISTRO=$(detect_distro)
                    case "$DISTRO" in
                        ubuntu|debian)
                            run_command "add NVIDIA repo" \
                                "curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg 2>/dev/null && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' > /etc/apt/sources.list.d/nvidia-container-toolkit.list && apt-get update"
                            run_command "apt-get install -y nvidia-container-toolkit" "apt-get install -y nvidia-container-toolkit"
                            ;;
                        fedora|rhel|centos)
                            run_command "add NVIDIA YUM repo" \
                                "curl -s -L https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo > /etc/yum.repos.d/nvidia-container-toolkit.repo"
                            run_command "dnf install -y nvidia-container-toolkit" "dnf install -y nvidia-container-toolkit"
                            ;;
                        arch)
                            run_command "pacman -S --noconfirm nvidia-container-toolkit" "pacman -S --noconfirm nvidia-container-toolkit"
                            ;;
                        *)
                            if command_exists nix-env; then
                                run_command "nix-env -iA nixpkgs.nvidia-container-toolkit" "nix-env -iA nixpkgs.nvidia-container-toolkit"
                            else
                                log_warn "Manual install needed: https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/"
                            fi
                            ;;
                    esac
                    ;;
                macos)
                    log_warn "NVIDIA GPUs not supported on macOS. Skipping."
                    exit 0
                    ;;
            esac
        fi

        # Generate CDI spec
        if command_exists nvidia-ctk && [[ ! -f /etc/cdi/nvidia.yaml ]]; then
            log_info "[$MODULE_NAME] Generating CDI specification..."
            mkdir -p /etc/cdi
            run_command "nvidia-ctk cdi generate" "nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml"
        fi
        ;;

    amd)
        log_info "[$MODULE_NAME] Configuring AMD GPU..."

        if command_exists rocm-smi; then
            log_success "AMD ROCm detected"
        else
            log_info "[$MODULE_NAME] Installing ROCm..."
            case "$OS" in
                linux|wsl2)
                    DISTRO=$(detect_distro)
                    case "$DISTRO" in
                        ubuntu|debian)
                            log_info "Installing ROCm via amdgpu-install..."
                            run_command "apt-get update" "apt-get update"
                            run_command "amdgpu-install -y --usecase=rocm" "amdgpu-install -y --usecase=rocm 2>/dev/null || apt-get install -y rocm-dev"
                            ;;
                        *)
                            log_warn "AMD ROCm auto-install not available for $DISTRO"
                            log_warn "Install manually: https://rocm.docs.amd.com/"
                            ;;
                    esac
                    ;;
                *)
                    log_warn "AMD GPU support requires Linux. Skipping."
                    exit 0
                    ;;
            esac
        fi

        # Ensure /dev/kfd exists
        if [[ ! -e /dev/kfd ]]; then
            log_warn "/dev/kfd not found — ROCm kernel module may not be loaded"
            log_warn "Run: sudo modprobe amdgpu"
        fi
        ;;

    intel)
        log_info "[$MODULE_NAME] Configuring Intel GPU..."

        case "$OS" in
            linux|wsl2)
                DISTRO=$(detect_distro)
                case "$DISTRO" in
                    ubuntu|debian)
                        log_info "Installing Intel compute runtime..."
                        run_command "apt-get update" "apt-get update"
                        run_command "apt-get install -y intel-opencl-icd intel-level-zero-gpu level-zero" "apt-get install -y intel-opencl-icd intel-level-zero-gpu level-zero 2>/dev/null || true"
                        ;;
                    fedora)
                        run_command "dnf install -y intel-opencl intel-level-zero-gpu" "dnf install -y intel-opencl intel-level-zero-gpu"
                        ;;
                    *)
                        log_warn "Intel GPU auto-install not available for $DISTRO"
                        log_warn "Install manually: https://github.com/intel/compute-runtime"
                        ;;
                esac
                ;;
            *)
                log_warn "Intel GPU support requires Linux. Skipping."
                exit 0
                ;;
        esac

        # Ensure /dev/dri exists
        if [[ ! -e /dev/dri ]]; then
            log_warn "/dev/dri not found — GPU kernel module may not be loaded"
        fi
        ;;

    *)
        log_error "Unknown GPU vendor: $GPU_VENDOR"
        exit 1
        ;;
esac

# Verify GPU access via crun
if command_exists crun; then
    log_info "[$MODULE_NAME] Testing GPU access via crun OCI sandbox..."
    TEST_DIR=$(mktemp -d)
    mkdir -p "$TEST_DIR/rootfs/dev" "$TEST_DIR/rootfs/proc" "$TEST_DIR/rootfs/sys"

    # Build OCI config with appropriate GPU device nodes
    case "$GPU_VENDOR" in
        nvidia)
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
            GPU_OUTPUT=$(crun run --bundle "$TEST_DIR" "gpu-verify" 2>/dev/null || echo "")
            crun delete -f "gpu-verify" 2>/dev/null || true
            if [[ -n "$GPU_OUTPUT" ]]; then
                log_success "GPU verified inside crun sandbox: $GPU_OUTPUT"
            else
                log_warn "GPU verification failed — check NVIDIA driver setup"
            fi
            ;;

        amd)
            cat > "$TEST_DIR/config.json" << 'CONFIG'
{
  "ociVersion": "1.0.2",
  "process": {
    "terminal": false,
    "user": {"uid": 0, "gid": 0},
    "args": ["rocm-smi"],
    "env": ["PATH=/usr/local/bin:/usr/bin:/bin", "ROCR_VISIBLE_DEVICES=0"],
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
    "namespaces": [{"type": "pid"}, {"type": "mount"}],
    "devices": [
      {"path": "/dev/kfd", "type": "c", "major": 235, "minor": 0, "permissions": "rwm"},
      {"path": "/dev/dri/renderD128", "type": "c", "major": 226, "minor": 128, "permissions": "rwm"},
      {"path": "/dev/dri/card0", "type": "c", "major": 226, "minor": 0, "permissions": "rwm"}
    ]
  }
}
CONFIG
            GPU_OUTPUT=$(crun run --bundle "$TEST_DIR" "gpu-verify" 2>/dev/null || echo "")
            crun delete -f "gpu-verify" 2>/dev/null || true
            if [[ -n "$GPU_OUTPUT" ]]; then
                log_success "GPU verified inside crun sandbox"
            else
                log_warn "GPU verification failed — check ROCm setup"
            fi
            ;;

        intel)
            cat > "$TEST_DIR/config.json" << 'CONFIG'
{
  "ociVersion": "1.0.2",
  "process": {
    "terminal": false,
    "user": {"uid": 0, "gid": 0},
    "args": ["clinfo"],
    "env": ["PATH=/usr/local/bin:/usr/bin:/bin", "ONEAPI_DEVICE_SELECTOR=level_zero:*"],
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
    "namespaces": [{"type": "pid"}, {"type": "mount"}],
    "devices": [
      {"path": "/dev/dri/renderD128", "type": "c", "major": 226, "minor": 128, "permissions": "rwm"},
      {"path": "/dev/dri/card0", "type": "c", "major": 226, "minor": 0, "permissions": "rwm"}
    ]
  }
}
CONFIG
            GPU_OUTPUT=$(crun run --bundle "$TEST_DIR" "gpu-verify" 2>/dev/null || echo "")
            crun delete -f "gpu-verify" 2>/dev/null || true
            if [[ -n "$GPU_OUTPUT" ]]; then
                log_success "GPU detected inside crun sandbox"
            else
                log_warn "GPU verification may need additional Intel compute runtime packages"
            fi
            ;;
    esac

    rm -rf "$TEST_DIR"
fi

log_success "[$MODULE_NAME] GPU runtime setup complete for: $GPU_VENDOR"
exit 0
