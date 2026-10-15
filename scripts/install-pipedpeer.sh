#!/usr/bin/env bash
set -euo pipefail

# Downloads the pipedpeer binary for this machine, verifies it against the
# release's checksums.txt, installs it and runs `pipedpeer setup`.
#
# Usage: install-pipedpeer.sh [--version <v>] [--channel nightly|stable] [-y]
#   default             latest nightly (the rolling "nightly" release)
#   --version <v>       a build: releases/download/nightly-<v>/ (e.g. 0.1.4-nightly.20261015-239e713)
#   --channel stable    latest tagged v* release (old; nightly is the supported stream)
#   -y                  passed through to `pipedpeer setup`
# Environment: PIPEDPEER_RELEASE_BASE_URL overrides the release host,
#              PIPEDPEER_INSTALL_DIR forces the target dir (no sudo).

script_path="${BASH_SOURCE[0]:-}"
if [[ -n "$script_path" && -f "$script_path" ]]; then
  repo_root="$(cd "$(dirname "$script_path")/.." && pwd)"
else
  repo_root=""
fi

CHANNEL="${PIPEDPEER_CHANNEL:-nightly}"
VERSION="${PIPEDPEER_VERSION:-}"
SETUP_ARGS=()

usage() { sed -n '4,13p' "$script_path" 2>/dev/null | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) VERSION="${2:-}"; [[ -n "$VERSION" ]] || { echo "--version requires a value" >&2; exit 1; }; shift 2 ;;
    --version=*) VERSION="${1#*=}"; shift ;;
    --channel) CHANNEL="${2:-}"; [[ -n "$CHANNEL" ]] || { echo "--channel requires a value" >&2; exit 1; }; shift 2 ;;
    --channel=*) CHANNEL="${1#*=}"; shift ;;
    -y|--yes) SETUP_ARGS+=("-y"); shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

case "$CHANNEL" in
  stable|nightly) ;;
  *) echo "Unknown channel: $CHANNEL (expected 'nightly' or 'stable')" >&2; exit 1 ;;
esac

BINARY_NAME="pipedpeer"
PLATFORM="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) ARCH="$(uname -m)" ;;
esac

# Install target: PIPEDPEER_INSTALL_DIR, else Termux $PREFIX/bin (no sudo),
# else /usr/local/bin via sudo (or as root), else ~/.local/bin.
USE_SUDO=""
NEED_PATH_NOTE=""
if [[ -n "${PIPEDPEER_INSTALL_DIR:-}" ]]; then
  INSTALL_DIR="$PIPEDPEER_INSTALL_DIR"
elif [[ -n "${TERMUX_VERSION:-}" && -n "${PREFIX:-}" ]]; then
  INSTALL_DIR="$PREFIX/bin"
elif [[ "$(id -u)" -eq 0 ]]; then
  INSTALL_DIR="/usr/local/bin"
elif command -v sudo >/dev/null 2>&1; then
  INSTALL_DIR="/usr/local/bin"
  USE_SUDO="sudo"
else
  INSTALL_DIR="$HOME/.local/bin"
  NEED_PATH_NOTE=1
fi

RELEASE_BASE_URL="${PIPEDPEER_RELEASE_BASE_URL:-https://github.com/vinayakyadav2709/Pipedpeer/releases}"
artifact_name="${BINARY_NAME}-${PLATFORM}-${ARCH}"
if [[ -n "$VERSION" ]]; then
  base_url="${RELEASE_BASE_URL}/download/nightly-${VERSION#nightly-}"
elif [[ "$CHANNEL" == "nightly" ]]; then
  base_url="${RELEASE_BASE_URL}/download/nightly"
else
  base_url="${RELEASE_BASE_URL}/latest/download"
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

fetch() { # url out
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
  else
    echo "Neither curl nor wget is installed." >&2
    return 1
  fi
}

echo "Installing ${BINARY_NAME} from ${base_url}"
out="$tmp_dir/$artifact_name"
if fetch "${base_url}/${artifact_name}" "$out" && fetch "${base_url}/checksums.txt" "$tmp_dir/checksums.txt"; then
  want="$(awk -v f="$artifact_name" '$2 == f || $2 == "*" f {print $1}' "$tmp_dir/checksums.txt")"
  got="$(sha256sum "$out" | awk '{print $1}')"
  if [[ -z "$want" || "$want" != "$got" ]]; then
    echo "Checksum mismatch for ${artifact_name} (want '${want}', got '${got}'); not installing." >&2
    exit 1
  fi
  echo "Checksum ok."
elif [[ -n "$repo_root" && -d "$repo_root/src" ]] && command -v go >/dev/null 2>&1; then
  echo "Download failed; building from the local checkout instead (no checksum)." >&2
  (cd "$repo_root/src" && go build -o "$out" .)
else
  echo "Download failed: ${base_url}/${artifact_name}" >&2
  echo "No Go toolchain and checkout to build from." >&2
  exit 1
fi

chmod +x "$out"
$USE_SUDO mkdir -p "$INSTALL_DIR"
$USE_SUDO install -m 0755 "$out" "$INSTALL_DIR/$BINARY_NAME"
echo "Installed: $INSTALL_DIR/$BINARY_NAME"

if [[ -n "$NEED_PATH_NOTE" ]]; then
  case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *) echo "ACTION NEEDED: no sudo, so ${BINARY_NAME} went to $INSTALL_DIR which is not on PATH. Add to your shell profile:"
       echo "  export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
  esac
fi

"$INSTALL_DIR/$BINARY_NAME" setup ${SETUP_ARGS[@]+"${SETUP_ARGS[@]}"}

echo
echo "Next: run 'pipedpeer invite' on the first machine; on every other machine run 'pipedpeer connect <invite>'."
