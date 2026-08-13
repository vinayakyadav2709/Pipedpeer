#!/usr/bin/env bash
set -euo pipefail

# Standard installer pattern:
# 1) Try downloading a release artifact for the selected channel.
# 2) Fall back to local Go build when release download is not configured.
# 3) Install into a user bin dir and print PATH instructions.
#
# Channels:
#   stable  (default) - latest tagged release
#   nightly           - rolling prerelease built from the dev branch

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CHANNEL="${PIPEDPEER_CHANNEL:-stable}"

usage() {
  cat <<'EOF'
Usage: install-pipedpeer.sh [--channel stable|nightly]

Options:
  --channel <name>   Release channel to install (default: stable)
  -h, --help         Show this help

Environment:
  PIPEDPEER_CHANNEL          Same as --channel
  PIPEDPEER_VERSION          Install a specific tag (stable channel only)
  PIPEDPEER_INSTALL_DIR      Install target dir (default: ~/.local/bin)
  PIPEDPEER_RELEASE_BASE_URL Override the release host
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --channel)
      CHANNEL="${2:-}"
      if [[ -z "$CHANNEL" ]]; then
        echo "--channel requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --channel=*)
      CHANNEL="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

case "$CHANNEL" in
  stable|nightly) ;;
  *)
    echo "Unknown channel: $CHANNEL (expected 'stable' or 'nightly')" >&2
    exit 1
    ;;
esac

INSTALL_DIR="${PIPEDPEER_INSTALL_DIR:-$HOME/.local/bin}"
BINARY_NAME="pipedpeer"
PLATFORM="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH_RAW="$(uname -m)"

case "$ARCH_RAW" in
  x86_64|amd64)
    ARCH="amd64"
    ;;
  aarch64|arm64)
    ARCH="arm64"
    ;;
  *)
    ARCH="$ARCH_RAW"
    ;;
esac

VERSION="${PIPEDPEER_VERSION:-latest}"
RELEASE_BASE_URL="${PIPEDPEER_RELEASE_BASE_URL:-https://github.com/vinayakyadav2709/Pipedpeer/releases}"

mkdir -p "$INSTALL_DIR"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

artifact_name="${BINARY_NAME}-${PLATFORM}-${ARCH}"
if [[ "$CHANNEL" == "nightly" ]]; then
  # Rolling prerelease republished from dev by the nightly workflow
  release_url="${RELEASE_BASE_URL}/download/nightly/${artifact_name}"
elif [[ "$VERSION" != "latest" ]]; then
  # Version tag format like v1.0.0
  release_url="${RELEASE_BASE_URL}/download/${VERSION}/${artifact_name}"
else
  # Uses GitHub's latest redirect
  release_url="${RELEASE_BASE_URL}/latest/download/${artifact_name}"
fi

echo "Installing ${BINARY_NAME} (${CHANNEL} channel)..."
echo "Target dir: ${INSTALL_DIR}"

download_and_install() {
  local url="$1"
  local out="$tmp_dir/$BINARY_NAME"

  if command -v curl >/dev/null 2>&1; then
    curl -fL "$url" -o "$out"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$out" "$url"
  else
    return 1
  fi

  chmod +x "$out"
  mv "$out" "$INSTALL_DIR/$BINARY_NAME"
}

local_build_install() {
  echo "Release artifact download unavailable; falling back to local build..."
  (
    cd "$repo_root/src"
    go build -o "$tmp_dir/$BINARY_NAME" .
  )
  chmod +x "$tmp_dir/$BINARY_NAME"
  mv "$tmp_dir/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
}

set +e
download_and_install "$release_url"
download_exit=$?
set -e

if [[ $download_exit -ne 0 ]]; then
  local_build_install
fi

if [[ ! -x "$INSTALL_DIR/$BINARY_NAME" ]]; then
  echo "Install failed: binary not found at $INSTALL_DIR/$BINARY_NAME" >&2
  exit 1
fi

echo
echo "Installed: $INSTALL_DIR/$BINARY_NAME"
"$INSTALL_DIR/$BINARY_NAME" --help >/dev/null 2>&1 || true

echo
echo "If command is not found, add this to your shell profile:"
echo "  export PATH=\"$INSTALL_DIR:\$PATH\""

echo
echo "Quick verify:"
echo "  $INSTALL_DIR/$BINARY_NAME jobs"
