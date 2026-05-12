#!/usr/bin/env bash
set -euo pipefail

# Standard installer pattern:
# 1) Try downloading a release artifact (placeholder URL by default).
# 2) Fall back to local Go build when release download is not configured.
# 3) Install into a user bin dir and print PATH instructions.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
# Placeholder release URL. Replace with your actual release host later.
# Example: https://github.com/<org>/<repo>/releases/download
RELEASE_BASE_URL="${PIPEDPEER_RELEASE_BASE_URL:-https://example.com/pipedpeer/releases/download}"

mkdir -p "$INSTALL_DIR"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

artifact_name="${BINARY_NAME}-${PLATFORM}-${ARCH}"
if [[ "$VERSION" != "latest" ]]; then
  release_url="${RELEASE_BASE_URL}/${VERSION}/${artifact_name}"
else
  release_url="${RELEASE_BASE_URL}/latest/${artifact_name}"
fi

echo "Installing ${BINARY_NAME}..."
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
