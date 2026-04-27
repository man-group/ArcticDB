#!/usr/bin/env bash
# Install the rustfs S3-compatible server (https://github.com/rustfs/rustfs).
# Downloads a prebuilt binary from GitHub releases.
#
# Usage:
#   ./build_tooling/install_rustfs.sh              # installs to ~/bin/rustfs
#   ./build_tooling/install_rustfs.sh /usr/local/bin  # installs to /usr/local/bin/rustfs
#   INSTALL_DIR=/tmp/bin ./build_tooling/install_rustfs.sh  # via env var

set -euo pipefail

INSTALL_DIR="${1:-${INSTALL_DIR:-$HOME/bin}}"
RUSTFS_VERSION="${RUSTFS_VERSION:-latest}"
WORK_DIR=$(mktemp -d)

cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

# Detect platform
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)   ASSET="rustfs-linux-x86_64-gnu"   ;;
    Linux-aarch64)  ASSET="rustfs-linux-aarch64-gnu"   ;;
    Darwin-arm64)   ASSET="rustfs-macos-aarch64"       ;;
    Darwin-x86_64)  ASSET="rustfs-macos-x86_64"        ;;
    MINGW*|MSYS*|CYGWIN*)
        ASSET="rustfs-windows-x86_64"
        ;;
    *)
        echo "Unsupported platform: $(uname -s)-$(uname -m)" >&2
        exit 1
        ;;
esac

if [[ "$RUSTFS_VERSION" == "latest" ]]; then
    # Resolve latest version tag from GitHub API (includes pre-releases)
    RUSTFS_VERSION=$(curl -fsSL "https://api.github.com/repos/rustfs/rustfs/releases?per_page=1" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['tag_name'])")
    echo "==> Resolved latest version: ${RUSTFS_VERSION}"
fi
DOWNLOAD_URL="https://github.com/rustfs/rustfs/releases/download/${RUSTFS_VERSION}/${ASSET}-v${RUSTFS_VERSION}.zip"

echo "==> Downloading rustfs (${ASSET})..."
cd "$WORK_DIR"
curl -fSL -o rustfs.zip "$DOWNLOAD_URL"

echo "==> Extracting..."
unzip -q rustfs.zip

echo "==> Installing to ${INSTALL_DIR}..."
mkdir -p "$INSTALL_DIR"
if [[ "$ASSET" == *windows* ]]; then
    cp rustfs.exe "$INSTALL_DIR/" 2>/dev/null || cp */rustfs.exe "$INSTALL_DIR/" 2>/dev/null || find . -name "rustfs.exe" -exec cp {} "$INSTALL_DIR/" \;
    BINARY="$INSTALL_DIR/rustfs.exe"
else
    cp rustfs "$INSTALL_DIR/" 2>/dev/null || cp */rustfs "$INSTALL_DIR/" 2>/dev/null || find . -name "rustfs" -type f -exec cp {} "$INSTALL_DIR/" \;
    chmod +x "$INSTALL_DIR/rustfs"
    BINARY="$INSTALL_DIR/rustfs"
fi

echo "==> Installed: $BINARY ($(wc -c < "$BINARY" | tr -d ' ') bytes)"

# Check if install dir is in PATH
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    echo ""
    echo "NOTE: ${INSTALL_DIR} is not in your PATH. Add it with:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
fi
