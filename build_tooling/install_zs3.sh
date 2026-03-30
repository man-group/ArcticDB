#!/usr/bin/env bash
# Install the zs3 S3-compatible mock server (https://github.com/Lulzx/zs3).
# Downloads Zig, builds zs3 from source, and installs the binary.
#
# Usage:
#   ./build_tooling/install_zs3.sh              # installs to ~/bin/zs3
#   ./build_tooling/install_zs3.sh /usr/local/bin  # installs to /usr/local/bin/zs3
#   INSTALL_DIR=/tmp/bin ./build_tooling/install_zs3.sh  # via env var

set -euo pipefail

INSTALL_DIR="${1:-${INSTALL_DIR:-$HOME/bin}}"
ZIG_VERSION="${ZIG_VERSION:-0.15.0}"
WORK_DIR=$(mktemp -d)

cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

# Detect platform
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)   ZIG_ARCH="x86_64-linux"   ;;
    Linux-aarch64)  ZIG_ARCH="aarch64-linux"   ;;
    Darwin-arm64)   ZIG_ARCH="aarch64-macos"   ;;
    Darwin-x86_64)  ZIG_ARCH="x86_64-macos"    ;;
    MINGW*|MSYS*|CYGWIN*)
        ZIG_ARCH="x86_64-windows"
        ;;
    *)
        echo "Unsupported platform: $(uname -s)-$(uname -m)" >&2
        exit 1
        ;;
esac

if [[ "$ZIG_ARCH" == *windows* ]]; then
    EXT="zip"
else
    EXT="tar.xz"
fi

ZIG_URL="https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-${ZIG_VERSION}.${EXT}"

echo "==> Downloading Zig ${ZIG_VERSION} for ${ZIG_ARCH}..."
cd "$WORK_DIR"
curl -fSL -o "zig.${EXT}" "$ZIG_URL"

echo "==> Extracting Zig..."
if [[ "$EXT" == "tar.xz" ]]; then
    tar xf "zig.${EXT}"
else
    unzip -q "zig.${EXT}"
fi
ZIG_DIR=$(ls -d zig-*/)
export PATH="$WORK_DIR/$ZIG_DIR:$PATH"

echo "==> Cloning zs3..."
git clone --depth 1 --quiet https://github.com/Lulzx/zs3.git zs3-src

echo "==> Building zs3 (ReleaseFast)..."
cd zs3-src
zig build -Doptimize=ReleaseFast

echo "==> Installing to ${INSTALL_DIR}..."
mkdir -p "$INSTALL_DIR"
if [[ "$ZIG_ARCH" == *windows* ]]; then
    cp zig-out/bin/zs3.exe "$INSTALL_DIR/"
    BINARY="$INSTALL_DIR/zs3.exe"
else
    cp zig-out/bin/zs3 "$INSTALL_DIR/"
    BINARY="$INSTALL_DIR/zs3"
fi

echo "==> Installed: $BINARY ($(wc -c < "$BINARY" | tr -d ' ') bytes)"

# Check if install dir is in PATH
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    echo ""
    echo "NOTE: ${INSTALL_DIR} is not in your PATH. Add it with:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
fi
