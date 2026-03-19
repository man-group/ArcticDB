#!/usr/bin/env bash
#
# Create a development virtualenv for ArcticDB.
#
# Usage: create_venv.sh [--proxy-cmd CMD] <venv_dir>
#
# The script:
#   1. Creates the venv
#   2. Installs runtime + test dependencies from setup.cfg
#   3. Installs lint/format tools
#
# Note: protobuf stubs are NOT generated here. Use `make setup` for a full
# setup, or run `make protoc` separately with the venv activated.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROXY_CMD=""
if [ "${1:-}" = "--proxy-cmd" ]; then
    PROXY_CMD="$2"
    shift 2
fi

if [ $# -ne 1 ]; then
    echo "Usage: $0 [--proxy-cmd CMD] <venv_dir>" >&2
    exit 1
fi

VENV_DIR="$1"

echo "Creating venv at $VENV_DIR"
/usr/bin/python3 -m venv "$VENV_DIR"

PIP="$VENV_DIR/bin/pip"
PYTHON="$VENV_DIR/bin/python"

echo "Upgrading pip"
$PROXY_CMD "$PIP" install --upgrade pip

echo "Installing dependencies from setup.cfg"
"$PYTHON" "$SCRIPT_DIR/parse_setup_deps.py" "$REPO_ROOT/setup.cfg" \
    > "$VENV_DIR/_deps.txt"
$PROXY_CMD "$PIP" install -r "$VENV_DIR/_deps.txt"
rm -f "$VENV_DIR/_deps.txt"

echo "Installing lint/format tools"
$PROXY_CMD "$PYTHON" "$SCRIPT_DIR/format.py" --install-tools

echo ""
echo "Activate with:"
echo "source \$(make activate NAME=$(basename "$VENV_DIR"))"
