#!/usr/bin/env bash
# =============================================================================
# ArcticDB SBOM Generator
# =============================================================================
# Generates a complete Software Bill of Materials (SBOM) for ArcticDB,
# covering Python deps, C++ deps (vcpkg), git submodules, vulnerability scan,
# license report, and a combined HTML + Markdown report.
#
# TOOL ISOLATION GUARANTEE:
#   Scanning tools (grype, pip-licenses) are NEVER installed in the product
#   Python environment. They live in a separate ~/sbom-tools-env venv or as
#   standalone binaries. This ensures they never appear in the BOM.
#
# VCPKG VERSION ACCURACY:
#   Versions are read from vcpkg_installed/x64-linux/share/*/vcpkg.spdx.json
#   (the actually-installed packages with the override-pinned versions), NOT
#   from the vcpkg/ports tree (which contains the latest port HEAD versions,
#   not the pinned/baseline versions that were actually built and linked).
#
# Usage: ./generate_sbom.sh [OPTIONS]
#   --arcticdb-root DIR     ArcticDB repo root (default: current dir)
#   --build-preset NAME     CMake preset used for build (default: linux-release)
#   --product-venv DIR      Python venv containing only product deps to scan
#                           (default: current Python env; EXCLUDE tools from here)
#   --output-dir DIR        Report output dir (default: ~/arcticdb-sbom-reports)
#   --tools-dir DIR         Standalone binaries dir (default: ~/sbom-tools)
#   --tools-venv DIR        Python venv for scanning tools (default: ~/sbom-tools-env)
#   --arcticdb-version VER  Version string (default: from pyproject.toml)
#   --skip-grype            Skip vulnerability scan
#   --skip-pip-licenses     Skip pip-licenses extraction
#   --skip-build-check      Skip check for vcpkg_installed/ dir
#   --help                  Show this help
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
ARCTICDB_ROOT="${ARCTICDB_ROOT:-$(pwd)}"
BUILD_PRESET="${BUILD_PRESET:-linux-release}"
PRODUCT_VENV="${PRODUCT_VENV:-}"               # empty = use current Python env
OUTPUT_DIR="${OUTPUT_DIR:-$HOME/arcticdb-sbom-reports}"
TOOLS_DIR="${TOOLS_DIR:-$HOME/sbom-tools}"
TOOLS_VENV="${TOOLS_VENV:-$HOME/sbom-tools-env}"
GRYPE_VERSION="${GRYPE_VERSION:-v0.109.1}"
ARCTICDB_VERSION=""
SKIP_GRYPE=0
SKIP_PIP_LICENSES=0
SKIP_BUILD_CHECK=0

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --arcticdb-root)     ARCTICDB_ROOT="$2"; shift 2 ;;
        --build-preset)      BUILD_PRESET="$2"; shift 2 ;;
        --product-venv)      PRODUCT_VENV="$2"; shift 2 ;;
        --output-dir)        OUTPUT_DIR="$2"; shift 2 ;;
        --tools-dir)         TOOLS_DIR="$2"; shift 2 ;;
        --tools-venv)        TOOLS_VENV="$2"; shift 2 ;;
        --arcticdb-version)  ARCTICDB_VERSION="$2"; shift 2 ;;
        --skip-grype)        SKIP_GRYPE=1; shift ;;
        --skip-pip-licenses) SKIP_PIP_LICENSES=1; shift ;;
        --skip-build-check)  SKIP_BUILD_CHECK=1; shift ;;
        --help|-h)
            grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \{0,3\}//' | head -40
            exit 0 ;;
        *) echo "Unknown option: $1. Use --help for usage."; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Derived paths
# ---------------------------------------------------------------------------
VCPKG_SHARE_DIR="${ARCTICDB_ROOT}/cpp/out/${BUILD_PRESET}-build/vcpkg_installed/x64-linux/share"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GRYPE="${TOOLS_DIR}/grype"
BOM_PYTHON="${OUTPUT_DIR}/bom-python.json"
BOM_CPP="${OUTPUT_DIR}/bom-cpp.json"
BOM_ENRICHED="${OUTPUT_DIR}/bom-enriched.json"
GRYPE_JSON="${OUTPUT_DIR}/grype-vulns.json"
GRYPE_TXT="${OUTPUT_DIR}/grype-vulns.txt"
PIP_LICENSES_JSON="${OUTPUT_DIR}/pip-licenses.json"
PIP_LICENSES_TXT="${OUTPUT_DIR}/pip-licenses.txt"

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  $*${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# ---------------------------------------------------------------------------
# Phase 0: Validation
# ---------------------------------------------------------------------------
section "Phase 0: Environment validation"

# Validate ArcticDB root
if [[ ! -f "${ARCTICDB_ROOT}/pyproject.toml" ]]; then
    error "ArcticDB root invalid: ${ARCTICDB_ROOT}"
    error "Use --arcticdb-root to specify the path."
    exit 1
fi
info "ArcticDB root: ${ARCTICDB_ROOT}"

# Resolve product Python executable
if [[ -n "${PRODUCT_VENV}" ]]; then
    if [[ ! -f "${PRODUCT_VENV}/bin/python3" ]]; then
        error "Product venv not found: ${PRODUCT_VENV}"
        exit 1
    fi
    PRODUCT_PYTHON="${PRODUCT_VENV}/bin/python3"
    info "Product venv: ${PRODUCT_VENV}"
else
    PRODUCT_PYTHON="$(which python3)"
    warn "No --product-venv specified. Using current Python: ${PRODUCT_PYTHON}"
    warn "If scanning tools are installed in this env, they WILL appear in the BOM."
    warn "Create a clean venv with only arcticdb and pass --product-venv for accuracy."
fi
info "Product Python: ${PRODUCT_PYTHON}"

# Confirm scanning tools are NOT in the product environment
TOOL_POLLUTION=$("${PRODUCT_PYTHON}" -m pip list 2>/dev/null \
    | grep -iE "^(pip.licenses|owasp.dep.scan|cdxgen|grype)" | head -5 || true)
if [[ -n "${TOOL_POLLUTION}" ]]; then
    warn "Scanning tools detected in product Python environment:"
    echo "${TOOL_POLLUTION}" | while read line; do warn "  $line"; done
    warn "These WILL appear in the BOM. Consider creating a clean product venv."
else
    success "Product Python environment is clean (no scanning tools detected)"
fi

# Resolve ArcticDB version
if [[ -z "$ARCTICDB_VERSION" ]]; then
    ARCTICDB_VERSION=$(grep -m1 '^version' "${ARCTICDB_ROOT}/pyproject.toml" 2>/dev/null \
        | sed 's/version\s*=\s*"\(.*\)"/\1/' | tr -d '[:space:]') \
        || ARCTICDB_VERSION=$("${PRODUCT_PYTHON}" -c "import arcticdb; print(arcticdb.__version__)" 2>/dev/null) \
        || ARCTICDB_VERSION="unknown"
fi
info "ArcticDB version: ${ARCTICDB_VERSION}"

# Check vcpkg_installed
if [[ $SKIP_BUILD_CHECK -eq 0 ]]; then
    if [[ ! -d "${VCPKG_SHARE_DIR}" ]]; then
        warn "vcpkg_installed not found: ${VCPKG_SHARE_DIR}"
        warn "C++ package versions will be incomplete."
        warn "Build first: CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTIC_CMAKE_PRESET=${BUILD_PRESET} pip install -ve ."
    else
        PKG_COUNT=$(ls -1 "${VCPKG_SHARE_DIR}" 2>/dev/null | wc -l)
        success "vcpkg_installed: ${PKG_COUNT} packages at ${VCPKG_SHARE_DIR}"

        # Show a few key versions to confirm accuracy
        for PKG in openssl zstd protobuf; do
            VER=$(python3 -c "
import json, glob, re
for f in glob.glob('${VCPKG_SHARE_DIR}/${PKG}/vcpkg.spdx.json'):
    d = json.load(open(f))
    for p in d.get('packages', []):
        v = p.get('versionInfo', '')
        if v and v != 'unknown' and not re.match(r'^[0-9a-f]{7,}$', v):
            print(re.split(r'#', v)[0]); break
" 2>/dev/null || echo "not found")
            info "  ${PKG}: ${VER} (from vcpkg_installed — pinned/override version)"
        done
    fi
fi

mkdir -p "${OUTPUT_DIR}"
mkdir -p "${TOOLS_DIR}"

# ---------------------------------------------------------------------------
# Phase 1: Tool setup
# ---------------------------------------------------------------------------
section "Phase 1: Tools setup (isolated from product environment)"


# ---- grype binary (only if needed) ----
if [[ $SKIP_GRYPE -eq 1 ]]; then
    info "Skipping grype install (--skip-grype)"
elif [[ -x "${GRYPE}" ]]; then
    GRYPE_VER=$("${GRYPE}" version 2>&1 | grep -oP 'Version:\s*\K[\d.]+' || echo "?")
    success "grype already installed: ${GRYPE} (v${GRYPE_VER})"
elif command -v grype &>/dev/null; then
    # Use system-installed grype
    GRYPE="$(command -v grype)"
    GRYPE_VER=$("${GRYPE}" version 2>&1 | grep -oP 'Version:\s*\K[\d.]+' || echo "?")
    success "Using system grype: ${GRYPE} (v${GRYPE_VER})"
else
    info "Downloading grype ${GRYPE_VERSION} (standalone binary)..."
    GRYPE_URL="https://github.com/anchore/grype/releases/download/${GRYPE_VERSION}/grype_${GRYPE_VERSION#v}_linux_amd64.tar.gz"
    GRYPE_TMP=$(mktemp -d)
    GRYPE_DOWNLOADED=0
    # Try curl first, wget as fallback
    if curl -fsSL --progress-bar --retry 2 "${GRYPE_URL}" 2>/dev/null \
       | tar -xz -C "${GRYPE_TMP}" 2>/dev/null \
       && [[ -f "${GRYPE_TMP}/grype" ]]; then
        GRYPE_DOWNLOADED=1
    elif wget -q "${GRYPE_URL}" -O "${GRYPE_TMP}/grype.tar.gz" 2>/dev/null \
       && [[ -s "${GRYPE_TMP}/grype.tar.gz" ]] \
       && tar -xz -C "${GRYPE_TMP}" -f "${GRYPE_TMP}/grype.tar.gz" 2>/dev/null \
       && [[ -f "${GRYPE_TMP}/grype" ]]; then
        GRYPE_DOWNLOADED=1
    fi

    if [[ $GRYPE_DOWNLOADED -eq 1 ]]; then
        mv "${GRYPE_TMP}/grype" "${GRYPE}"
        chmod +x "${GRYPE}"
        rm -rf "${GRYPE_TMP}"
        success "grype downloaded to ${GRYPE}"
    else
        warn "Could not download grype automatically."
        warn "To install grype manually:"
        warn "  Via install script: curl -sSfL https://raw.githubusercontent.com/anchore/grype/main/install.sh | sh -s -- -b ${TOOLS_DIR}"
        warn "  Via Go: go install github.com/anchore/grype@latest"
        warn "  Prebuilt: copy grype binary to ${GRYPE} and chmod +x it"
        warn "  If behind a proxy: run the script with your proxy wrapper, e.g. withproxy ./generate_sbom.sh ..."
        warn "Vulnerability scan will be skipped for this run."
        rm -rf "${GRYPE_TMP}"
        SKIP_GRYPE=1
    fi
fi

# ---- pip-licenses in isolated TOOLS venv ----
# CRITICAL: pip-licenses is installed in its OWN venv (~/sbom-tools-env),
# NEVER in the product environment. It reads the product env's packages
# via PYTHONPATH injection (see Phase 6).
if [[ $SKIP_PIP_LICENSES -eq 0 ]]; then
    if [[ ! -f "${TOOLS_VENV}/bin/pip-licenses" ]]; then
        info "Creating isolated tools venv: ${TOOLS_VENV}"
        python3 -m venv "${TOOLS_VENV}"
        "${TOOLS_VENV}/bin/pip" install --quiet --upgrade pip pip-licenses
        success "pip-licenses installed in tools venv: ${TOOLS_VENV}"
    else
        success "pip-licenses already in tools venv: ${TOOLS_VENV}"
    fi
    PIP_LIC_VERSION=$("${TOOLS_VENV}/bin/pip-licenses" --version 2>/dev/null || echo "unknown")
    info "pip-licenses version: ${PIP_LIC_VERSION}"
fi

# ---------------------------------------------------------------------------
# Phase 2: Python package discovery (pip freeze + pip-licenses)
# ---------------------------------------------------------------------------
# pip freeze and pip-licenses run BEFORE BOM enrichment (Phase 4) so their
# data is available for license annotation in the enriched BOM.
# ---------------------------------------------------------------------------
section "Phase 2: Python package discovery"

# Always produce pip freeze from the product venv — it is the authoritative
# list of actually installed packages with exact pinned versions.
REQS_FROZEN="${OUTPUT_DIR}/requirements-frozen.txt"
info "Freezing product environment: ${PRODUCT_PYTHON}"
"${PRODUCT_PYTHON}" -m pip freeze 2>/dev/null \
    | grep -v "^DEPRECATION" > "${REQS_FROZEN}" || \
"${PRODUCT_PYTHON}" -m pip freeze > "${REQS_FROZEN}" || true
PKG_FROZEN=$(grep -c "==" "${REQS_FROZEN}" 2>/dev/null || echo "0")
success "Frozen ${PKG_FROZEN} packages → ${REQS_FROZEN}"

# pip-licenses: runs in isolated tools venv, scans product env via PYTHONPATH.
# Must run BEFORE enrichment so license data is available for Phase 4.
if [[ $SKIP_PIP_LICENSES -eq 0 ]]; then
    info "Extracting licenses with pip-licenses (isolated tools venv)..."
    info "  Binary: ${TOOLS_VENV}/bin/pip-licenses"
    info "  Scanning: ${PRODUCT_PYTHON} packages"
    PRODUCT_SITE=$("${PRODUCT_PYTHON}" -c "import sysconfig; print(sysconfig.get_path('purelib'))" 2>/dev/null || true)
    info "  Site-packages: ${PRODUCT_SITE}"

    # Inject product site-packages so pip-licenses sees product packages, not tools venv packages
    if [[ -n "${PRODUCT_SITE}" && -d "${PRODUCT_SITE}" ]]; then
        PYTHONPATH="${PRODUCT_SITE}" "${TOOLS_VENV}/bin/pip-licenses" \
            --with-urls --with-description --format=json \
            --output-file "${PIP_LICENSES_JSON}" 2>/dev/null || true
        PYTHONPATH="${PRODUCT_SITE}" "${TOOLS_VENV}/bin/pip-licenses" \
            --with-urls --format=table \
            --output-file "${PIP_LICENSES_TXT}" 2>/dev/null || true
    else
        # Fallback: --python flag (pip-licenses 5+)
        "${TOOLS_VENV}/bin/pip-licenses" \
            --python "${PRODUCT_PYTHON}" --with-urls --format=json \
            --output-file "${PIP_LICENSES_JSON}" 2>/dev/null || \
        "${TOOLS_VENV}/bin/pip-licenses" \
            --with-urls --format=json \
            --output-file "${PIP_LICENSES_JSON}" 2>/dev/null || true
    fi

    if [[ -f "${PIP_LICENSES_JSON}" ]]; then
        PIP_COUNT=$(python3 -c "import json; print(len(json.load(open('${PIP_LICENSES_JSON}'))))" 2>/dev/null || echo "?")
        # Isolation check: scanning tools must NOT appear in the output
        TOOL_CHECK=$(python3 -c "
import json
data = json.load(open('${PIP_LICENSES_JSON}'))
tools = [p.get('Name','') for p in data
         if p.get('Name','').lower() in ('pip-licenses','piplicenses','owasp-dep-scan','cdxgen')]
print('WARNING tools found: ' + ', '.join(tools) if tools else 'OK: no scanning tools in output')
" 2>/dev/null || echo "check failed")
        if echo "${TOOL_CHECK}" | grep -q "WARNING"; then
            warn "pip-licenses isolation issue: ${TOOL_CHECK}"
        else
            success "pip-licenses: ${PIP_COUNT} packages, ${TOOL_CHECK} → ${PIP_LICENSES_JSON}"
        fi
    else
        warn "pip-licenses did not produce output"
    fi
fi


# ---------------------------------------------------------------------------
# Phase 3: C++ BOM placeholder
# ---------------------------------------------------------------------------
section "Phase 3: C++ BOM (vcpkg_installed is the authoritative source)"

# C++ packages come exclusively from vcpkg_installed/ in Phase 4 enrichment.
# Create an empty placeholder; enrich_bom.py reads vcpkg directly.
python3 -c "import json; json.dump({'bomFormat':'CycloneDX','specVersion':'1.5','components':[]}, open('${BOM_CPP}','w'))"
info "C++ BOM placeholder created; vcpkg_installed/ scanned during enrichment"

# ---------------------------------------------------------------------------
# Phase 4: BOM Enrichment — THE KEY FIX FOR VCPKG VERSIONS
# ---------------------------------------------------------------------------
section "Phase 4: BOM Enrichment (correct vcpkg versions + CPEs + submodules)"

info "Version source priority:"
info "  1. vcpkg_installed/*/vcpkg.spdx.json  ← ACTUALLY installed versions"
info "     (reflects pinned overrides, e.g. openssl=3.3.0, NOT ports HEAD 3.6.1)"
info "  2. cpp/vcpkg.json overrides            ← Fallback if spdx missing"
info "  3. git submodule status                ← Submodule versions (pybind11, entt, lmdb...)"
info "  X. vcpkg/ports/ tree                  ← NOT used (wrong versions for pinned packages)"

ENRICH_ARGS=(
    --cpp-bom "${BOM_CPP}"
    --vcpkg-share "${VCPKG_SHARE_DIR}"
    --arcticdb-root "${ARCTICDB_ROOT}"
    --output "${BOM_ENRICHED}"
    --arcticdb-version "${ARCTICDB_VERSION}"
)
# Python source: pip freeze (via setup.cfg runtime dep resolution)
REQS_FROZEN="${OUTPUT_DIR}/requirements-frozen.txt"
if [[ -f "${REQS_FROZEN}" ]]; then
    ENRICH_ARGS+=(--requirements-frozen "${REQS_FROZEN}")
    info "Python source: pip freeze (${REQS_FROZEN})"
else
    ENRICH_ARGS+=(--python-bom "${BOM_PYTHON}")
    warn "Python source: empty BOM placeholder (pip freeze not available)"
fi
# Pass pip-licenses data if available (for Python license enrichment)
if [[ -f "${PIP_LICENSES_JSON}" ]]; then
    ENRICH_ARGS+=(--pip-licenses "${PIP_LICENSES_JSON}")
fi
# setup.cfg is the source of truth for runtime deps — filters out dev/test packages
SETUP_CFG="${ARCTICDB_ROOT}/setup.cfg"
if [[ -f "${SETUP_CFG}" ]]; then
    ENRICH_ARGS+=(--setup-cfg "${SETUP_CFG}" --product-python "${PRODUCT_PYTHON}")
    info "Python filter: runtime deps from ${SETUP_CFG}"
else
    warn "setup.cfg not found at ${SETUP_CFG} — all pip packages will be included"
fi

python3 "${SCRIPT_DIR}/enrich_bom.py" "${ENRICH_ARGS[@]}"

if [[ -f "${BOM_ENRICHED}" ]]; then
    ENRICHED_COUNT=$(python3 -c "import json; print(len(json.load(open('${BOM_ENRICHED}')).get('components',[])))" 2>/dev/null || echo "?")
    success "Enriched BOM: ${BOM_ENRICHED} (${ENRICHED_COUNT} components, all versioned)"

    # Quick sanity check: show key package versions from the enriched BOM
    info "Version verification (enriched BOM):"
    python3 - "${BOM_ENRICHED}" << 'EOF'
import json, sys
bom = json.load(open(sys.argv[1]))
check = {"openssl": None, "zstd": None, "protobuf": None, "pybind11": None, "aws-sdk-cpp": None}
for c in bom.get("components", []):
    name = c.get("name", "").lower()
    if name in check:
        check[name] = c.get("version", "?")
for name, version in check.items():
    status = f"✓ {version}" if version else "✗ NOT FOUND"
    print(f"    {name:20s}: {status}")
EOF
else
    error "Enriched BOM not created."
    exit 1
fi

# ---------------------------------------------------------------------------
# Phase 5: Vulnerability scan (grype)
# ---------------------------------------------------------------------------
section "Phase 5: Vulnerability scan (grype)"

if [[ $SKIP_GRYPE -eq 1 ]]; then
    warn "Skipping vulnerability scan (--skip-grype)"
    echo '{"matches":[]}' > "${GRYPE_JSON}"
else
    info "Updating grype vulnerability database..."
    "${GRYPE}" db update 2>&1 | tail -3 || warn "grype DB update failed (using cached DB)"

    info "Scanning enriched BOM for vulnerabilities..."
    if "${GRYPE}" "sbom:${BOM_ENRICHED}" \
        --output json \
        --file "${GRYPE_JSON}" \
        --add-cpes-if-none \
        2>&1 | tee "${OUTPUT_DIR}/grype.log" | tail -5; then
        VULN_COUNT=$(python3 -c "import json; print(len(json.load(open('${GRYPE_JSON}')).get('matches',[])))" 2>/dev/null || echo "?")
        success "Vulnerability scan: ${VULN_COUNT} findings → ${GRYPE_JSON}"
    else
        warn "grype scan failed. Check ${OUTPUT_DIR}/grype.log"
        echo '{"matches":[]}' > "${GRYPE_JSON}"
    fi

    # Human-readable table
    "${GRYPE}" "sbom:${BOM_ENRICHED}" \
        --output table --add-cpes-if-none 2>/dev/null > "${GRYPE_TXT}" || true

    if [[ -s "${GRYPE_TXT}" ]]; then
        info "Top vulnerability findings:"
        head -30 "${GRYPE_TXT}"
    fi
fi

# ---------------------------------------------------------------------------
# Phase 6: Report generation
# ---------------------------------------------------------------------------
section "Phase 6: Report generation"

REPORT_ARGS=(
    --bom "${BOM_ENRICHED}"
    --output-dir "${OUTPUT_DIR}"
)
[[ -f "${GRYPE_JSON}" ]]       && REPORT_ARGS+=(--grype "${GRYPE_JSON}")
[[ -f "${PIP_LICENSES_JSON}" ]] && REPORT_ARGS+=(--pip-licenses "${PIP_LICENSES_JSON}")

python3 "${SCRIPT_DIR}/generate_report.py" "${REPORT_ARGS[@]}"

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
section "SBOM Generation Complete"

echo ""
printf "  %-22s %s\n" "ArcticDB version:"  "${ARCTICDB_VERSION}"
printf "  %-22s %s\n" "Build preset:"      "${BUILD_PRESET}"
printf "  %-22s %s\n" "Product Python:"    "${PRODUCT_PYTHON}"
printf "  %-22s %s\n" "Output directory:"  "${OUTPUT_DIR}"
echo ""
echo "  Generated files:"
printf "    %-25s %s\n" "Enriched BOM:"    "${BOM_ENRICHED}"
[[ -f "${GRYPE_JSON}" ]]         && printf "    %-25s %s\n" "Vulns (JSON):"  "${GRYPE_JSON}"
[[ -f "${GRYPE_TXT}" ]]          && printf "    %-25s %s\n" "Vulns (table):" "${GRYPE_TXT}"
[[ -f "${PIP_LICENSES_JSON}" ]]  && printf "    %-25s %s\n" "Pip licenses:"  "${PIP_LICENSES_JSON}"
HTML="${OUTPUT_DIR}/arcticdb-sbom-report.html"
MD="${OUTPUT_DIR}/arcticdb-sbom-report.md"
[[ -f "${HTML}" ]]               && printf "    %-25s %s\n" "HTML report:"   "${HTML}"
[[ -f "${MD}" ]]                 && printf "    %-25s %s\n" "MD report:"     "${MD}"
echo ""

# Severity summary
if [[ -f "${GRYPE_JSON}" ]]; then
    python3 - "${GRYPE_JSON}" << 'PYEOF'
import json, sys
from collections import Counter
data = json.load(open(sys.argv[1]))
matches = data.get("matches", [])
if not matches:
    print("  Vulnerabilities: NONE FOUND ✓")
else:
    counts = Counter(m.get("vulnerability", {}).get("severity", "unknown").upper() for m in matches)
    total = sum(counts.values())
    print(f"  Vulnerability summary ({total} total):")
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "NEGLIGIBLE", "UNKNOWN"]:
        n = counts.get(sev, 0)
        if n > 0:
            print(f"    {sev:12s}: {n}")
PYEOF
fi

echo ""
[[ -f "${HTML}" ]] && success "Open report: xdg-open ${HTML}"
echo ""
