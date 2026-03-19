# ArcticDB SBOM Tools

Generates a Software Bill of Materials (SBOM), license report, and vulnerability scan for ArcticDB (Linux only).

## Files

| File | Purpose |
|------|---------|
| `generate_sbom.sh` | Main driver — runs all phases end to end |
| `enrich_bom.py` | Builds clean enriched BOM with correct vcpkg versions and CPEs |
| `generate_report.py` | Generates HTML + Markdown reports |

## Quick Start

Run from the ArcticDB repo root:

```bash
# Full run (all phases)
./build_tooling/sbom_scan/generate_sbom.sh \
  --build-preset linux-release \
  --output-dir ~/arcticdb-sbom-reports

# If behind a corporate proxy, prepend your proxy wrapper:
./build_tooling/sbom_scan/generate_sbom.sh \
  --build-preset linux-release \
  --output-dir ~/arcticdb-sbom-reports
```

## Prerequisites

- Python 3.10+ with pip
- ArcticDB built so `vcpkg_installed/` exists
- Internet access to download grype (or set `TOOLS_DIR` to a pre-populated dir)

## Options

```
--arcticdb-root DIR      ArcticDB repo root (default: current dir)
--build-preset NAME      CMake preset (default: linux-release)
--output-dir DIR         Report output dir (default: ~/arcticdb-sbom-reports)
--tools-dir DIR          Tool download dir (default: ~/sbom-tools)
--tools-venv DIR         Python venv for scanning tools (default: ~/sbom-tools-env)
--arcticdb-version VER   Version string (default: from pyproject.toml)
--skip-grype             Skip vulnerability scan
--skip-pip-licenses      Skip pip-licenses extraction
--skip-build-check       Skip check for vcpkg_installed/ dir
```

## Phases

| Phase | What it does |
|-------|-------------|
| 0 | Validate environment, resolve version, check git tag and vcpkg consistency |
| 1 | Download grype if needed; create pip-licenses tools venv |
| 2 | Freeze product Python packages; extract licenses with pip-licenses |
| 3 | Create empty C++ BOM placeholder |
| 4 | Enrich BOM: read vcpkg_installed/ for C++ versions, add CPEs, add git submodule deps |
| 5 | Vulnerability scan with grype |
| 6 | Generate HTML + Markdown report |

## Output Files

```
~/arcticdb-sbom-reports/
  bom-enriched.json        # Clean, authoritative BOM (use this for audits)
  requirements-frozen.txt  # pip freeze snapshot of the product environment
  grype-vulns.json         # Vulnerability scan results (JSON)
  grype-vulns.txt          # Vulnerability scan results (table)
  pip-licenses.json        # Python package licenses (from pip-licenses)
  pip-licenses.txt         # Python package licenses (human-readable)
  arcticdb-sbom-report.html  # Combined HTML report (tabbed)
  arcticdb-sbom-report.md   # Markdown summary
  grype.log                # grype scan log
```

## CPE Coverage

CPEs (Common Platform Enumeration) are added for ~50 C++ libraries to enable
grype to match them against the NVD vulnerability database. Without CPEs,
`pkg:generic/openssl@3.3.0` would not be recognized by grype; with CPE
`cpe:2.3:a:openssl:openssl:3.3.0:*:*:*:*:*:*:*` it is matched correctly.

## Version Accuracy

C++ package versions come from `vcpkg_installed/x64-linux/share/*/vcpkg.spdx.json`
(the actually-installed packages), not from the vcpkg ports tree (which contains
the latest port HEAD versions). For example, openssl may be pinned to 3.3.0 via
a vcpkg.json override while the ports tree HEAD is 3.6.1 — the SBOM correctly
reports 3.3.0.

The script checks that the vcpkg submodule commit matches the `builtin-baseline`
in `cpp/vcpkg.json`. A mismatch means the installed packages may not correspond
to the declared baseline, and a rebuild is recommended before running the scan.
