# ArcticDB SBOM Tools

Generates a Software Bill of Materials (SBOM), license report, and vulnerability scan for ArcticDB.

## Files

| File | Purpose |
|------|---------|
| `generate_sbom.sh` | Main driver — runs all phases end to end |
| `enrich_bom.py` | Builds clean enriched BOM with correct vcpkg versions and CPEs |
| `generate_report.py` | Generates HTML + Markdown reports |

## Quick Start

```bash
cd ~/ArcticDB

# Full run (all phases)
./build_tooling/sbom_scan/generate_sbom.sh \
  --arcticdb-root ~/ArcticDB \
  --build-preset linux-release \
  --output-dir ~/arcticdb-sbom-reports

# If behind a corporate proxy, prepend your proxy wrapper:
withproxy ./build_tooling/sbom_scan/generate_sbom.sh \
  --arcticdb-root ~/ArcticDB \
  --build-preset linux-release \
  --output-dir ~/arcticdb-sbom-reports
```

## Prerequisites

- Python 3.10+ with pip
- ArcticDB built so `vcpkg_installed/` exists
- Internet access to download cdxgen and grype (or set `TOOLS_DIR` to a pre-populated dir)
- Optional: Java 21+ for cdxgen atom slicing (improves analysis depth; non-fatal if absent)

## Options

```
--arcticdb-root DIR      ArcticDB repo root (default: current dir)
--build-preset NAME      CMake preset (default: linux-release)
--output-dir DIR         Report output dir (default: ~/arcticdb-sbom-reports)
--tools-dir DIR          Tool download dir (default: ~/sbom-tools)
--tools-venv DIR         Python venv for scanning tools (default: ~/sbom-tools-env)
--python-version VER     Python type for cdxgen (default: auto, e.g. python312)
--arcticdb-version VER   Version string (default: from pyproject.toml)
--skip-cdxgen            Use existing bom-python.json / bom-cpp.json
--skip-grype             Skip vulnerability scan
--skip-pip-licenses      Skip pip-licenses extraction
--skip-build-check       Skip check for vcpkg_installed/ dir
```

## Phases

| Phase | What it does |
|-------|-------------|
| 0 | Validate environment, resolve versions |
| 1 | Download cdxgen + grype if needed; create tools venv |
| 2 | Generate Python BOM with cdxgen |
| 3 | Generate initial C++ BOM with cdxgen (raw, versionless junk expected) |
| 4 | Enrich BOM: replace vcpkg versions from vcpkg_installed/, add CPEs, add git submodule deps |
| 5 | Vulnerability scan with grype |
| 6 | License extraction with pip-licenses (in isolated tools venv) |
| 7 | Generate HTML + Markdown report |

## Output Files

```
~/arcticdb-sbom-reports/
  bom-python.json          # Raw Python BOM from cdxgen
  bom-cpp.json             # Raw C++ BOM from cdxgen (has versionless junk)
  bom-enriched.json        # Clean, authoritative BOM (use this for audits)
  grype-vulns.json         # Vulnerability scan results (JSON)
  grype-vulns.txt          # Vulnerability scan results (table)
  pip-licenses.json        # Python package licenses (from pip-licenses)
  pip-licenses.txt         # Python package licenses (human-readable)
  arcticdb-sbom-report.html  # Combined HTML report (tabbed)
  arcticdb-sbom-report.md   # Markdown summary
  cdxgen-python.log        # cdxgen Python scan log
  cdxgen-cpp.log           # cdxgen C++ scan log
  grype.log                # grype scan log
```

## CPE Coverage

CPEs (Common Platform Enumeration) are added for ~50 C++ libraries to enable
grype to match them against the NVD vulnerability database. Without CPEs,
`pkg:generic/openssl@3.3.0` would not be recognized by grype; with CPE
`cpe:2.3:a:openssl:openssl:3.3.0:*:*:*:*:*:*:*` it is matched correctly.
