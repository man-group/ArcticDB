#!/usr/bin/env python3
"""
ArcticDB SBOM Report Generator
================================
Generates a Markdown report from:
  - Enriched CycloneDX BOM (bom-enriched.json)
  - grype vulnerability scan results (grype-vulns.json)
  - pip-licenses output (pip-licenses.json)  [optional]

Sections: Summary, Vulnerabilities, Copyleft Flags, Python Deps,
          C++ Deps, BSL-1.1 Compatibility, Version Range Coverage.
"""

from __future__ import annotations

import json
import os
import re
import sys
import argparse
from datetime import datetime



def generate_range_boundaries(name: str, constraint: str, installed_version: str) -> list[tuple[str, str]]:
    """Mirror of enrich_bom.generate_range_boundaries — kept in sync manually."""
    lower_m = re.search(r">=?\s*((\d+)\.[\d.a-zA-Z]+)", constraint.split(",")[0])
    if not lower_m:
        return []
    lower_major = int(lower_m.group(2))
    lower_version = re.sub(r"\.post\d+$", "", lower_m.group(1))
    upper_m = re.search(r"<\s*(\d+)", constraint)
    upper_major = int(upper_m.group(1)) - 1 if upper_m else None
    try:
        installed_major = int(installed_version.split(".")[0])
    except (ValueError, AttributeError, IndexError):
        return []
    max_major = upper_major if upper_major is not None else installed_major
    if max_major < lower_major or max_major == lower_major == installed_major:
        return []
    boundaries = []
    for major in range(lower_major, max_major + 1):
        if major == installed_major:
            continue
        version = lower_version if major == lower_major else f"{major}.0.0"
        boundaries.append((version, f"range-boundary (v{major}.x supported)"))
    return boundaries


SEVERITY_COLORS = {
    "critical": "#d32f2f",
    "high": "#e64a19",
    "medium": "#f57c00",
    "low": "#388e3c",
    "negligible": "#757575",
    "unknown": "#9e9e9e",
}

COPYLEFT_SPDX = {"GPL-2.0", "GPL-3.0", "LGPL-2.1", "LGPL-3.0", "AGPL-3.0", "MPL-2.0", "CDDL-1.0",
                 "GPL-2.0-only", "GPL-3.0-only", "LGPL-2.1-only", "LGPL-3.0-only", "AGPL-3.0-only"}

# ---------------------------------------------------------------------------
# BSL-1.1 compatibility classification
# ArcticDB is distributed under Business Source License 1.1 (BSL-1.1).
# This classifies each dependency license for BSL-1.1 distribution compatibility.
# ---------------------------------------------------------------------------
# Values: "permissive", "weak_copyleft", "strong_copyleft", "unknown"
BSL_COMPAT: dict[str, str] = {
    # Permissive — attribution required, no copyleft concerns
    "MIT": "permissive",
    "MIT License": "permissive",
    "BSD-2-Clause": "permissive",
    "BSD-3-Clause": "permissive",
    "BSD License": "permissive",
    "Apache-2.0": "permissive",
    "Apache Software License": "permissive",
    "ISC": "permissive",
    "Zlib": "permissive",
    "zlib/libpng": "permissive",
    "curl": "permissive",
    "Unlicense": "permissive",
    "BSL-1.0": "permissive",         # Boost — permissive
    "OLDAP-2.8": "permissive",       # OpenLDAP — permissive with attribution
    "OpenSSL": "permissive",         # OpenSSL legacy (now Apache-2.0)
    "PSF-2.0": "permissive",         # Python Software Foundation
    "PSF License": "permissive",
    "bzip2-1.0.6": "permissive",     # bzip2 — BSD-like, no copyleft
    "CC0-1.0": "permissive",
    "Public Domain": "permissive",
    "Proprietary": "unknown",        # e.g. ahl.pkglib — needs case-by-case review
    # Weak copyleft — legal review required for binary distribution
    "LGPL-2.1": "weak_copyleft",     # Requires ability to relink (binary dist concern)
    "LGPL-3.0": "weak_copyleft",
    "LGPL-2.1-only": "weak_copyleft",
    "LGPL-3.0-only": "weak_copyleft",
    "MPL-2.0": "weak_copyleft",      # File-level copyleft; manageable if not modified
    "Mozilla Public License 2.0 (MPL 2.0)": "weak_copyleft",
    "CDDL-1.0": "weak_copyleft",
    # Strong copyleft — incompatible with BSL-1.1 distribution
    "GPL-2.0": "strong_copyleft",
    "GPL-3.0": "strong_copyleft",
    "AGPL-3.0": "strong_copyleft",
    "GPL-2.0-only": "strong_copyleft",
    "GPL-3.0-only": "strong_copyleft",
    "AGPL-3.0-only": "strong_copyleft",
}

BSL_COMPAT_LABEL = {
    "permissive": "✓ Permissive",
    "weak_copyleft": "⚠ Weak Copyleft",
    "strong_copyleft": "✗ Strong Copyleft",
    "unknown": "? Unknown",
}

BSL_COMPAT_COLOR = {
    "permissive": "#2e7d32",
    "weak_copyleft": "#e65100",
    "strong_copyleft": "#b71c1c",
    "unknown": "#757575",
}


def classify_bsl_compat(license_str: str) -> str:
    """Classify a license string for BSL-1.1 compatibility.
    Returns one of: permissive, weak_copyleft, strong_copyleft, unknown.
    """
    if not license_str or license_str.lower() in ("unknown", ""):
        return "unknown"
    # Check exact matches first
    if license_str in BSL_COMPAT:
        return BSL_COMPAT[license_str]
    # Check partial matches (handles compound licenses like "Apache Software License; BSD License")
    result = "permissive"  # default if any match found
    matched = False
    for lic_key, compat in BSL_COMPAT.items():
        if lic_key.lower() in license_str.lower():
            matched = True
            # Escalate severity: strong > weak > permissive > unknown
            if compat == "strong_copyleft":
                return "strong_copyleft"
            if compat == "weak_copyleft":
                result = "weak_copyleft"
            elif compat == "unknown" and result == "permissive":
                result = "unknown"
    return result if matched else "unknown"


def load_json(path: str) -> dict | list | None:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"  Warning: could not load {path}: {e}", file=sys.stderr)
        return None


def get_component_source(comp: dict) -> str:
    """Determine component source from properties."""
    for prop in comp.get("properties", []):
        if prop.get("name") == "cdx:build:source":
            return prop.get("value", "unknown")
    # Fallback: infer from purl
    purl = comp.get("purl", "")
    if "pkg:pypi" in purl or "pkg:python" in purl:
        return "pip"
    if "git-submodule" in comp.get("description", ""):
        return "git-submodule"
    return "cpp"


def get_version_source(comp: dict) -> str:
    """Return the vcpkg version source property if present, else empty string."""
    for prop in comp.get("properties", []):
        if prop.get("name") == "vcpkg:version-source":
            return prop.get("value", "")
    return ""


def categorize_components(bom: dict) -> dict:
    """Split components by source type. Range-boundary entries are excluded from
    the main lists (they are not installed components) and collected separately."""
    python_comps = []
    vcpkg_comps = []
    submodule_comps = []
    other_comps = []
    range_boundary_comps = []

    for comp in bom.get("components", []):
        if get_prop(comp, "python:version-role") == "range-boundary":
            range_boundary_comps.append(comp)
            continue
        source = get_component_source(comp)
        if source == "pip":
            python_comps.append(comp)
        elif source == "vcpkg":
            vcpkg_comps.append(comp)
        elif source == "git-submodule":
            submodule_comps.append(comp)
        else:
            other_comps.append(comp)

    return {
        "python": python_comps,
        "vcpkg": vcpkg_comps,
        "submodule": submodule_comps,
        "other": other_comps,
        "range_boundary": range_boundary_comps,
    }


def get_license_str(comp: dict) -> str:
    licenses = comp.get("licenses", [])
    if not licenses:
        return "Unknown"
    parts = []
    for lic in licenses:
        if "license" in lic:
            parts.append(lic["license"].get("id") or lic["license"].get("name", "Unknown"))
        elif "expression" in lic:
            parts.append(lic["expression"])
    return ", ".join(parts) if parts else "Unknown"


def get_prop(comp: dict, key: str) -> str:
    """Return a named BOM property value, or empty string if absent."""
    for p in comp.get("properties", []):
        if p.get("name") == key:
            return p.get("value", "")
    return ""


def build_source_lookup(bom: dict) -> dict[str, dict]:
    """Build lookup maps from the enriched BOM.

    Returns two keys per component:
      - by purl  (exact match used by grype artifacts)
      - by name  (fallback; installed version takes priority over range-boundary)
    """
    lookup: dict[str, dict] = {}
    for comp in bom.get("components", []):
        name = comp.get("name", "").lower()
        purl = comp.get("purl", "")
        version_role = get_prop(comp, "python:version-role") or "installed"
        entry = {
            "source": get_component_source(comp),
            "version_source": get_version_source(comp),
            "version_role": version_role,
            "version_constraint": get_prop(comp, "python:version-constraint"),
            "version_role_label": get_prop(comp, "python:version-role-label"),
        }
        if purl:
            lookup[purl] = entry
        # Name-level fallback: prefer installed over range-boundary
        if version_role == "installed" or name not in lookup:
            lookup[name] = entry
    return lookup


def parse_grype_results(grype_data: dict | None, source_lookup: dict | None = None) -> list:
    """Parse grype JSON output into flat vulnerability list."""
    if not grype_data:
        return []
    vulns = []
    sl = source_lookup or {}
    for match in grype_data.get("matches", []):
        vuln = match.get("vulnerability", {})
        artifact = match.get("artifact", {})
        severity = vuln.get("severity", "unknown").lower()
        pkg_name = artifact.get("name", "")
        artifact_purl = artifact.get("purl", "")
        bom_entry = sl.get(artifact_purl) or sl.get(pkg_name.lower(), {})
        source = bom_entry.get("source", "")
        version_source = bom_entry.get("version_source", "")
        version_role = bom_entry.get("version_role", "installed")
        version_role_label = bom_entry.get("version_role_label", "")
        if not source:
            grype_type = artifact.get("type", "")
            source = "pip" if grype_type == "python" else ("vcpkg" if grype_type else "unknown")
        vulns.append({
            "id": vuln.get("id", ""),
            "severity": severity,
            "package": pkg_name,
            "version": artifact.get("version", ""),
            "fix_versions": ", ".join(vuln.get("fix", {}).get("versions", [])) or "None",
            "description": vuln.get("description", "")[:200],
            "urls": vuln.get("urls", [])[:2],
            "source": source,
            "version_source": version_source,
            "version_role": version_role,
            "version_role_label": version_role_label,
        })
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "negligible": 4, "unknown": 5}
    vulns.sort(key=lambda v: (0 if v["version_role"] == "installed" else 1,
                              order.get(v["severity"], 5)))
    return vulns


def count_severities(vulns: list) -> dict:
    counts = {"critical": 0, "high": 0, "medium": 0, "low": 0, "negligible": 0, "unknown": 0}
    for v in vulns:
        sev = v["severity"].lower()
        counts[sev] = counts.get(sev, 0) + 1
    return counts


def generate_markdown_report(bom: dict, grype_data: dict | None, pip_licenses: list | None, output_path: str):
    """Generate a Markdown summary report."""
    categories = categorize_components(bom)
    vulns = parse_grype_results(grype_data, build_source_lookup(bom))
    sev_counts = count_severities(vulns)

    arcticdb_meta = bom.get("metadata", {}).get("component", {})
    arcticdb_version = arcticdb_meta.get("version", "unknown")
    scan_date = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    copyleft_comps = []
    for comp in bom.get("components", []):
        lic_str = get_license_str(comp)
        for spdx in COPYLEFT_SPDX:
            if spdx.lower() in lic_str.lower():
                copyleft_comps.append((comp, lic_str))
                break

    installed_vulns = [v for v in vulns if v["version_role"] == "installed"]
    boundary_vulns  = [v for v in vulns if v["version_role"] == "range-boundary"]
    installed_ids = {(v["id"], v["package"]) for v in installed_vulns}
    boundary_only_vulns = [v for v in boundary_vulns
                           if (v["id"], v["package"]) not in installed_ids]

    total_components = (len(categories["python"]) + len(categories["vcpkg"]) +
                        len(categories["submodule"]) + len(categories["other"]))

    bsl_counts: dict[str, int] = {"permissive": 0, "weak_copyleft": 0, "strong_copyleft": 0, "unknown": 0}
    installed_comps = (categories["python"] + categories["vcpkg"] +
                       categories["submodule"] + categories["other"])
    bsl_rows = []
    for comp in sorted(installed_comps, key=lambda c: c.get("name", "").lower()):
        lic_str = get_license_str(comp)
        compat = classify_bsl_compat(lic_str)
        bsl_counts[compat] = bsl_counts.get(compat, 0) + 1
        bsl_rows.append({
            "name": comp.get("name", ""),
            "version": comp.get("version", ""),
            "license": lic_str,
            "source": get_component_source(comp),
            "compat": compat,
        })

    lines = [
        f"# ArcticDB SBOM Report — v{arcticdb_version}",
        f"",
        f"**Generated:** {scan_date}",
        f"**Distribution License:** Business Source License 1.1 (BSL-1.1) — Licensor: Man Group Operations Limited",
        f"",
        f"## Summary",
        f"",
        f"| Category | Count |",
        f"|----------|-------|",
        f"| Total components (installed) | {total_components} |",
        f"| Python (pip) | {len(categories['python'])} |",
        f"| C++ (vcpkg) | {len(categories['vcpkg'])} |",
        f"| Git submodules | {len(categories['submodule'])} |",
        f"| Vulnerabilities (installed version) | {len(installed_vulns)} |",
        f"| Vulnerabilities (supported range only) | {len(boundary_only_vulns)} |",
        f"| Critical | {sev_counts['critical']} |",
        f"| High | {sev_counts['high']} |",
        f"| Medium | {sev_counts['medium']} |",
        f"| Low | {sev_counts['low']} |",
        f"| Copyleft licenses | {len(copyleft_comps)} |",
        f"| BSL: Permissive deps | {bsl_counts['permissive']} |",
        f"| BSL: Weak Copyleft (review needed) | {bsl_counts['weak_copyleft']} |",
        f"| BSL: Strong Copyleft (incompatible) | {bsl_counts['strong_copyleft']} |",
        f"| BSL: Unknown license | {bsl_counts['unknown']} |",
        f"",
    ]

    lines += ["## Vulnerabilities", ""]
    if vulns:
        lines += ["| CVE | Severity | Package | Version | Scope | Source | Version Source | Fix Available |",
                  "|-----|----------|---------|---------|-------|--------|----------------|---------------|"]
        for v in vulns:
            scope = "range boundary" if v["version_role"] == "range-boundary" else "installed"
            lines.append(f"| {v['id']} | **{v['severity'].upper()}** | {v['package']} | {v['version']} | {scope} | {v['source']} | {v['version_source'] or '-'} | {v['fix_versions']} |")
    else:
        lines.append("✓ No vulnerabilities found")
    lines.append("")

    # Copyleft
    lines += ["## Copyleft / Restrictive Licenses", ""]
    if copyleft_comps:
        lines += ["| Package | Version | License | Source |", "|---------|---------|---------|--------|"]
        for comp, lic in copyleft_comps:
            lines.append(f"| {comp.get('name', '')} | {comp.get('version', '')} | **{lic}** | {get_component_source(comp)} |")
        lines += ["", "> ⚠️ These components require legal review before distribution."]
    else:
        lines.append("✓ No copyleft licenses detected")
    lines.append("")

    constraint_lookup: dict[str, str] = {
        comp.get("name", "").lower(): get_prop(comp, "python:version-constraint")
        for comp in categories["python"]
        if get_prop(comp, "python:version-constraint")
    }

    pip_lic_rows = []
    if pip_licenses:
        pip_lic_rows = [{"name": p.get("Name", p.get("name", "")),
                         "version": p.get("Version", p.get("version", "")),
                         "license": p.get("License", "Unknown"),
                         "constraint": constraint_lookup.get(
                             p.get("Name", p.get("name", "")).lower(), "")} for p in pip_licenses]
    else:
        pip_lic_rows = [{"name": c.get("name", ""), "version": c.get("version", ""),
                         "license": get_license_str(c),
                         "constraint": get_prop(c, "python:version-constraint")}
                        for c in categories["python"]]
    pip_lic_rows.sort(key=lambda x: x["name"].lower())

    lines += [f"## Python Dependencies ({len(pip_lic_rows)} packages)", ""]
    lines += ["| Package | Version | Constraint | License |",
              "|---------|---------|------------|---------|"]
    for row in pip_lic_rows:
        lines.append(f"| {row['name']} | {row['version']} | {row['constraint'] or '—'} | {row['license']} |")
    lines.append("")

    # C++ dependencies — full list
    cpp_lic_rows = []
    for comp in sorted(categories["vcpkg"] + categories["submodule"] + categories["other"],
                       key=lambda c: c.get("name", "").lower()):
        cpp_lic_rows.append({
            "name": comp.get("name", ""),
            "version": comp.get("version", ""),
            "license": get_license_str(comp),
            "source": get_component_source(comp),
            "version_source": get_version_source(comp),
        })

    lines += [f"## C++ Dependencies ({len(cpp_lic_rows)} packages)", ""]
    lines += ["| Package | Version | Version Source | License | Source |",
              "|---------|---------|----------------|---------|--------|"]
    for row in cpp_lic_rows:
        lines.append(f"| {row['name']} | {row['version']} | {row['version_source'] or 'git-submodule'} | {row['license']} | {row['source']} |")
    lines.append("")

    # BSL-1.1 Compatibility Analysis
    lines += [
        "## BSL-1.1 License Compatibility Analysis",
        "",
        "ArcticDB is distributed under **Business Source License 1.1 (BSL-1.1)**.",
        "The table below classifies each dependency:",
        "- **✓ Permissive** — Compatible (MIT, BSD, Apache-2.0, BSL-1.0, etc.); attribution required",
        "- **⚠ Weak Copyleft** — Legal review required (LGPL requires relinking ability; MPL is file-level)",
        "- **✗ Strong Copyleft** — Incompatible with BSL-1.1 distribution (GPL, AGPL)",
        "- **? Unknown** — License not identified; manual review needed",
        "",
    ]

    # Group by compatibility for readability
    for compat_key, label in [("strong_copyleft", "✗ Strong Copyleft — INCOMPATIBLE"),
                               ("weak_copyleft", "⚠ Weak Copyleft — Legal Review Required"),
                               ("unknown", "? Unknown License — Manual Review Needed"),
                               ("permissive", "✓ Permissive — Compatible")]:
        group = [r for r in bsl_rows if r["compat"] == compat_key]
        if not group:
            continue
        lines += [f"### {label} ({len(group)} components)", ""]
        lines += ["| Package | Version | License | Source |", "|---------|---------|---------|--------|"]
        for row in group:
            lines.append(f"| {row['name']} | {row['version']} | {row['license']} | {row['source']} |")
        lines.append("")

    # Version Range Coverage
    if constraint_lookup:
        lines += [
            "## Version Range Coverage",
            "",
            "ArcticDB supports a range of versions for some Python dependencies. "
            "The table shows which boundary versions were included in the grype scan "
            "and whether any CVEs were found at those versions.",
            "",
            "| Package | Installed | Supported Range | Boundary Versions Scanned | CVEs (boundaries only) |",
            "|---------|-----------|-----------------|---------------------------|------------------------|",
        ]
        for comp in sorted(categories["python"], key=lambda c: c.get("name", "").lower()):
            constraint = get_prop(comp, "python:version-constraint")
            if not constraint:
                continue
            name = comp.get("name", "")
            installed_ver = comp.get("version", "")
            boundaries = generate_range_boundaries(name, constraint, installed_ver)
            boundary_versions = ", ".join(ver for ver, _ in boundaries) or "—"
            pkg_boundary_only = [
                f"{v['id']} @ {v['version']}"
                for v in boundary_only_vulns
                if v["package"].lower() == name.lower()
            ]
            cve_cell = "; ".join(pkg_boundary_only) if pkg_boundary_only else "✓ None"
            lines.append(f"| {name} | {installed_ver} | `{constraint}` | {boundary_versions} | {cve_cell} |")
        lines.append("")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    print(f"  Markdown report written to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate SBOM reports from enriched BOM and scan results")
    parser.add_argument("--bom", required=True, help="Path to enriched CycloneDX BOM JSON")
    parser.add_argument("--grype", help="Path to grype JSON scan results")
    parser.add_argument("--pip-licenses", help="Path to pip-licenses JSON output")
    parser.add_argument("--output-dir", required=True, help="Directory for report output")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print("\n[Report Generation] Loading data...")
    bom = load_json(args.bom)
    if not bom:
        print(f"ERROR: Could not load BOM from {args.bom}", file=sys.stderr)
        sys.exit(1)

    grype_data = load_json(args.grype)
    if grype_data:
        print(f"  Loaded grype results: {len(grype_data.get('matches', []))} matches")
    else:
        print("  No grype results (will show empty vulnerability table)")

    pip_licenses_raw = load_json(args.pip_licenses)
    pip_licenses = None
    if isinstance(pip_licenses_raw, list):
        pip_licenses = pip_licenses_raw
    elif isinstance(pip_licenses_raw, dict):
        pip_licenses = pip_licenses_raw.get("licenses", [])

    md_out = os.path.join(args.output_dir, "arcticdb-sbom-report.md")

    generate_markdown_report(bom, grype_data, pip_licenses, md_out)

    print(f"\n[Report Generation] Done!")
    print(f"  Markdown: {md_out}")


if __name__ == "__main__":
    main()
