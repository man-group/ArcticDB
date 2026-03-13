#!/usr/bin/env python3
"""
ArcticDB SBOM Report Generator
================================
Generates a combined HTML + Markdown report from:
  - Enriched CycloneDX BOM (arcticdb-bom-enriched.json)
  - grype vulnerability scan results (grype-vulns.json)
  - pip-licenses output (pip-licenses.json)  [optional]

The HTML report has tabs for:
  1. Summary (component counts, severity counts)
  2. Vulnerabilities (table with severity badges)
  3. Python Licenses
  4. C++ Licenses (vcpkg + submodules)
  5. Copyleft Flags (GPL/LGPL/AGPL components needing legal review)
"""

from __future__ import annotations

import json
import os
import sys
import argparse
from datetime import datetime


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
    """Split components by source type."""
    python_comps = []
    vcpkg_comps = []
    submodule_comps = []
    other_comps = []

    for comp in bom.get("components", []):
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


def build_source_lookup(bom: dict) -> dict[str, dict]:
    """Build a package-name → {source, version_source} map from the enriched BOM."""
    lookup: dict[str, dict] = {}
    for comp in bom.get("components", []):
        name = comp.get("name", "").lower()
        lookup[name] = {
            "source": get_component_source(comp),
            "version_source": get_version_source(comp),
        }
    return lookup


def parse_grype_results(grype_data: dict | None, source_lookup: dict | None = None) -> list:
    """Parse grype JSON output into flat vulnerability list."""
    if not grype_data:
        return []
    vulns = []
    for match in grype_data.get("matches", []):
        vuln = match.get("vulnerability", {})
        artifact = match.get("artifact", {})
        severity = vuln.get("severity", "unknown").lower()
        pkg_name = artifact.get("name", "")
        # Resolve source + version_source from BOM lookup; fall back to grype artifact type
        bom_entry = (source_lookup or {}).get(pkg_name.lower(), {})
        source = bom_entry.get("source", "")
        version_source = bom_entry.get("version_source", "")
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
        })
    # Sort by severity
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "negligible": 4, "unknown": 5}
    vulns.sort(key=lambda v: order.get(v["severity"], 5))
    return vulns


def count_severities(vulns: list) -> dict:
    counts = {"critical": 0, "high": 0, "medium": 0, "low": 0, "negligible": 0, "unknown": 0}
    for v in vulns:
        sev = v["severity"].lower()
        counts[sev] = counts.get(sev, 0) + 1
    return counts


def generate_html_report(bom: dict, grype_data: dict | None, pip_licenses: list | None, output_path: str):
    """Generate the combined HTML report."""
    categories = categorize_components(bom)
    vulns = parse_grype_results(grype_data, build_source_lookup(bom))
    sev_counts = count_severities(vulns)

    arcticdb_meta = bom.get("metadata", {}).get("component", {})
    arcticdb_version = arcticdb_meta.get("version", "unknown")
    scan_date = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    total_components = len(bom.get("components", []))

    # Find copyleft components
    copyleft_comps = []
    for comp in bom.get("components", []):
        lic_str = get_license_str(comp)
        for spdx in COPYLEFT_SPDX:
            if spdx.lower() in lic_str.lower():
                copyleft_comps.append((comp, lic_str))
                break

    # BSL compatibility analysis — classify all components
    bsl_all_rows = []
    bsl_counts: dict[str, int] = {"permissive": 0, "weak_copyleft": 0, "strong_copyleft": 0, "unknown": 0}
    for comp in sorted(bom.get("components", []), key=lambda c: c.get("name", "").lower()):
        lic_str = get_license_str(comp)
        compat = classify_bsl_compat(lic_str)
        bsl_counts[compat] = bsl_counts.get(compat, 0) + 1
        bsl_all_rows.append({
            "name": comp.get("name", ""),
            "version": comp.get("version", ""),
            "license": lic_str,
            "source": get_component_source(comp),
            "compat": compat,
        })

    # Build pip license table data (from pip-licenses JSON or BOM)
    pip_lic_rows = []
    if pip_licenses:
        for pkg in pip_licenses:
            pip_lic_rows.append({
                "name": pkg.get("Name", pkg.get("name", "")),
                "version": pkg.get("Version", pkg.get("version", "")),
                "license": pkg.get("License", pkg.get("license", "Unknown")),
            })
    else:
        for comp in categories["python"]:
            pip_lic_rows.append({
                "name": comp.get("name", ""),
                "version": comp.get("version", ""),
                "license": get_license_str(comp),
            })
    pip_lic_rows.sort(key=lambda x: x["name"].lower())

    # Build C++ license table data
    cpp_lic_rows = []
    for comp in sorted(categories["vcpkg"] + categories["submodule"] + categories["other"],
                       key=lambda c: c.get("name", "").lower()):
        source = get_component_source(comp)
        cpp_lic_rows.append({
            "name": comp.get("name", ""),
            "version": comp.get("version", ""),
            "license": get_license_str(comp),
            "source": source,
            "version_source": get_version_source(comp),
            "cpe": comp.get("cpe", ""),
        })

    def sev_badge(sev: str) -> str:
        color = SEVERITY_COLORS.get(sev.lower(), "#9e9e9e")
        return f'<span style="background:{color};color:white;padding:2px 8px;border-radius:3px;font-size:0.85em;font-weight:bold">{sev.upper()}</span>'

    def summary_card(label: str, value: str | int, color: str = "#1565c0") -> str:
        return f"""
        <div style="background:white;border-left:4px solid {color};padding:16px 20px;border-radius:4px;box-shadow:0 1px 3px rgba(0,0,0,0.1);min-width:150px">
            <div style="font-size:2em;font-weight:bold;color:{color}">{value}</div>
            <div style="color:#555;font-size:0.9em">{label}</div>
        </div>"""

    # Vulnerability rows
    SOURCE_BADGE_COLORS = {"pip": ("#1b5e20", "#e8f5e9"), "vcpkg": ("#4a148c", "#f3e5f5"),
                           "git-submodule": ("#e65100", "#fff3e0")}
    def source_badge(src: str) -> str:
        fg, bg = SOURCE_BADGE_COLORS.get(src, ("#555", "#f5f5f5"))
        return f'<span style="background:{bg};color:{fg};padding:1px 7px;border-radius:3px;font-size:0.8em;font-weight:600">{src}</span>'

    vuln_rows_html = ""
    if vulns:
        for v in vulns:
            url_links = " ".join(f'<a href="{u}" target="_blank" style="color:#1565c0">[link]</a>' for u in v["urls"])
            vs = v["version_source"]
            if vs == "vcpkg:override":
                vs_hint = '<br><span style="font-size:0.75em;color:#1b5e20">● pinned override</span>'
            elif vs.startswith("vcpkg:baseline"):
                commit = vs.split("(")[1].rstrip(")") if "(" in vs else ""
                vs_hint = f'<br><span style="font-size:0.75em;color:#5d4037" title="cpp/vcpkg.json builtin-baseline">● baseline {commit}</span>'
            else:
                vs_hint = ""
            vuln_rows_html += f"""
            <tr>
                <td><code>{v['id']}</code> {url_links}</td>
                <td>{sev_badge(v['severity'])}</td>
                <td><strong>{v['package']}</strong> {v['version']}{vs_hint}</td>
                <td>{source_badge(v['source'])}</td>
                <td><code>{v['fix_versions']}</code></td>
                <td style="font-size:0.85em;color:#555">{v['description']}</td>
            </tr>"""
    else:
        vuln_rows_html = '<tr><td colspan="6" style="text-align:center;color:green;padding:20px">✓ No vulnerabilities found</td></tr>'

    # pip license rows
    pip_lic_html = ""
    for row in pip_lic_rows:
        lic = row["license"]
        flag = " ⚠️" if any(c.lower() in lic.lower() for c in ["gpl", "lgpl", "agpl"]) else ""
        pip_lic_html += f"<tr><td>{row['name']}</td><td>{row['version']}</td><td>{lic}{flag}</td></tr>"

    # C++ license rows
    cpp_lic_html = ""
    for row in cpp_lic_rows:
        lic = row["license"]
        flag = " ⚠️" if any(c.lower() in lic.lower() for c in ["gpl", "lgpl", "agpl"]) else ""
        source_badge = f'<span style="background:#e3f2fd;color:#1565c0;padding:1px 6px;border-radius:3px;font-size:0.8em">{row["source"]}</span>'
        cpe_hint = f'<br><code style="font-size:0.75em;color:#888">{row["cpe"]}</code>' if row["cpe"] else ""
        vs = row["version_source"]
        if vs == "vcpkg:override":
            vs_hint = '<br><span style="font-size:0.75em;color:#1b5e20">● pinned override</span>'
        elif vs.startswith("vcpkg:baseline"):
            commit = vs.split("(")[1].rstrip(")") if "(" in vs else ""
            vs_hint = f'<br><span style="font-size:0.75em;color:#5d4037" title="cpp/vcpkg.json builtin-baseline">● baseline {commit}</span>'
        else:
            vs_hint = ""
        cpp_lic_html += f"<tr><td>{row['name']}{cpe_hint}</td><td>{row['version']}{vs_hint}</td><td>{lic}{flag}</td><td>{source_badge}</td></tr>"

    # Copyleft rows
    copyleft_html = ""
    if copyleft_comps:
        for comp, lic in copyleft_comps:
            source = get_component_source(comp)
            copyleft_html += f"""
            <tr style="background:#fff3e0">
                <td><strong>{comp.get('name', '')}</strong></td>
                <td>{comp.get('version', '')}</td>
                <td style="color:#e65100"><strong>{lic}</strong></td>
                <td>{source}</td>
                <td>⚠️ Legal review required</td>
            </tr>"""
    else:
        copyleft_html = '<tr><td colspan="5" style="text-align:center;color:green;padding:20px">✓ No copyleft licenses detected</td></tr>'

    # BSL compatibility rows
    bsl_rows_html = ""
    for row in bsl_all_rows:
        compat = row["compat"]
        color = BSL_COMPAT_COLOR.get(compat, "#757575")
        label = BSL_COMPAT_LABEL.get(compat, "? Unknown")
        compat_badge = f'<span style="color:{color};font-weight:bold">{label}</span>'
        row_bg = ""
        if compat == "strong_copyleft":
            row_bg = ' style="background:#ffebee"'
        elif compat == "weak_copyleft":
            row_bg = ' style="background:#fff8e1"'
        elif compat == "unknown":
            row_bg = ' style="background:#f5f5f5"'
        bsl_rows_html += f'<tr{row_bg}><td>{row["name"]}</td><td>{row["version"]}</td><td>{row["license"]}</td><td>{compat_badge}</td><td>{row["source"]}</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ArcticDB SBOM Report v{arcticdb_version}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin:0; padding:0; background:#f5f5f5; color:#333 }}
  .header {{ background:linear-gradient(135deg,#1a237e,#283593); color:white; padding:24px 32px }}
  .header h1 {{ margin:0 0 4px; font-size:1.8em }}
  .header p {{ margin:0; opacity:0.8; font-size:0.9em }}
  .summary-cards {{ display:flex; gap:16px; flex-wrap:wrap; padding:24px 32px; background:white; border-bottom:1px solid #e0e0e0 }}
  .tabs {{ display:flex; background:white; border-bottom:2px solid #e0e0e0; padding:0 32px }}
  .tab-btn {{ padding:12px 20px; cursor:pointer; border:none; background:none; font-size:0.95em; color:#555; border-bottom:3px solid transparent; margin-bottom:-2px; font-family:inherit }}
  .tab-btn.active {{ color:#1a237e; border-bottom-color:#1a237e; font-weight:600 }}
  .tab-btn:hover:not(.active) {{ color:#333; background:#f5f5f5 }}
  .tab-content {{ display:none; padding:24px 32px }}
  .tab-content.active {{ display:block }}
  table {{ width:100%; border-collapse:collapse; background:white; border-radius:4px; box-shadow:0 1px 3px rgba(0,0,0,0.1) }}
  th {{ background:#283593; color:white; padding:10px 14px; text-align:left; font-weight:600; font-size:0.9em }}
  td {{ padding:8px 14px; border-bottom:1px solid #e0e0e0; font-size:0.9em; vertical-align:top }}
  tr:last-child td {{ border-bottom:none }}
  tr:hover td {{ background:#fafafa }}
  .section-title {{ font-size:1.1em; font-weight:600; color:#283593; margin:0 0 12px }}
  code {{ background:#f5f5f5; padding:1px 4px; border-radius:3px; font-size:0.9em }}
</style>
</head>
<body>

<div class="header">
  <h1>ArcticDB SBOM Report</h1>
  <p>Version: <strong>{arcticdb_version}</strong> &nbsp;|&nbsp; Generated: {scan_date}</p>
</div>

<div class="summary-cards">
  {summary_card("Total Components", total_components)}
  {summary_card("Python (pip)", len(categories['python']), "#1b5e20")}
  {summary_card("C++ (vcpkg)", len(categories['vcpkg']), "#4a148c")}
  {summary_card("Submodules", len(categories['submodule']), "#e65100")}
  {summary_card("Vulnerabilities", len(vulns), "#b71c1c")}
  {summary_card("Critical", sev_counts['critical'], SEVERITY_COLORS['critical'])}
  {summary_card("High", sev_counts['high'], SEVERITY_COLORS['high'])}
  {summary_card("Medium", sev_counts['medium'], SEVERITY_COLORS['medium'])}
  {summary_card("Copyleft", len(copyleft_comps), "#e65100")}
  {summary_card("BSL: Permissive", bsl_counts['permissive'], "#2e7d32")}
  {summary_card("BSL: Weak Copyleft", bsl_counts['weak_copyleft'], "#e65100")}
  {summary_card("BSL: Strong Copyleft", bsl_counts['strong_copyleft'], "#b71c1c")}
  {summary_card("BSL: Unknown", bsl_counts['unknown'], "#757575")}
</div>

<div class="tabs">
  <button class="tab-btn active" onclick="showTab('vulns')">Vulnerabilities ({len(vulns)})</button>
  <button class="tab-btn" onclick="showTab('pylic')">Python Licenses ({len(pip_lic_rows)})</button>
  <button class="tab-btn" onclick="showTab('cpplic')">C++ Licenses ({len(cpp_lic_rows)})</button>
  <button class="tab-btn" onclick="showTab('copyleft')">Copyleft ({len(copyleft_comps)})</button>
  <button class="tab-btn" onclick="showTab('bslcompat')">BSL-1.1 Compatibility</button>
</div>

<div id="vulns" class="tab-content active">
  <div class="section-title">Vulnerability Findings</div>
  <table>
    <thead><tr>
      <th>CVE / ID</th><th>Severity</th><th>Package</th><th>Source</th><th>Fix Available</th><th>Description</th>
    </tr></thead>
    <tbody>{vuln_rows_html}</tbody>
  </table>
</div>

<div id="pylic" class="tab-content">
  <div class="section-title">Python Package Licenses</div>
  <table>
    <thead><tr><th>Package</th><th>Version</th><th>License</th></tr></thead>
    <tbody>{pip_lic_html}</tbody>
  </table>
</div>

<div id="cpplic" class="tab-content">
  <div class="section-title">C++ Library Licenses (vcpkg + submodules)</div>
  <table>
    <thead><tr><th>Package</th><th>Version</th><th>License</th><th>Source</th></tr></thead>
    <tbody>{cpp_lic_html}</tbody>
  </table>
</div>

<div id="copyleft" class="tab-content">
  <div class="section-title">Copyleft / Restrictive Licenses — Legal Review Required</div>
  <table>
    <thead><tr><th>Package</th><th>Version</th><th>License</th><th>Source</th><th>Action</th></tr></thead>
    <tbody>{copyleft_html}</tbody>
  </table>
</div>

<div id="bslcompat" class="tab-content">
  <div class="section-title">BSL-1.1 License Compatibility Analysis</div>
  <p style="margin:0 0 16px;color:#555;font-size:0.9em">
    ArcticDB is distributed under the <strong>Business Source License 1.1 (BSL-1.1)</strong>
    (Licensor: Man Group Operations Limited). This table classifies each dependency license
    for BSL-1.1 distribution compatibility.
    <br><br>
    <strong style="color:#2e7d32">✓ Permissive</strong> — Attribution required; no copyleft concerns (MIT, BSD, Apache-2.0, BSL-1.0, etc.).
    <strong style="color:#e65100">⚠ Weak Copyleft</strong> — Legal review required; LGPL requires ability to relink, MPL is file-level copyleft.
    <strong style="color:#b71c1c">✗ Strong Copyleft</strong> — Incompatible with BSL-1.1 distribution (GPL, AGPL).
    <strong style="color:#757575">? Unknown</strong> — License not identified; manual review needed.
  </p>
  <table>
    <thead><tr><th>Package</th><th>Version</th><th>License</th><th>BSL-1.1 Compat</th><th>Source</th></tr></thead>
    <tbody>{bsl_rows_html}</tbody>
  </table>
</div>

<script>
function showTab(id) {{
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  event.target.classList.add('active');
}}
</script>
</body>
</html>"""

    with open(output_path, "w") as f:
        f.write(html)
    print(f"  HTML report written to: {output_path}")


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

    # BSL compat counts for summary
    bsl_counts: dict[str, int] = {"permissive": 0, "weak_copyleft": 0, "strong_copyleft": 0, "unknown": 0}
    bsl_rows = []
    for comp in sorted(bom.get("components", []), key=lambda c: c.get("name", "").lower()):
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
        f"| Total components | {len(bom.get('components', []))} |",
        f"| Python (pip) | {len(categories['python'])} |",
        f"| C++ (vcpkg) | {len(categories['vcpkg'])} |",
        f"| Git submodules | {len(categories['submodule'])} |",
        f"| Total vulnerabilities | {len(vulns)} |",
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

    # Vulnerabilities — full list, no cap
    lines += ["## Vulnerabilities", ""]
    if vulns:
        lines += ["| CVE | Severity | Package | Version | Source | Version Source | Fix Available |",
                  "|-----|----------|---------|---------|--------|----------------|---------------|"]
        for v in vulns:
            lines.append(f"| {v['id']} | **{v['severity'].upper()}** | {v['package']} | {v['version']} | {v['source']} | {v['version_source'] or '-'} | {v['fix_versions']} |")
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

    # Python dependencies — full list
    pip_lic_rows = []
    if pip_licenses:
        pip_lic_rows = [{"name": p.get("Name", p.get("name", "")),
                         "version": p.get("Version", p.get("version", "")),
                         "license": p.get("License", "Unknown")} for p in pip_licenses]
    else:
        pip_lic_rows = [{"name": c.get("name", ""), "version": c.get("version", ""),
                         "license": get_license_str(c)} for c in categories["python"]]
    pip_lic_rows.sort(key=lambda x: x["name"].lower())

    lines += [f"## Python Dependencies ({len(pip_lic_rows)} packages)", ""]
    lines += ["| Package | Version | License |", "|---------|---------|---------|"]
    for row in pip_lic_rows:
        lines.append(f"| {row['name']} | {row['version']} | {row['license']} |")
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

    html_out = os.path.join(args.output_dir, "arcticdb-sbom-report.html")
    md_out = os.path.join(args.output_dir, "arcticdb-sbom-report.md")

    generate_html_report(bom, grype_data, pip_licenses, html_out)
    generate_markdown_report(bom, grype_data, pip_licenses, md_out)

    print(f"\n[Report Generation] Done!")
    print(f"  HTML: {html_out}")
    print(f"  Markdown: {md_out}")


if __name__ == "__main__":
    main()
