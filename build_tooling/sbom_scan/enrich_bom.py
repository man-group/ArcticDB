#!/usr/bin/env python3
"""
ArcticDB BOM Enrichment Script
================================
Builds a clean, authoritative CycloneDX BOM from:
  1. Python components from pip freeze output (requirements-frozen.txt) — exact installed versions
     Licensed data merged from pip-licenses JSON if provided.
  2. C++ components from vcpkg installed packages — versions from vcpkg.spdx.json, NOT ports tree.
     The vcpkg ports tree contains the latest port HEAD version, not the pinned/override version.
     Example: openssl ports/ HEAD = 3.6.1, but installed (and shipped) = 3.3.0 (pinned override).
  3. Git submodule dependencies with exact versions from `git submodule status`.

CPEs are added for known C++ libraries to enable grype vulnerability matching.
"""

from __future__ import annotations

import json
import os
import re
import sys
import glob
import subprocess
import argparse
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# CPE map: maps lowercase package name -> (vendor, product) for NVD CPE
# !Attention! Should be updated periodically
# ---------------------------------------------------------------------------
CPE_MAP = {
    # Crypto / TLS
    "openssl": ("openssl", "openssl"),
    # Networking
    "curl": ("haxx", "curl"),
    "libcurl": ("haxx", "curl"),
    "nghttp2": ("nghttp2", "nghttp2"),
    "c-ares": ("c-ares_project", "c-ares"),
    "libevent": ("libevent_project", "libevent"),
    # Compression
    "zlib": ("zlib", "zlib"),
    "lz4": ("lz4_project", "lz4"),
    "zstd": ("facebook", "zstd"),
    "bzip2": ("bzip", "bzip2"),
    "snappy": ("google", "snappy"),
    "liblzma": ("tukaani", "xz"),
    "xz-utils": ("tukaani", "xz"),
    # Serialization / encoding
    "protobuf": ("google", "protobuf"),
    "libprotobuf": ("google", "protobuf"),
    "flatbuffers": ("google", "flatbuffers"),
    "rapidjson": ("tencent", "rapidjson"),
    "nlohmann-json": ("nlohmann", "json"),
    # String / regex
    "pcre2": ("pcre", "pcre2"),
    "pcre": ("pcre", "pcre"),
    "double-conversion": ("google", "double-conversion"),
    "re2": ("google", "re2"),
    # Google libraries
    "abseil": ("google", "abseil-cpp"),
    "abseil-cpp": ("google", "abseil-cpp"),
    "grpc": ("grpc", "grpc"),
    "gflags": ("google", "gflags"),
    "glog": ("google", "glog"),
    "gtest": ("google", "googletest"),
    "benchmark": ("google", "benchmark"),
    # Logging / formatting
    "fmt": ("fmtlib", "fmt"),
    "spdlog": ("gabime", "spdlog"),
    # Database / storage
    "lmdb": ("openldap", "lmdb"),
    "mongo-c-driver": ("mongodb", "mongo-c-driver"),
    "mongo-cxx-driver": ("mongodb", "mongo-cxx-driver"),
    "libbson": ("mongodb", "libbson"),
    "rocksdb": ("facebook", "rocksdb"),
    "leveldb": ("google", "leveldb"),
    "sqlite3": ("sqlite", "sqlite"),
    # Arrow ecosystem
    "arrow": ("apache", "arrow"),
    # AWS SDK
    "aws-sdk-cpp": ("amazon", "aws-sdk-cpp"),
    "aws-crt-cpp": ("amazon", "aws-crt-cpp"),
    "aws-c-common": ("amazon", "aws-c-common"),
    "aws-c-io": ("amazon", "aws-c-io"),
    "aws-c-http": ("amazon", "aws-c-http"),
    "aws-c-auth": ("amazon", "aws-c-auth"),
    "aws-c-cal": ("amazon", "aws-c-cal"),
    "aws-c-mqtt": ("amazon", "aws-c-mqtt"),
    "aws-c-s3": ("amazon", "aws-c-s3"),
    "aws-c-event-stream": ("amazon", "aws-c-event-stream"),
    "aws-c-sdkutils": ("amazon", "aws-c-sdkutils"),
    "aws-checksums": ("amazon", "aws-checksums"),
    "s2n": ("amazon", "s2n-tls"),
    # Azure SDK
    "azure-core-cpp": ("microsoft", "azure-sdk-for-cpp"),
    "azure-identity-cpp": ("microsoft", "azure-sdk-for-cpp"),
    "azure-storage-blobs-cpp": ("microsoft", "azure-sdk-for-cpp"),
    # Python bindings / header-only
    "pybind11": ("pybind11_project", "pybind11"),
    # Hash / math
    "xxhash": ("cyan4973", "xxhash"),
    # System
    "libxml2": ("xmlsoft", "libxml2"),
    "libiconv": ("gnu", "libiconv"),
    "libuv": ("libuv", "libuv"),
    # Monitoring
    "prometheus-cpp": ("jupp0r", "prometheus-cpp"),
    # Folly
    "folly": ("meta", "folly"),
}

# ---------------------------------------------------------------------------
# License detection patterns (ordered by specificity)
# !Attention! Should be updated periodically
# ---------------------------------------------------------------------------
LICENSE_PATTERNS = [
    # Apache — match across the line break vcpkg copyright files insert (requires re.DOTALL)
    (r"Apache License.*Version 2", "Apache-2.0"),
    (r"Apache-2\.0", "Apache-2.0"),
    # MIT — match both "MIT License" header and bare license text (no header)
    (r"MIT License", "MIT"),
    (r"\bMIT\b", "MIT"),
    # MIT license body text: used by fmt, libxml2, brotli and many others that omit the header
    (r"Permission is hereby granted, free of charge, to any person obtaining", "MIT"),
    # Boost
    (r"Boost Software License.*1\.0", "BSL-1.0"),
    (r"BSL-1\.0", "BSL-1.0"),
    # BSD variants
    (r"BSD 3-Clause", "BSD-3-Clause"),
    (r"BSD-3-Clause", "BSD-3-Clause"),
    # BSD-3 boilerplate — "with or without" may wrap to next line in vcpkg copyright files
    (r"Redistribution and use in source and binary forms,?\s+with or without", "BSD-3-Clause"),
    (r"BSD 2-Clause", "BSD-2-Clause"),
    (r"BSD-2-Clause", "BSD-2-Clause"),
    # OpenSSL (legacy, pre Apache-2.0 relicensing)
    (r"OpenSSL License", "OpenSSL"),
    # ISC
    (r"ISC License", "ISC"),
    (r"\bISC\b", "ISC"),
    # GNU licenses
    (r"GNU Lesser General Public License.*version 2\.1", "LGPL-2.1"),
    (r"GNU Lesser General Public License.*version 3", "LGPL-3.0"),
    (r"GNU General Public License.*version 2", "GPL-2.0"),
    (r"GNU General Public License.*version 3", "GPL-3.0"),
    (r"GNU Affero General Public License.*version 3", "AGPL-3.0"),
    # Mozilla
    (r"Mozilla Public License.*2\.0", "MPL-2.0"),
    # Zlib — match "as-is" + "implied warranty" (the Zlib boilerplate); must come AFTER MIT
    # because MIT also has "as-is" — Zlib is distinguished by NOT having "free of charge"
    (r"zlib License", "Zlib"),
    # Zlib license body — "implied" may wrap; match "as-is" + "implied warranty" with any whitespace
    (r"This software is provided 'as-is'.*implied\s+warranty", "Zlib"),
    # curl
    (r"curl License", "curl"),
    # LDAP
    (r"The OpenLDAP Public License", "OLDAP-2.8"),
    # Public domain
    (r"Public Domain", "Unlicense"),
]

COPYLEFT_LICENSES = {"GPL-2.0", "GPL-3.0", "LGPL-2.1", "LGPL-3.0", "AGPL-3.0", "MPL-2.0", "CDDL-1.0"}

# ---------------------------------------------------------------------------
# Fallback license map: known licenses for packages whose copyright files
# do not contain parseable license text (e.g. "see LICENCE file" stubs).
# Keys are lowercase package names.
# !Attention! Should be reviewed and updated periodically
# ---------------------------------------------------------------------------
KNOWN_LICENSES_FALLBACK: dict[str, str] = {
    # vcpkg C++ packages
    "zstd": "BSD-3-Clause",           # dual-licensed BSD-3-Clause + GPLv2; vcpkg ships BSD
    "pcre2": "BSD-3-Clause",          # PCRE2 uses a BSD-style license
    "bzip2": "bzip2-1.0.6",          # bzip2/libbzip2 license (BSD-like, Seward)
    "libiconv": "LGPL-2.1",           # GNU libiconv is LGPL-2.1
    "boost-uninstall": "BSL-1.0",     # Boost software — BSL-1.0
    "xsimd": "BSD-3-Clause",          # xsimd BSD-3-Clause (confirmed from copyright)
    "re2": "BSD-3-Clause",            # Google RE2 BSD-3-Clause
    "gflags": "BSD-3-Clause",         # Google gflags — BSD-3-Clause
    "glog": "BSD-3-Clause",           # Google glog — BSD-3-Clause
    "gtest": "BSD-3-Clause",          # Google Test — BSD-3-Clause (pre-1.11; >=1.11 is Apache-2.0)
    "double-conversion": "BSD-3-Clause",  # Google double-conversion — BSD-3-Clause
    # pip packages not covered by pip-licenses scan
    "pyyaml": "MIT",
    "attrs": "MIT",
    "dataclasses": "MIT",             # backport of Python 3.7 dataclasses; MIT
    "msgpack": "Apache-2.0",
    "protobuf": "BSD-3-Clause",       # Python + C++ protobuf both BSD-3-Clause
}


def detect_license_from_text(text: str) -> str | None:
    """Detect SPDX license ID from copyright/license file text.

    Uses re.DOTALL so patterns like 'Apache License.*Version 2' match across
    the line break that vcpkg copyright files typically insert between those words.
    """
    for pattern, spdx_id in LICENSE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE | re.DOTALL):
            return spdx_id
    return None


def resolve_runtime_deps(setup_cfg_path: str, python_exe: str = sys.executable) -> dict[str, str]:
    """
    Resolve the full transitive closure of ArcticDB's runtime dependencies.

    Reads direct deps from setup.cfg [options] install_requires, then walks
    the dependency tree via `pip show` to find every package that ships with
    arcticdb at runtime.  Returns {canonical_name: installed_version}.

    This is the source-of-truth filter: only packages reachable from
    install_requires end up in the BOM, regardless of what else is installed
    in the current environment (dev tools, test frameworks, etc.).
    """
    import configparser

    cfg = configparser.ConfigParser()
    cfg.read(setup_cfg_path)
    raw = cfg.get("options", "install_requires", fallback="")
    direct: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Strip version specifiers, extras, and inline comments
        name = re.split(r"[>=<!#\s\[]", line)[0].strip()
        if name:
            direct.append(name)

    if not direct:
        print("  Warning: no install_requires found in setup.cfg", file=sys.stderr)
        return {}

    print(f"  Direct runtime deps from setup.cfg ({len(direct)}): {', '.join(direct)}")

    resolved: dict[str, str] = {}   # canonical_name -> version
    queue: list[str] = list(direct)
    visited: set[str] = set()

    while queue:
        pkg = queue.pop(0)
        key = re.sub(r"[-_.]", "_", pkg.lower())
        if key in visited:
            continue
        visited.add(key)

        result = subprocess.run(
            [python_exe, "-m", "pip", "show", pkg],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  Warning: pip show {pkg!r} failed — skipping", file=sys.stderr)
            continue

        version = ""
        requires: list[str] = []
        for line in result.stdout.splitlines():
            if line.startswith("Name:"):
                pkg = line.split(":", 1)[1].strip()       # canonical capitalisation
            elif line.startswith("Version:"):
                version = line.split(":", 1)[1].strip()
            elif line.startswith("Requires:"):
                req_str = line.split(":", 1)[1].strip()
                if req_str:
                    requires = [r.strip() for r in req_str.split(",") if r.strip()]

        if version:
            resolved[pkg] = version
            queue.extend(requires)

    print(f"  Resolved {len(resolved)} runtime packages (direct + transitive)")
    return resolved


def parse_requirements_frozen(reqs_path: str, pip_licenses: list | None = None) -> list:
    """
    Parse pip freeze output into BOM components with exact installed versions.

    This is the authoritative Python source — unlike cdxgen which reads version range
    constraints from pyproject.toml and creates dozens of entries per package, pip freeze
    gives a single exact installed version per package.

    Editable installs (-e git+...) are included with version "dev" and the git hash.
    pip-licenses data (if provided) is used to enrich with license information.
    """
    if not reqs_path or not os.path.exists(reqs_path):
        return []

    # Build license lookup from pip-licenses JSON
    license_map: dict[str, str] = {}
    if pip_licenses:
        for pkg in pip_licenses:
            name = pkg.get("Name") or pkg.get("name") or ""
            lic = pkg.get("License") or pkg.get("license") or ""
            if name and lic and lic.lower() not in ("unknown", ""):
                license_map[name.lower()] = lic

    components = []
    with open(reqs_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("-r "):
                continue

            name = ""
            version = ""
            is_editable = False

            if line.startswith("-e "):
                # Editable install: -e git+https://...@<hash>#egg=<name>
                is_editable = True
                m = re.search(r"#egg=([A-Za-z0-9_.-]+)", line)
                if m:
                    name = m.group(1)
                # Try to extract git hash as version
                m_hash = re.search(r"@([0-9a-f]{7,40})", line)
                version = m_hash.group(1)[:12] if m_hash else "dev"
            elif "==" in line:
                # Standard pinned: package==version (or package==version+local)
                name, version = line.split("==", 1)
                name = name.strip()
                version = version.strip()
            elif " @ " in line:
                # PEP 440 URL requirement: package @ https://...
                name = line.split(" @ ")[0].strip()
                version = "unknown"
            else:
                # No version pinned (shouldn't happen in pip freeze output)
                name = line.split("[")[0].strip()
                version = "unknown"

            if not name:
                continue

            purl = f"pkg:pypi/{name.lower()}@{version}"
            comp: dict = {
                "type": "library",
                "name": name,
                "version": version,
                "purl": purl,
                "bom-ref": purl,
                "properties": [{"name": "cdx:build:source", "value": "pip"}],
            }

            if is_editable:
                comp["properties"].append({"name": "pip:editable", "value": "true"})
    from collections import deque
    resolved: dict[str, str] = {}
    queue: deque[str] = deque(direct)
    visited: set[str] = set()

    while queue:
        pkg = queue.popleft()
            lic = license_map.get(name.lower())
            if not lic:
                lic = KNOWN_LICENSES_FALLBACK.get(name.lower())
            if lic:
                comp["licenses"] = [{"license": {"name": lic}}]

            components.append(comp)

    return components


def get_submodule_versions(arcticdb_root: str) -> dict:
    """Read git submodule status to get actual pinned versions."""
    submodules = {}
    try:
        result = subprocess.run(
            ["git", "submodule", "status"],
            capture_output=True, text=True, cwd=arcticdb_root
        )
        # Format: <hash> <path> (<describe>)
        for line in result.stdout.splitlines():
            m = re.match(r"[\s+-]?([0-9a-f]+)\s+(\S+)\s+\((.+)\)", line.strip())
            if m:
                commit_hash, path, describe = m.group(1), m.group(2), m.group(3)
                name = os.path.basename(path)
                # Extract clean version: "v3.13.2" -> "3.13.2", "LMDB_0.9.22" -> "0.9.22"
                version = re.sub(r"^[vV]", "", describe.split("-")[0])
                version = re.sub(r"^LMDB_", "", version)
                requires = [re.split(r"[\[;]", r.strip())[0].strip()
                            for r in req_str.split(",") if r.strip()]
    except Exception as e:
        print(f"  Warning: could not read git submodule status: {e}", file=sys.stderr)
    return submodules


def load_vcpkg_overrides(arcticdb_root: str) -> tuple[dict, str]:
    """Load pinned versions from vcpkg.json overrides and the builtin-baseline commit hash.

    Returns:
        (overrides dict {name_lower: version}, baseline_commit str)
    """
    vcpkg_json = os.path.join(arcticdb_root, "cpp", "vcpkg.json")
    overrides = {}
    baseline = ""
    try:
        with open(vcpkg_json) as f:
            data = json.load(f)
        for override in data.get("overrides", []):
            name = override.get("name", "").lower()
            version = override.get("version") or override.get("version-string", "")
            # Strip build metadata like "3.21.8#1"
            version = re.split(r"#", version)[0]
            if name and version:
                overrides[name] = version
        baseline = data.get("builtin-baseline", "")
    except Exception as e:
        print(f"  Warning: could not load vcpkg.json overrides: {e}", file=sys.stderr)
    return overrides, baseline


def read_vcpkg_installed_packages(vcpkg_share_dir: str, vcpkg_overrides: dict,
                                  builtin_baseline: str = "") -> list:
    """
    Read actually-installed vcpkg packages from vcpkg_installed/x64-linux/share/*/vcpkg.spdx.json.

    This is the KEY FIX: cdxgen's C++ scanner traverses the entire vcpkg ports tree
    (including portfiles for every dependency of every package ever listed), producing
    1000+ versionless junk components. By reading from vcpkg_installed/ instead, we get
    only the packages that were actually installed with their correct versions.
    """
    packages = []
    if not os.path.isdir(vcpkg_share_dir):
        print(f"  Warning: vcpkg share dir not found: {vcpkg_share_dir}", file=sys.stderr)
        print("  C++ packages will come only from cdxgen output (may have version issues).", file=sys.stderr)
        return packages

    spdx_files = sorted(glob.glob(os.path.join(vcpkg_share_dir, "*", "vcpkg.spdx.json")))
    print(f"  Found {len(spdx_files)} vcpkg.spdx.json files in {vcpkg_share_dir}")

    for spdx_file in spdx_files:
        pkg_name = os.path.basename(os.path.dirname(spdx_file))
        # Skip debug/tool packages
        if pkg_name.endswith("-debug") or pkg_name in ("vcpkg-cmake", "vcpkg-cmake-config",
                                                         "vcpkg-pkgconfig-get-modules"):
            continue

        version = None
        version_source = None
        try:
            with open(spdx_file) as f:
                spdx = json.load(f)
            for pkg in spdx.get("packages", []):
                v = pkg.get("versionInfo", "")
                if not v or v == "unknown":
                    continue
                # Skip hash-only versions (git commit hashes)
                if re.match(r"^[0-9a-f]{7,40}$", v):
                    continue
                version = re.split(r"#", v)[0]  # Strip build metadata
                break
        except Exception:
            pass

        # Classify version source: explicit override vs builtin-baseline
        if version:
            if pkg_name.lower() in vcpkg_overrides:
                version_source = "vcpkg:override"
            else:
                version_source = f"vcpkg:baseline({builtin_baseline[:8]})" if builtin_baseline else "vcpkg:baseline"
        else:
            # Fallback to vcpkg.json overrides if spdx doesn't have a clean version
            version = vcpkg_overrides.get(pkg_name.lower())
            if version:
                version_source = "vcpkg:override"

        if not version:
            continue  # Skip packages with no identifiable version

        # Detect license from copyright file
        license_id = None
        copyright_file = os.path.join(os.path.dirname(spdx_file), "copyright")
        if os.path.exists(copyright_file):
            try:
                with open(copyright_file, errors="replace") as f:
                    content = f.read()
                license_id = detect_license_from_text(content)
            except Exception:
                pass
        # Fallback to known licenses map for packages where copyright file text is
        # insufficient (e.g. "see LICENCE file" stubs)
        if not license_id:
            license_id = KNOWN_LICENSES_FALLBACK.get(pkg_name.lower())

        # Build the component
        purl = f"pkg:generic/{pkg_name}@{version}"
        props = [{"name": "cdx:build:source", "value": "vcpkg"}]
        if version_source:
            props.append({"name": "vcpkg:version-source", "value": version_source})
        comp = {
            "type": "library",
            "name": pkg_name,
            "version": version,
            "purl": purl,
            "bom-ref": purl,
            "description": f"{pkg_name} (C++ via vcpkg)",
            "properties": props,
        }

        if license_id:
            comp["licenses"] = [{"license": {"id": license_id}}]

        # Add CPE for grype vulnerability matching
        pkg_lower = pkg_name.lower()
        if pkg_lower in CPE_MAP:
            vendor, product = CPE_MAP[pkg_lower]
            comp["cpe"] = f"cpe:2.3:a:{vendor}:{product}:{version}:*:*:*:*:*:*:*"
        elif pkg_lower.startswith("boost-") or pkg_lower == "boost":
            comp["cpe"] = f"cpe:2.3:a:boost:boost:{version}:*:*:*:*:*:*:*"
        elif pkg_lower.startswith("aws-"):
            comp["cpe"] = f"cpe:2.3:a:amazon:{pkg_lower}:{version}:*:*:*:*:*:*:*"
        elif pkg_lower.startswith("azure-"):
            comp["cpe"] = f"cpe:2.3:a:microsoft:{pkg_lower}:{version}:*:*:*:*:*:*:*"

        packages.append(comp)

    return packages


def build_submodule_components(submodule_versions: dict) -> list:
    """Build BOM components for git submodule dependencies."""
    # Static metadata for known submodules
    SUBMODULE_META = {
        "pybind11": {
            "cpe_vendor": "pybind11_project",
            "cpe_product": "pybind11",
            "license": "BSD-3-Clause",
            "description": "Python C++ bindings (git submodule)",
        },
        "entt": {
            "cpe_vendor": None,
            "cpe_product": None,
            "license": "MIT",
            "description": "Entity Component System (git submodule)",
        },
        "lmdb": {
            "cpe_vendor": "openldap",
            "cpe_product": "lmdb",
            "license": "OLDAP-2.8",
            "description": "Lightning Memory-Mapped Database (git submodule)",
        },
        "lmdbxx": {
            "cpe_vendor": None,
            "cpe_product": None,
            "license": "MIT",
            "description": "C++ wrapper for LMDB (git submodule)",
        },
        "rapidcheck": {
            "cpe_vendor": None,
            "cpe_product": None,
            "license": "BSD-2-Clause",
            "description": "Property-based testing (git submodule, test-only)",
        },
        "recycle": {
            "cpe_vendor": None,
            "cpe_product": None,
            "license": "BSD-3-Clause",
            "description": "Memory recycling (git submodule)",
        },
    }

    components = []
    for name, info in submodule_versions.items():
        if name == "vcpkg":
            continue  # vcpkg itself is a build tool, not a shipped dep
        version = info["version"]
        if not version:
            version = info.get("hash", "unknown")[:12]

        meta = SUBMODULE_META.get(name, {})
        purl = f"pkg:generic/{name}@{version}"
        comp = {
            "type": "library",
            "name": name,
            "version": version,
            "purl": purl,
            "bom-ref": purl,
            "description": meta.get("description", f"{name} (git submodule)"),
            "properties": [
                {"name": "cdx:build:source", "value": "git-submodule"},
                {"name": "git:describe", "value": info.get("describe", "")},
                {"name": "git:commit", "value": info.get("hash", "")},
            ],
        }

        license_id = meta.get("license")
        if license_id:
            comp["licenses"] = [{"license": {"id": license_id}}]

        vendor = meta.get("cpe_vendor")
        product = meta.get("cpe_product")
        if vendor and product:
            comp["cpe"] = f"cpe:2.3:a:{vendor}:{product}:{version}:*:*:*:*:*:*:*"

        components.append(comp)

    return components


def enrich_bom(
    python_bom_path: str,
    cpp_bom_path: str | None,
    vcpkg_share_dir: str,
    arcticdb_root: str,
    output_path: str,
    arcticdb_version: str = "unknown",
    requirements_frozen_path: str | None = None,
    pip_licenses_path: str | None = None,
    setup_cfg_path: str | None = None,
    product_python: str | None = None,
):
    """Build the enriched BOM from all sources."""
    print("\n[BOM Enrichment] Starting...")

    # Load pip-licenses data for license enrichment
    pip_licenses_data = None
    if pip_licenses_path and os.path.exists(pip_licenses_path):
        try:
            with open(pip_licenses_path) as f:
                raw = json.load(f)
            pip_licenses_data = raw if isinstance(raw, list) else raw.get("licenses", [])
            print(f"  Loaded pip-licenses: {len(pip_licenses_data)} packages")
        except Exception as e:
            print(f"  Warning: could not load pip-licenses: {e}", file=sys.stderr)

    cpp_bom = None
    if cpp_bom_path and os.path.exists(cpp_bom_path):
        print(f"  Loading C++ BOM: {cpp_bom_path}")
        with open(cpp_bom_path) as f:
            cpp_bom = json.load(f)
    else:
        print("  No C++ BOM provided, relying on vcpkg installed packages only")

    components = []
    seen = set()  # (name_lower, version) tuples to deduplicate

    # ----- Resolve runtime dependency filter from setup.cfg -----
    # setup.cfg install_requires is the source of truth for what arcticdb ships.
    # We resolve the full transitive closure so only runtime packages appear in
    # the BOM — dev/test packages (pytest, coverage, boto3, etc.) are excluded.
    runtime_filter: dict[str, str] | None = None
    if setup_cfg_path and os.path.exists(setup_cfg_path):
        py_exe = product_python or sys.executable
        print(f"  Resolving runtime deps from: {setup_cfg_path}")
        runtime_filter = resolve_runtime_deps(setup_cfg_path, py_exe)
        if not runtime_filter:
            print("  Warning: runtime dep resolution returned nothing — including all pip packages", file=sys.stderr)
            runtime_filter = None
    else:
        print("  No --setup-cfg provided — including all pip freeze packages")

    def _normalise(name: str) -> str:
        return re.sub(r"[-_.]", "_", name.lower())

    # ----- Python components from pip freeze (authoritative) -----
    # We do NOT use cdxgen's Python BOM here because cdxgen reads version range
    # constraints from pyproject.toml, creating dozens of entries per package
    # (e.g. numpy 1.18, 1.20, 1.22, 1.24, 1.26, 2.0, latest).
    # pip freeze gives a single exact installed version per package.
    python_count = 0
    if requirements_frozen_path and os.path.exists(requirements_frozen_path):
        print(f"  Python packages from pip freeze: {requirements_frozen_path}")
        pip_comps = parse_requirements_frozen(requirements_frozen_path, pip_licenses_data)
        skipped = []
        for comp in pip_comps:
            if runtime_filter is not None:
                if _normalise(comp["name"]) not in {_normalise(k) for k in runtime_filter}:
                    skipped.append(comp["name"])
                    continue
            key = (comp["name"].lower(), comp["version"])
            if key in seen:
                continue
            seen.add(key)
            components.append(comp)
            python_count += 1
        if skipped:
            print(f"  Excluded {len(skipped)} non-runtime pip packages (dev/test deps)")
        print(f"  Python components (from pip freeze, runtime only): {python_count}")
    else:
        # Fallback: use cdxgen Python BOM but only take versioned, non-duplicate entries
        print(f"  Loading Python BOM (cdxgen fallback): {python_bom_path}")
        with open(python_bom_path) as f:
            python_bom = json.load(f)
        for comp in python_bom.get("components", []):
            name = comp.get("name", "").lower()
            version = comp.get("version", "")
            if not version or version in ("", "latest", "unknown"):
                continue
            # Skip single-digit versions (from version range specs like ">=3") and hash versions
            if re.match(r"^[0-9a-f]{7,40}$", version) or re.match(r"^\d+$", version):
                continue
            key = (name, version)
            if key in seen:
                continue
            seen.add(key)
            comp.setdefault("properties", [])
            if not any(p.get("name") == "cdx:build:source" for p in comp["properties"]):
                comp["properties"].append({"name": "cdx:build:source", "value": "pip"})
            components.append(comp)
            python_count += 1
        print(f"  Python components (cdxgen fallback): {python_count}")

    # cdxgen C++ BOM is intentionally NOT used as a component source.
    #
    # cdxgen scans the entire vcpkg ports tree (thousands of find_package() references
    # across all ports, not just installed ones), producing entries like Doxygen, SDL2_gfx,
    # nana, winpty, libodb-*, jack — none of which are ArcticDB dependencies.
    # Even with aggressive filtering, the noise cannot be reliably separated from
    # genuine deps because cdxgen has no concept of "installed vs available".
    #
    # vcpkg_installed/ is the authoritative and complete C++ source: it contains exactly
    # the packages that were compiled and linked into the build, with correct versions.
    # Git submodules cover the remaining header-only / vendored deps.
    if cpp_bom:
        n = len(cpp_bom.get("components", []))
        print(f"  C++ cdxgen BOM loaded ({n} raw components) — NOT used as component source")
        print(f"  (cdxgen scans the full vcpkg ports tree, not just installed packages)")
    else:
        print(f"  No C++ cdxgen BOM provided")

    # ----- vcpkg installed packages (the authoritative C++ source) -----
    vcpkg_overrides, builtin_baseline = load_vcpkg_overrides(arcticdb_root)
    print(f"  vcpkg builtin-baseline: {builtin_baseline or '(unknown)'}")
    print(f"  vcpkg explicit overrides: {len(vcpkg_overrides)} packages")
    vcpkg_packages = read_vcpkg_installed_packages(vcpkg_share_dir, vcpkg_overrides, builtin_baseline)
    vcpkg_added = 0
    for comp in vcpkg_packages:
        key = (comp["name"].lower(), comp["version"])
        if key in seen:
            continue
        seen.add(key)
        components.append(comp)
        vcpkg_added += 1
    print(f"  vcpkg installed packages added: {vcpkg_added}")

    # ----- Git submodule dependencies -----
    # Note: some submodules (e.g. rapidcheck) are also listed in vcpkg.json and
    # installed via vcpkg. In that case, prefer the vcpkg version (the one actually
    # compiled and linked), and skip the submodule entry to avoid duplicates.
    submodule_versions = get_submodule_versions(arcticdb_root)
    submodule_components = build_submodule_components(submodule_versions)
    submodule_added = 0
    for comp in submodule_components:
        name_lower = comp["name"].lower()
        version = comp["version"]
        key = (name_lower, version)
        # Skip if this submodule is already covered by vcpkg (any version of same name)
        already_in_vcpkg = any(
            c.get("name", "").lower() == name_lower
            and any(p.get("value") == "vcpkg" for p in c.get("properties", []))
            for c in components
        )
        if already_in_vcpkg:
            print(f"  Skipping submodule {comp['name']} (already in vcpkg-installed)")
            continue
        if key in seen:
            continue
        seen.add(key)
        components.append(comp)
        submodule_added += 1
    print(f"  Git submodule components added: {submodule_added}")
    # Remove duplicate arcticdb entries — it's the scanned product (listed in metadata),
    components = [c for c in components if c.get("name", "").lower() != product_name]
    product_name = "arcticdb"
    components = [
        c for c in components
        if c.get("name", "").lower() != product_name
        or any(p.get("value") == "pip" and not any(
            runtime_normalised = {_normalise(k) for k in runtime_filter}
        for comp in pip_comps:
            if runtime_filter is not None:
                if _normalise(comp["name"]) not in runtime_normalised:
        ) for p in c.get("properties", []))
    ]
    # Simpler: just remove all "arcticdb" entries from components entirely;
    # it belongs in metadata.component only.
    components = [c for c in components if c.get("name", "").lower() != product_name]
    print(f"  TOTAL components in enriched BOM: {len(components)}")
    print(f"  (arcticdb removed from components — it is the BOM subject in metadata)")

    # ----- Assemble final BOM -----
    enriched_bom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{_generate_uuid()}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tools": [
                {"vendor": "Anthropic / Man Group", "name": "arcticdb-sbom", "version": "1.0"}
            ],
            "component": {
                "type": "application",
                "name": "arcticdb",
                "version": arcticdb_version,
                "purl": f"pkg:pypi/arcticdb@{arcticdb_version}",
                "description": "High-performance serverless DataFrame database",
            },
        },
        "components": components,
    }

    output_dir = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(enriched_bom, f, indent=2)
    print(f"  Enriched BOM written to: {output_path}")
    return enriched_bom


def _generate_uuid() -> str:
    """Generate a simple UUID v4."""
    import uuid
    return str(uuid.uuid4())


def main():
    parser = argparse.ArgumentParser(description="Enrich ArcticDB SBOM with correct vcpkg versions and CPEs")
    parser.add_argument("--python-bom", help="Path to cdxgen Python BOM JSON (fallback if no --requirements-frozen)")
    parser.add_argument("--cpp-bom", help="Path to cdxgen C++ BOM JSON (optional)")
    parser.add_argument("--requirements-frozen",
                        help="Path to pip freeze output (requirements-frozen.txt). "
                             "Preferred over --python-bom for accurate Python package versions.")
    parser.add_argument("--setup-cfg",
                        help="Path to ArcticDB setup.cfg. When provided, only packages in "
                             "install_requires (direct + transitive) are included in the BOM. "
                             "Excludes dev/test packages from the environment.")
    parser.add_argument("--product-python",
                        help="Path to the product Python executable for pip show resolution "
                             "(default: current interpreter)")
    parser.add_argument("--pip-licenses",
                        help="Path to pip-licenses JSON output for Python license enrichment")
    parser.add_argument("--vcpkg-share", required=True,
                        help="Path to vcpkg_installed/x64-linux/share")
    parser.add_argument("--arcticdb-root", default=".", help="ArcticDB repository root")
    parser.add_argument("--output", required=True, help="Output path for enriched BOM JSON")
    parser.add_argument("--arcticdb-version", default="unknown", help="ArcticDB version being scanned")
    args = parser.parse_args()

    if not args.python_bom and not args.requirements_frozen:
        parser.error("Provide either --python-bom or --requirements-frozen")

    enrich_bom(
        python_bom_path=args.python_bom or "/dev/null",
        cpp_bom_path=args.cpp_bom,
        vcpkg_share_dir=args.vcpkg_share,
        arcticdb_root=args.arcticdb_root,
        output_path=args.output,
        arcticdb_version=args.arcticdb_version,
        requirements_frozen_path=args.requirements_frozen,
        pip_licenses_path=args.pip_licenses,
        setup_cfg_path=args.setup_cfg,
        product_python=args.product_python,
    )


if __name__ == "__main__":
    main()
