"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.

Ratchet on the include dependencies between the C++ modules (the directories directly under cpp/arcticdb).

Every `#include "arcticdb/<module>/..."` (or `<arcticdb/<module>/...>`) in a production file of another module is an edge
`<from> -> <module>`. The set of edges must match build_tooling/cpp_layering_allowlist.txt exactly:
- an edge that is not in the allowlist fails, so no new module dependencies appear unnoticed;
- an allowlist line whose edge no longer exists fails, so the allowlist shrinks as the code is untangled.

Usage:
    python build_tooling/check_cpp_layering.py            # check
    python build_tooling/check_cpp_layering.py --update   # rewrite the allowlist from the code
"""

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = Path("cpp") / "arcticdb"
DEFAULT_ALLOWLIST = Path("build_tooling") / "cpp_layering_allowlist.txt"
SOURCE_SUFFIXES = {".cpp", ".cc", ".cxx", ".hpp", ".h", ".hxx", ".ipp", ".tpp"}
EXCLUDED_DIRS = {"test", "tests", "third_party", "vcpkg"}
INCLUDE_RE = re.compile(r'^\s*#\s*include\s*[<"]arcticdb/([A-Za-z0-9_]+)/')

ALLOWLIST_HEADER = """\
# Allowed include dependencies between the C++ modules under cpp/arcticdb, one `from -> to` per line.
# Checked by build_tooling/check_cpp_layering.py on every PR. Test and benchmark files are not counted.
#
# This is a ratchet. The target layering (see docs/claude/cpp/README.md) is
#   log < util < entity < codec < column_store < [stream+pipeline] < storage < async < processing < version
#       < toolbox < python
# and today's code does not follow it. The check fails if:
# - a new edge appears that is not listed here. Avoid adding one; prefer moving the code to the right module.
# - a listed edge no longer exists. Delete the line in the same PR as the change that removed the edge.
# Regenerate with: python build_tooling/check_cpp_layering.py --update
"""


def is_production_file(rel_path):
    if rel_path.suffix not in SOURCE_SUFFIXES:
        return False
    if any(part in EXCLUDED_DIRS for part in rel_path.parts[:-1]):
        return False
    return not rel_path.name.startswith("benchmark")


def collect_edges(repo_root):
    """Return {(from_module, to_module): [(path, line_number), ...]} for production files."""
    source_dir = repo_root / SOURCE_DIR
    edges = defaultdict(list)
    for path in sorted(source_dir.rglob("*")):
        rel = path.relative_to(source_dir)
        if len(rel.parts) < 2 or not path.is_file() or not is_production_file(rel):
            continue
        from_module = rel.parts[0]
        with open(path, encoding="utf-8", errors="replace") as f:
            for line_number, line in enumerate(f, 1):
                match = INCLUDE_RE.match(line)
                if match and match.group(1) != from_module:
                    edges[(from_module, match.group(1))].append((SOURCE_DIR / rel, line_number))
    return edges


def format_edge(edge):
    return f"{edge[0]} -> {edge[1]}"


def read_allowlist(path):
    allowed = set()
    for line_number, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("->")]
        if len(parts) != 2 or not all(parts):
            raise ValueError(f"{path}:{line_number}: expected 'from -> to', got {raw!r}")
        allowed.add((parts[0], parts[1]))
    return allowed


def write_allowlist(path, edges):
    path.write_text(ALLOWLIST_HEADER + "".join(format_edge(e) + "\n" for e in sorted(edges)))


def check(edges, allowed, allowlist_display):
    new_edges = sorted(set(edges) - allowed)
    stale_edges = sorted(allowed - set(edges))
    if new_edges:
        print(f"New C++ module dependencies that are not in {allowlist_display}:")
        for edge in new_edges:
            print(f"\n  {format_edge(edge)}")
            for path, line_number in edges[edge]:
                print(f"    {path.as_posix()}:{line_number}")
        print(
            "\nThese include a module from a module that did not depend on it before. Prefer moving the needed code to"
            " a lower module. If the dependency is intended, add the edge to the allowlist."
        )
    if stale_edges:
        if new_edges:
            print()
        print(f"Edges in {allowlist_display} that no longer exist in the code. Remove these lines:")
        for edge in stale_edges:
            print(f"  {format_edge(edge)}")
    if new_edges or stale_edges:
        print("\nRun `python build_tooling/check_cpp_layering.py --update` to regenerate the allowlist.")
        return 1
    print(f"C++ module layering OK: {len(edges)} module edges, all in {allowlist_display}.")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__[__doc__.index("Ratchet") :], formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="Repository root (default: %(default)s)")
    parser.add_argument("--allowlist", type=Path, help=f"Allowlist file (default: <root>/{DEFAULT_ALLOWLIST})")
    parser.add_argument("--update", action="store_true", help="Rewrite the allowlist from the current code")
    args = parser.parse_args(argv)

    allowlist = args.allowlist if args.allowlist is not None else args.root / DEFAULT_ALLOWLIST
    edges = collect_edges(args.root)
    if args.update:
        write_allowlist(allowlist, edges)
        print(f"Wrote {len(edges)} module edges to {allowlist}")
        return 0
    if not allowlist.exists():
        print(f"{allowlist} does not exist. Create it with --update.")
        return 1
    return check(edges, read_allowlist(allowlist), allowlist.name)


if __name__ == "__main__":
    sys.exit(main())
