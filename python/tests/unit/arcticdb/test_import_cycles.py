"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import ast
import pathlib
from collections import defaultdict

# Static analysis only - deliberately does not import arcticdb, so this runs without a built arcticdb_ext.
_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[3] / "arcticdb"


def _module_name(path):
    return ".".join(path.relative_to(_PACKAGE_ROOT.parent).with_suffix("").parts)


def _iter_modules():
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        yield path, ast.parse(path.read_text(encoding="utf-8"))


def _imported_first_party(node):
    """The arcticdb.* modules an import node refers to. arcticdb_ext is the C++ extension, not first party Python."""
    if isinstance(node, ast.ImportFrom):
        names = [node.module] if node.module else []
    else:
        names = [alias.name for alias in node.names]
    return [n for n in names if n == "arcticdb" or n.startswith("arcticdb.")]


def test_no_module_level_import_cycles():
    graph = defaultdict(set)
    for path, tree in _iter_modules():
        source = _module_name(path)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                graph[source].update(_imported_first_party(node))

    cycles = []
    visiting = []
    visited = set()

    def visit(module):
        visited.add(module)
        visiting.append(module)
        for target in sorted(graph.get(module, ())):
            if target in visiting:
                cycles.append(visiting[visiting.index(target) :] + [target])
            elif target not in visited and target in graph:
                visit(target)
        visiting.pop()

    for module in sorted(graph):
        if module not in visited:
            visit(module)

    assert not cycles, "Import cycles found:\n" + "\n".join(" -> ".join(c) for c in cycles)


def test_no_function_local_first_party_imports():
    """CLAUDE.md forbids lazy imports. A function-local arcticdb import usually hides a module-level cycle."""
    offenders = []
    for path, tree in _iter_modules():
        functions = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        for function in functions:
            for node in ast.walk(function):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    for name in _imported_first_party(node):
                        offenders.append(f"{path.name}:{node.lineno} in {function.name}() imports {name}")

    assert not offenders, "Function-local first-party imports found:\n" + "\n".join(offenders)
