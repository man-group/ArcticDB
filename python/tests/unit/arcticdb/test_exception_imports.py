"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import ast
import functools
import importlib
import os

from arcticdb.exceptions import ArcticException

# The one canonical place tests are allowed to import ArcticDB exceptions from.
CANONICAL_MODULE = "arcticdb.exceptions"


@functools.lru_cache(maxsize=None)
def _load_module(module):
    try:
        return importlib.import_module(module)
    except Exception:
        return None


@functools.lru_cache(maxsize=None)
def _is_arctic_exception(module, name):
    if not module or not module.startswith("arcticdb"):
        return False
    resolved = _load_module(module)
    if resolved is None:
        return False
    obj = getattr(resolved, name, None)
    return isinstance(obj, type) and issubclass(obj, ArcticException)


def _dotted_name(node):
    """Return the dotted source of an attribute chain, e.g. ``a.b.C`` -> "a.b.C"."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def find_exception_import_violations(source, filename="<source>"):
    tree = ast.parse(source, filename=filename)
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module is None or node.module == CANONICAL_MODULE:
                continue
            for alias in node.names:
                if _is_arctic_exception(node.module, alias.name):
                    violations.append((node.lineno, alias.name, node.module))
        elif isinstance(node, ast.Attribute):
            dotted = _dotted_name(node)
            module, _, name = dotted.rpartition(".")
            if module and module != CANONICAL_MODULE and _is_arctic_exception(module, name):
                violations.append((node.lineno, dotted, module))
    return violations


def find_tests_root():
    path = os.path.dirname(os.path.abspath(__file__))
    while os.path.basename(path) != "tests":
        parent = os.path.dirname(path)
        assert parent != path, "Could not locate the 'tests' root directory"
        path = parent
    return path


def iter_test_python_files(tests_root):
    for dirpath, _, filenames in os.walk(tests_root):
        if "__pycache__" in dirpath:
            continue
        for filename in filenames:
            if filename.endswith(".py"):
                yield os.path.join(dirpath, filename)


def test_exceptions_only_imported_from_arcticdb_exceptions():
    """Every ArcticDB exception used in the test suite must come from ``arcticdb.exceptions``.

    We don't stop the exceptions being importable from their original locations (that would
    break backwards compatibility), but the tests themselves should use the single canonical
    module so there is one obvious place to find them.
    """
    tests_root = find_tests_root()
    this_file = os.path.abspath(__file__)

    violations = []
    for path in iter_test_python_files(tests_root):
        if os.path.abspath(path) == this_file:
            continue
        with open(path, encoding="utf-8") as file:
            source = file.read()
        relative_path = os.path.relpath(path, tests_root)
        for lineno, reference, module in find_exception_import_violations(source, filename=path):
            violations.append(
                f"tests/{relative_path}:{lineno}: '{reference}' from '{module}' (use '{CANONICAL_MODULE}')"
            )

    assert (
        not violations
    ), f"{len(violations)} exception reference(s) must come from '{CANONICAL_MODULE}':\n" + "\n".join(
        sorted(violations)
    )
