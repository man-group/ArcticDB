"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import ast
import os

import arcticdb.exceptions as arcticdb_exceptions

# The one canonical place tests are allowed to import exceptions from.
CANONICAL_MODULE = "arcticdb.exceptions"

# Native submodules the exceptions are also (still) reachable from. Used to catch
# fully-qualified attribute access such as ``arcticdb_ext.exceptions.UserInputException``.
NATIVE_EXCEPTION_MODULES = {
    "arcticdb_ext.exceptions",
    "arcticdb_ext.storage",
    "arcticdb_ext.version_store",
}


def canonical_exception_names():
    """Every exception re-exported from ``arcticdb.exceptions``.

    A name qualifies if it resolves to a subclass of ``BaseException``. This is name-agnostic
    (so it catches exceptions like ``StreamDescriptorMismatch`` or ``UnknownLibraryOption`` that
    do not end in ``Exception``/``Error``) and excludes error-metadata enums such as
    ``ErrorCode``/``ErrorCategory`` and non-exception data containers such as ``DataError``.
    Names that are *not* re-exported from ``arcticdb.exceptions`` are out of scope and never
    flagged.
    """
    names = set()
    for name in dir(arcticdb_exceptions):
        obj = getattr(arcticdb_exceptions, name)
        if isinstance(obj, type) and issubclass(obj, BaseException):
            names.add(name)
    return names


def dotted_name(node):
    """Return the dotted source of an attribute chain, e.g. ``a.b.C`` -> "a.b.C"."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


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
    """Every exception used in the test suite must be imported from ``arcticdb.exceptions``.

    We don't stop the exceptions being importable from their original locations (that would
    break backwards compatibility), but the tests themselves should use the single canonical
    module so there is one obvious place to find them.
    """
    exception_names = canonical_exception_names()
    tests_root = find_tests_root()
    this_file = os.path.abspath(__file__)

    violations = []
    for path in iter_test_python_files(tests_root):
        if os.path.abspath(path) == this_file:
            continue
        with open(path, encoding="utf-8") as file:
            tree = ast.parse(file.read(), filename=path)
        relative_path = os.path.relpath(path, tests_root)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module == CANONICAL_MODULE:
                    continue
                for imported in node.names:
                    if imported.name in exception_names:
                        violations.append(
                            f"tests/{relative_path}:{node.lineno}: '{imported.name}' from '{node.module}'"
                        )
            elif isinstance(node, ast.Attribute) and node.attr in exception_names:
                dotted = dotted_name(node)
                module = dotted[: -(len(node.attr) + 1)]
                if module in NATIVE_EXCEPTION_MODULES:
                    violations.append(f"tests/{relative_path}:{node.lineno}: '{dotted}' (use '{CANONICAL_MODULE}')")

    assert not violations, f"{len(violations)} exception import(s) must come from '{CANONICAL_MODULE}':\n" + "\n".join(
        sorted(violations)
    )
