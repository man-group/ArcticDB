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

# The only module ArcticDB exceptions are allowed to be imported from.
ALLOWED_EXCEPTIONS_MODULE = "arcticdb.exceptions"


@functools.lru_cache(maxsize=None)
def _import_module(module_name):
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None


@functools.lru_cache(maxsize=None)
def _is_arctic_exception(module_name, exception_name):
    if not module_name or not module_name.startswith("arcticdb"):
        return False
    module = _import_module(module_name)
    if module is None:
        return False
    exception = getattr(module, exception_name, None)
    return isinstance(exception, type) and issubclass(exception, ArcticException)


def _get_dotted_name(node):
    """Return the dotted string of an attribute chain:
    The module a.b.C becomes the string "a.b.C"
    'c' and 'b' are attributes, 'a' is id.
    The iteration is in reverse order: 'c' -> 'b' -> 'a'"""

    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def find_exception_import_violations(file_content, file_path="<source>"):
    tree = ast.parse(file_content, filename=file_path)
    violations = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module is None or node.module == ALLOWED_EXCEPTIONS_MODULE:
                continue
            for alias in node.names:
                # A star import gives alias.name == "*", which never resolves to an exception, so
                # `from arcticdb.x import *` goes undetected. Catching it would mean importing the
                # module and scanning its namespace - not worth the complexity for now.
                if _is_arctic_exception(node.module, alias.name):
                    # Arctic exception but it is imported from NOT ALLOWED module -> violation
                    violations.append((node.lineno, alias.name, node.module))
        elif isinstance(node, ast.Attribute):
            dotted_name = _get_dotted_name(node)
            module_name, _, exception_name = dotted_name.rpartition(".")
            if (
                module_name
                and module_name != ALLOWED_EXCEPTIONS_MODULE
                and _is_arctic_exception(module_name, exception_name)
            ):
                violations.append((node.lineno, dotted_name, module_name))
    return violations


def find_python_root():
    directory = os.path.dirname(os.path.realpath(__file__))

    while True:
        if os.path.basename(directory) == "python":
            return directory
        parent_directory = os.path.dirname(directory)
        assert parent_directory != directory, "Could not locate the 'python' root directory"
        directory = parent_directory


def iter_python_files(root):
    for directory_path, _, filenames_in_directory in os.walk(root):
        if "__pycache__" in directory_path:
            continue

        for file_name in filenames_in_directory:
            if file_name.endswith(".py"):
                yield os.path.join(directory_path, file_name)


def test_exceptions_only_imported_from_arcticdb_exceptions():
    python_root = find_python_root()
    this_file_path = os.path.realpath(__file__)
    allowed_exceptions_file_path = os.path.join(python_root, *ALLOWED_EXCEPTIONS_MODULE.split(".")) + ".py"
    assert os.path.isfile(allowed_exceptions_file_path), f"Expected '{allowed_exceptions_file_path}' to exist"
    files_to_skip = {this_file_path, allowed_exceptions_file_path}

    violations = []
    for path in iter_python_files(python_root):
        if os.path.realpath(path) in files_to_skip:
            continue

        with open(path, encoding="utf-8") as file:
            file_content = file.read()

        relative_path = os.path.relpath(path, python_root)

        for lineno, exception_name, module_name in find_exception_import_violations(file_content, file_path=path):
            violations.append(
                f"{relative_path}:{lineno}: '{exception_name}' from '{module_name}' (use '{ALLOWED_EXCEPTIONS_MODULE}')"
            )

    assert (
        not violations
    ), f"{len(violations)} exception reference(s) must come from '{ALLOWED_EXCEPTIONS_MODULE}':\n" + "\n".join(
        sorted(violations)
    )
