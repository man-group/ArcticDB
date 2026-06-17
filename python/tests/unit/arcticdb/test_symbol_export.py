"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import subprocess
import sys
import textwrap

import pytest

from tests.util.mark import MACOS_WHEEL_BUILD


@pytest.mark.skipif(
    not MACOS_WHEEL_BUILD,
    reason="Export restriction (-exported_symbols_list) only applies to the macOS vcpkg/wheel build",
)
def test_macos_extension_exports_only_module_init():
    """Packaging-level regression: the macOS extension must export only the Python module
    init symbol. Everything else (AWS SDK, folly, protobuf, ...) is statically linked and must
    stay local, otherwise dyld can coalesce a weak symbol with another extension's incompatible
    copy (e.g. pyarrow's bundled AWS SDK in libarrow), aborting in aws_fatal_assert.
    """
    import arcticdb_ext

    so_path = arcticdb_ext.__file__
    # -g: external symbols, -U: defined only, -j: just the names.
    result = subprocess.run(["nm", "-gjU", so_path], capture_output=True, text=True, check=True)
    exported = sorted(line.strip() for line in result.stdout.splitlines() if line.strip())

    assert exported == ["_PyInit_arcticdb_ext"], (
        f"{so_path} exports symbols other than the module init. Statically-linked dependency "
        f"symbols leaking into the dynamic export table are eligible for dyld weak coalescing "
        f"and can crash when another extension's copy is loaded first. Exported: {exported}"
    )


@pytest.mark.skipif(
    not MACOS_WHEEL_BUILD,
    reason="dyld weak-symbol coalescing across statically-linked AWS SDK copies is macOS-specific",
)
def test_import_pyarrow_before_arcticdb_does_not_abort_on_s3_client():
    """Runtime regression: importing pyarrow (which bundles its own AWS SDK in libarrow) before
    arcticdb, then constructing an S3-backed Arctic — which eagerly builds Aws::S3::S3Client —
    used to abort with aws_fatal_assert. A clean connection failure is fine; a process abort
    (SIGABRT -> negative returncode) is the regression.
    """
    pytest.importorskip("pyarrow")

    script = textwrap.dedent("""
        import pyarrow  # noqa: F401  -- load libarrow's bundled AWS SDK first
        import arcticdb as adb
        try:
            # Eagerly constructs Aws::S3::S3Client; endpoint is bogus so any network op
            # fails fast with a normal exception. The crash point is the constructor itself.
            adb.Arctic("s3://localhost:arcticdb-export-test?access=fake&secret=fake&port=1")
        except Exception:
            pass
        """)
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)

    assert result.returncode == 0, (
        f"Subprocess exited with returncode={result.returncode} (negative => killed by signal, "
        f"e.g. SIGABRT from aws_fatal_assert).\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
