"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import contextlib
import errno
import os
import sys

import pytest

STRESS_HEAVY = "stress_heavy"

# OS locks rather than a marker file: they are released if the holding worker dies.
if sys.platform == "win32":
    import msvcrt

    def _lock(f):
        f.seek(0)
        while True:
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
                return
            except OSError as e:  # LK_LOCK gives up with EDEADLOCK after about 10 s
                if e.errno != errno.EDEADLOCK:
                    raise

    def _unlock(f):
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock(f):
        fcntl.flock(f, fcntl.LOCK_EX)

    def _unlock(f):
        fcntl.flock(f, fcntl.LOCK_UN)


@contextlib.contextmanager
def _exclusive_file_lock(path):
    with open(path, "a+b") as f:
        _lock(f)
        try:
            yield
        finally:
            _unlock(f)


@pytest.fixture(autouse=True)
def serialise_stress_heavy_tests(request, tmp_path_factory):
    """Run at most one xdist_group("stress_heavy") test at a time across the xdist workers of a session.

    These tests peak at several GB each and two at once OOM the CI runners (#3097). xdist_group alone is not
    enough: worksteal ignores it, and pytest 9.2 with pytest-xdist 3.8.0 drops it under loadgroup too
    (pytest-dev/pytest-xdist#1386). The lock covers the test body and the fixtures set up after this one;
    class-level state such as setup_class data stays alive outside it.
    """
    mark = request.node.get_closest_marker("xdist_group")
    group = mark and (mark.args[0] if mark.args else mark.kwargs.get("name"))
    if group != STRESS_HEAVY or "PYTEST_XDIST_WORKER" not in os.environ:
        yield
        return
    # Each worker's basetemp is <session basetemp>/popen-gwN, so the parent is shared by the whole session.
    with _exclusive_file_lock(tmp_path_factory.getbasetemp().parent / f"{STRESS_HEAVY}.lock"):
        yield
