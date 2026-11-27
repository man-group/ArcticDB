"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import contextlib
import os
import threading

import pandas as pd
import pytest

from arcticdb.exceptions import StorageException

# Every delete of this symbol's keys fails - failures are logged to stderr
SYMBOL = "sym#Failure_Delete_99_0"


@contextlib.contextmanager
def stderr_drained_by_python_thread():
    """
    Points fd 2 at a pipe drained by a Python thread, as ipykernel does. 
    
    The thread needs the GIL after every read, so a call that logs to stderr while holding the GIL deadlocks.
    """
    original_stderr = os.dup(2)
    read_fd, write_fd = os.pipe()
    os.dup2(write_fd, 2)
    os.close(write_fd)

    def drain():
        while os.read(read_fd, 1000):
            pass

    drainer = threading.Thread(target=drain)
    drainer.start()
    try:
        yield
    finally:
        os.dup2(original_stderr, 2)
        os.close(original_stderr)
        drainer.join()
        os.close(read_fd)


@pytest.fixture
def lib(mock_s3_store_with_error_simulation_factory):
    lib = mock_s3_store_with_error_simulation_factory(segment_row_size=1)
    # Two versions of 1000 data keys each, so deleting either one fills up the pipe
    for version in range(2):
        lib.write(SYMBOL, pd.DataFrame({"col": range(version, version + 1000)}))
    return lib


def delete_snapshot(lib, symbol):
    lib.snapshot("snap")
    lib.delete_version(symbol, 0)  # The snapshot still references version 0's keys, so this deletes none of them
    lib.delete_snapshot("snap")

@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    "delete",
    [
        lambda lib, sym: lib.delete(sym),
        lambda lib, sym: lib.delete_version(sym, 0),
        lambda lib, sym: lib.delete_versions(sym, [0]),
        lambda lib, sym: lib.batch_delete_versions([sym], [[0]]),
        lambda lib, sym: lib.batch_delete_symbols([sym]),
        lambda lib, sym: lib.prune_previous_versions(sym),
        delete_snapshot,
    ],
    ids=[
        "delete",
        "delete_version",
        "delete_versions",
        "batch_delete_versions",
        "batch_delete_symbols",
        "prune_previous_versions",
        "delete_snapshot",
    ],
)
def test_failing_delete_does_not_deadlock_when_stderr_is_drained_under_the_gil(lib, delete):
    with stderr_drained_by_python_thread(), contextlib.suppress(StorageException):
        delete(lib, SYMBOL)
