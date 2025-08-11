"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import os
from multiprocessing import Process

import pandas as pd
import numpy as np
import pytest

from arcticdb.util.test import assert_frame_equal


@pytest.mark.storage
def test_many_version_store(basic_store_factory):
    idx2 = np.arange(10, 20)
    d2 = {"x": np.arange(20, 30, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)

    for i in range(10):
        version_store = basic_store_factory(name=f"local.test{i}")

        symbol = "sym_{}".format(i)
        version_store.write(symbol, df2)
        vit = version_store.read(symbol)
        assert_frame_equal(vit.data, df2)


def killed_worker(lib, io_threads, cpu_threads):
    if io_threads:
        lib.delete_snapshot("snap")
    if cpu_threads:
        lib.read("sym")
    os._exit(0)

@pytest.mark.parametrize("io_threads_spawned_in_child", [True, False])
@pytest.mark.parametrize("cpu_threads_spawned_in_child", [True, False])
def test_os_exit_exits_within_timeout(lmdb_storage, lib_name, io_threads_spawned_in_child, cpu_threads_spawned_in_child):
    lib = lmdb_storage.create_arctic().create_library(lib_name)
    df = pd.DataFrame()
    lib.write("sym", df)
    lib.snapshot("snap")
    proc = Process(target=killed_worker, args=(lib, io_threads_spawned_in_child, cpu_threads_spawned_in_child))
    proc.start()
    proc.join(timeout=5)

    if proc.is_alive():
        proc.terminate()
        pytest.fail("os._exit did not exit within 5 seconds")

    assert proc.exitcode == 0
