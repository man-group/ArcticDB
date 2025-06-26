"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
from multiprocessing import Pool
import numpy as np
import pandas as pd
import pytest
from arcticdb import Arctic

from arcticdb.util.test import assert_frame_equal

from tests.util.mark import SKIP_CONDA_MARK, MACOS_WHEEL_BUILD


def df(symbol):
    return pd.DataFrame({symbol: np.arange(100)})


def write_symbol(args):
    store, symbol = args
    print("start {}".format(symbol))
    store.write(symbol, df(symbol))
    print("end {}".format(symbol))
    return symbol


def test_map(lmdb_version_store):
    symbols = ["XXX", "YYY"]
    p = Pool(1)
    p.map(write_symbol, [(lmdb_version_store, s) for s in symbols])
    for s in symbols:
        vit = lmdb_version_store.read(s)
        assert_frame_equal(vit.data, df(s))
    p.close()
    p.join()


def _read_and_assert_symbol(args):
    lib, symbol, idx = args
    for attempt in range(1, 11):
        print("start {}_{} attempt {}".format(symbol, idx, attempt))
        ss = lib.read(symbol)
        if df("test1").equals(ss.data):
            assert_frame_equal(ss.data, df("test1"))
            print("end {}".format(idx))
            break
        else:
            print("attempt {} fail".format(attempt))
            time.sleep(0.5)  # Make sure the writes have finished, especially azurite.


@pytest.mark.skipif(MACOS_WHEEL_BUILD, reason="This test times out on macOS (the reason is unknown)")
def test_parallel_reads(local_object_version_store):
    symbols = ["XXX"] * 20
    p = Pool(10)
    local_object_version_store.write(symbols[0], df("test1"))
    time.sleep(0.1)  # Make sure the writes have finished, especially azurite.
    p.map(_read_and_assert_symbol, [(local_object_version_store, s, idx) for idx, s in enumerate(symbols)])
    p.close()
    p.join()


@pytest.mark.parametrize("storage_name", ["s3_storage", "gcp_storage"])
@pytest.mark.skipif(MACOS_WHEEL_BUILD, reason="This test times out on macOS (the reason is unknown)")
@SKIP_CONDA_MARK
def test_parallel_reads_arctic(storage_name, request, lib_name):
    storage = request.getfixturevalue(storage_name)
    ac = Arctic(storage.arctic_uri)
    try:
        lib = ac.create_library(lib_name)
        symbols = [f"{i}" for i in range(1)]
        for s in symbols:
            lib.write(s, df("test1"))
        p = Pool(10)
        p.map(_read_and_assert_symbol, [(lib, s, idx) for idx, s in enumerate(symbols)])
        p.close()
        p.join()
    finally:
        ac.delete_library(lib_name)
