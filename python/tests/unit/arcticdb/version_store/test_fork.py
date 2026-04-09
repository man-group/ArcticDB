"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import multiprocessing
import sys
import time
from multiprocessing import Pool
import numpy as np
import pandas as pd
import pytest
from arcticdb import Arctic
from arcticdb_ext import set_config_int, unset_config_int

from arcticdb.util.test import assert_frame_equal

from tests.util.mark import SKIP_CONDA_MARK

FORK_SUPPORTED = pytest.mark.skipif(sys.platform == "win32", reason="fork/forkserver not available on Windows")


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


def test_parallel_reads(local_object_version_store):
    symbols = ["XXX"] * 20
    p = Pool(10)
    local_object_version_store.write(symbols[0], df("test1"))
    time.sleep(0.1)  # Make sure the writes have finished, especially azurite.
    p.map(_read_and_assert_symbol, [(local_object_version_store, s, idx) for idx, s in enumerate(symbols)])
    p.close()
    p.join()


@pytest.mark.parametrize("storage_name", ["s3_storage", "gcp_storage"])
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


def _check_config_in_child(args):
    """Worker function: verify ConfigsMap was propagated via pickle."""
    _obj, key, expected = args
    from arcticdb_ext import get_config_int

    actual = get_config_int(key)
    assert actual == expected, f"Config {key}: expected {expected}, got {actual}"


@pytest.mark.parametrize(
    "start_method",
    [
        "spawn",
        pytest.param("forkserver", marks=FORK_SUPPORTED),
    ],
)
@pytest.mark.parametrize("store_fixture", ["lmdb_version_store_v1", "lmdb_library"])
def test_configs_propagated_to_child_process(request, store_fixture, start_method):
    """ConfigsMap settings must survive spawn/forkserver process boundaries via pickle."""
    store = request.getfixturevalue(store_fixture)
    set_config_int("TestPropagation", 12345)
    try:
        ctx = multiprocessing.get_context(start_method)
        with ctx.Pool(1) as p:
            p.map(_check_config_in_child, [(store, "TestPropagation", 12345)])
    finally:
        unset_config_int("TestPropagation")


def test_set_config_int_overrides_env_var_after_spawn(lmdb_version_store_v1, monkeypatch):
    """When a config key has both an env var (ARCTICDB_<KEY>_INT) and a
    set_config_int override, the child sees the set_config_int value.

    Env vars survive spawn naturally because the child re-runs
    set_config_from_env_vars on import. The pickle payload is then merged
    on top via __setstate__, so the explicit value takes precedence."""
    monkeypatch.setenv("ARCTICDB_SPAWNOVERRIDE_INT", "42")
    set_config_int("SPAWNOVERRIDE", 99)
    try:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(1) as p:
            # Child import sets SPAWNOVERRIDE=42 from env var,
            # then __setstate__ overwrites it to 99 from the pickle payload.
            p.map(_check_config_in_child, [(lmdb_version_store_v1, "SPAWNOVERRIDE", 99)])
    finally:
        unset_config_int("SPAWNOVERRIDE")
