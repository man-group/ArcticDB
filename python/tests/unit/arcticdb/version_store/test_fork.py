"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import gc
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

from tests.util.mark import AZURE_TESTS_MARK, SKIP_CONDA_MARK

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
    from arcticdb_ext.version_store import NoSuchVersionException

    lib, symbol, idx = args
    for attempt in range(1, 11):
        print("start {}_{} attempt {}".format(symbol, idx, attempt))
        try:
            ss = lib.read(symbol)
        except NoSuchVersionException:
            print("attempt {} fail (symbol not found yet)".format(attempt))
            time.sleep(0.5)
            continue
        if df("test1").equals(ss.data):
            assert_frame_equal(ss.data, df("test1"))
            print("end {}".format(idx))
            return
        print("attempt {} fail".format(attempt))
        time.sleep(0.5)  # Make sure the writes have finished, especially azurite.
    raise AssertionError(f"Symbol {symbol!r} not readable after 10 attempts")


@pytest.mark.parametrize(
    "store_factory",
    [
        "s3_store_factory",
        # Fails: Azure SDK's CurlConnectionPool is global, so forked children transact on sockets the parent
        # opened. Monday ref 12128961896
        pytest.param("azure_store_factory", marks=AZURE_TESTS_MARK),
    ],
)
def test_parallel_reads(store_factory, request):
    lib = request.getfixturevalue(store_factory)()
    symbols = ["XXX"] * 20
    lib.write(symbols[0], df("test1"))
    time.sleep(0.1)  # Make sure the writes have finished, especially azurite.
    p = Pool(10)
    p.map(_read_and_assert_symbol, [(lib, s, idx) for idx, s in enumerate(symbols)])
    p.close()
    p.join()


@pytest.mark.parametrize("storage_name", ["s3_storage", "gcp_storage"])
@SKIP_CONDA_MARK
def test_parallel_reads_arctic(storage_name, request, lib_name):
    storage = request.getfixturevalue(storage_name)
    ac = Arctic(storage.arctic_uri)
    try:
        lib = ac.create_library(lib_name)
        symbols = [f"{i}" for i in range(10)]
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


_INHERITED_AT_FORK = {}


def _child_read_inherited():
    lib = _INHERITED_AT_FORK["lib"]
    assert_frame_equal(lib.read("sym").data, df("sym"))


def _run_in_fork(target, timeout=120):
    ctx = multiprocessing.get_context("fork")
    proc = ctx.Process(target=target)
    proc.start()
    proc.join(timeout)
    if proc.is_alive():
        proc.kill()
        proc.join()
        pytest.fail(f"Forked child running {target.__name__} did not exit within {timeout}s")
    return proc.exitcode


FORK_STORAGES = [
    pytest.param("lmdb_storage", id="lmdb"),
    pytest.param("s3_storage", marks=SKIP_CONDA_MARK, id="s3"),
    pytest.param("gcp_storage", marks=SKIP_CONDA_MARK, id="gcp"),
    pytest.param("azurite_storage", marks=AZURE_TESTS_MARK, id="azure"),
]


@pytest.fixture
def used_library(request, lib_name):
    """An Arctic and a Library that have served at least one read and one write, held in a module global so
    that a forked child can reach them through the memory it inherits."""
    storage = request.getfixturevalue(request.param)
    ac = Arctic(storage.arctic_uri)
    lib = ac.create_library(lib_name)
    lib.write("sym", df("sym"))
    assert_frame_equal(lib.read("sym").data, df("sym"))
    _INHERITED_AT_FORK["ac"] = ac
    _INHERITED_AT_FORK["lib"] = lib
    del ac, lib
    gc.collect()
    try:
        yield
    finally:
        _INHERITED_AT_FORK.clear()
        gc.collect()
        Arctic(storage.arctic_uri).delete_library(lib_name)


@FORK_SUPPORTED
@pytest.mark.parametrize("used_library", FORK_STORAGES, indirect=True)
def test_fork_child_reads_inherited_library(used_library):
    """A forked child reading through a Library inherited from the parent, rather than one reconstructed
    from a pickle, drives the storage's post-fork client rebuild and must still see correct data."""
    assert _run_in_fork(_child_read_inherited) == 0

    assert_frame_equal(_INHERITED_AT_FORK["lib"].read("sym").data, df("sym"))


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
