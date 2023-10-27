"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import time
from multiprocessing import Pool
from pickle import loads, dumps
import numpy as np
import pandas as pd
import os

from arcticdb.util.test import assert_frame_equal


def df(symbol):
    return pd.DataFrame({symbol: np.arange(100)})


def write_symbol(args):
    store, symbol = args
    print("start {}".format(symbol))
    store.write(symbol, df(symbol))
    print("end {}".format(symbol))
    return symbol


def check_lib_config(lib):
    assert lib.env == "test"
    found_test_normalizer = False
    for normalizer in lib._custom_normalizer._normalizers:
        if normalizer.__class__.__name__ == "TestCustomNormalizer":
            found_test_normalizer = True

    assert found_test_normalizer


def get_pickle_store(lmdb_version_store):
    d = {"a": "b"}
    lmdb_version_store.write("xxx", d)
    ser = dumps(lmdb_version_store)
    nvs = loads(ser)
    out = nvs.read("xxx")
    assert d == out.data


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
        if df("test1").equals(ss.data) or attempt == 10:
            assert_frame_equal(ss.data, df("test1"))
            print("end {}".format(idx))
            break
        else:
            print("attempt {} fail".format(attempt))
            time.sleep(0.5)  # Make sure the writes have finished, especially azurite.


def test_parallel_reads(local_object_version_store):
    from arcticdb.config import set_log_level

    set_log_level("WARN")
    symbols = ["XXX"] * 20
    p = Pool(10)
    local_object_version_store.write(symbols[0], df("test1"))
    time.sleep(0.1)  # Make sure the writes have finished, especially azurite.
    p.map(_read_and_assert_symbol, [(local_object_version_store, s, idx) for idx, s in enumerate(symbols)])
    p.close()
    p.join()


import threading
from datetime import datetime

dff = pd.DataFrame({"symbol": np.arange(1000000000)})


def r_mp(args):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} r_mp {threading.get_native_id()}")
    lib = args
    assert_frame_equal(lib.read("symbol").data, dff)


def test_mp(s3_store_factory):

    lib = s3_store_factory()
    lib.write("symbol", dff)

    def r():
        print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} r {threading.get_native_id()}")
        assert_frame_equal(lib.read("symbol").data, dff)

    def mp_parent():
        print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} mp_parent {threading.get_native_id()}")
        p = Pool(1)
        time.sleep(1.5)  # give the multithread read process a head start, make sure the mutex is locked
        p.map(r_mp, [(lib)])
        p.close()
        p.join()

    thread_list = [threading.Thread(target=r), threading.Thread(target=mp_parent), threading.Thread(target=r)]
    [thread.start() for thread in thread_list]
    [thread.join() for thread in thread_list]
