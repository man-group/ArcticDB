"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import gc
import numpy as np
import pandas as pd
import os
import psutil
import pytest

from arcticdb import Arctic, LibraryOptions, QueryBuilder


ac = Arctic("lmdb:///tmp/memleak_test")
lib = ac.get_library("memleak_test", create_if_missing=True, library_options=LibraryOptions(columns_per_segment=1_000_000))
wide_sym_num_rows = 1
wide_sym_num_cols = 100_000
wide_sym_columns=[f"col_{idx}" for idx in range(wide_sym_num_cols)]
long_sym_num_rows = 100_000_000
long_sym_num_cols = 10
long_sym_columns=[f"col_{idx}" for idx in range(long_sym_num_cols)]


def test_write_data():
    lib._nvs.version_store.clear()
    df = pd.DataFrame(
        np.random.randint(0, 100, size=(wide_sym_num_rows, wide_sym_num_cols)),
        columns=wide_sym_columns,
    )
    lib.write("wide_sym", df)
    df = pd.DataFrame(
        np.random.randint(0, 100, size=(long_sym_num_rows, long_sym_num_cols)),
        columns=long_sym_columns,
    )
    lib.write("long_sym", df)


@pytest.mark.parametrize("read_from_arctic", [True, False])
def test_read_data(read_from_arctic):
    print(f"read_from_arctic {read_from_arctic}")
    retrieved = []
    get_rss = lambda: psutil.Process(os.getpid()).memory_info().rss / 1e6

    for i in range(10):
        if read_from_arctic:
            small = lib.read("wide_sym").data
        else:
            small = pd.DataFrame(
                np.random.randint(0, 100, size=(wide_sym_num_rows, wide_sym_num_cols)),
                columns=wide_sym_columns,
                index=pd.date_range("2000-01-01", periods=wide_sym_num_rows),
            )
        retrieved.append(small)
        print(f"RSS: {get_rss():.2f} MB")


def test_read_data_and_drop_columns():
    get_rss = lambda: psutil.Process(os.getpid()).memory_info().rss / 1e6
    df = lib.read("long_sym").data
    print(f"RSS after read: {get_rss():.2f} MB")
    arrays = [df[f"col_{idx}"].to_numpy() for idx in range(long_sym_num_cols)]
    print(f"RSS after arrays built: {get_rss():.2f} MB")
    del df
    print(f"RSS after del df: {get_rss():.2f} MB")
    for idx in range(long_sym_num_cols):
        arrays.pop()
        print(f"RSS: {get_rss():.2f} MB")
