"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import os
import psutil

from arcticdb import Arctic, LibraryOptions, QueryBuilder


ac = Arctic("lmdb:///tmp/memleak_test")
lib = ac.get_library("memleak_test", create_if_missing=True, library_options=LibraryOptions(columns_per_segment=100_000))

def test_write_data():
    lib._nvs.version_store.clear()
    num_rows = 1_000
    num_columns = 10_000
    df = pd.DataFrame(np.random.randint(0, 100, size=(num_rows, num_columns)), index=pd.date_range("2000-01-01", periods=num_rows))
    lib.write("sym", df)


def test_read_data():
    date_range = (pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-04"))
    retrieved = []
    get_rss = lambda: psutil.Process(os.getpid()).memory_info().rss / 1e6

    for i in range(1):
        small = lib.read("sym", query_builder=QueryBuilder().date_range(date_range)).data
        retrieved.append(small)
        print(f"RSS: {get_rss():.2f} MB")
