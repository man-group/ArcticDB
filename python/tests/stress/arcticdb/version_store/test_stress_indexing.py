"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import time
from numpy.random import RandomState


def generate_floats(n, pct_null=0.1, repeats=10):
    rand = RandomState(0x42)
    nunique = int(n / repeats)
    unique_values = rand.randn(nunique)

    num_nulls = int(nunique * pct_null)
    null_indices = rand.choice(nunique, size=num_nulls, replace=False)
    unique_values[null_indices] = np.nan

    return unique_values.repeat(repeats)


def generate_data(nrows, ncols, pct_null=0.1, repeats=1, dtype="float"):
    type_ = np.dtype(float)

    data = {"c" + str(i): generate_floats(nrows, pct_null, repeats) for i in range(ncols)}
    return pd.DataFrame(data)


def append_to_arctic(df, symbol, version_store, count):
    start = time.time()
    vit = version_store.append(symbol, df, write_if_missing=True)
    elapsed = time.time() - start
    return vit


# This test is running only against LMDB because it is **very** slow, if ran against a persistent storage
def test_stress_indexing(lmdb_version_store_big_map):
    symbol = "symbol"

    for x in range(1000):
        df = generate_data(2, 11)
        append_to_arctic(df, symbol, lmdb_version_store_big_map, x)
