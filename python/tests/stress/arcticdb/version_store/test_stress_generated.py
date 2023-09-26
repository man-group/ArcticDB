"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import time
from numpy.random import RandomState
import pytest

from arcticdb.util.test import assert_frame_equal


def generate_floats(n, pct_null, repeats=1):
    rand = RandomState(0x42)
    nunique = int(n / repeats)
    unique_values = rand.randn(nunique)

    num_nulls = int(nunique * pct_null)
    null_indices = rand.choice(nunique, size=num_nulls, replace=False)
    unique_values[null_indices] = np.nan

    return unique_values.repeat(repeats)


DATA_GENERATORS = {"float": generate_floats}


def generate_data(total_size, ncols, pct_null=0.1, repeats=1, dtype="float"):
    type_ = np.dtype(float)
    nrows = total_size / ncols / np.dtype(type_).itemsize

    datagen_func = DATA_GENERATORS[dtype]

    data = {"c" + str(i): datagen_func(nrows, pct_null, repeats) for i in range(ncols)}
    return pd.DataFrame(data)


def generate_perf_data(nrows, ncols, pct_null=0.1, repeats=1, dtype="float"):
    type_ = np.dtype(float)

    datagen_func = DATA_GENERATORS[dtype]

    data = {"c" + str(i): datagen_func(nrows, pct_null, repeats) for i in range(ncols)}
    return pd.DataFrame(data)


def write_to_arctic(df, symbol, version_store):
    start = time.time()
    vit = version_store.write(symbol, df)
    elapsed = time.time() - start
    print("arctic write time: " + symbol + " " + str(elapsed))
    return vit


MEGABYTE = 1 << 20
DATA_SIZE = 512 * MEGABYTE
NCOLS = 16


def param_dict(*fields, **cases):
    ids, _ = zip(*cases.items())
    params = [tuple(list(v) + [k]) for k, v in cases.items()]
    return pytest.mark.parametrize(tuple(fields), params, ids=ids)


@param_dict("pct_null", "repeats", "symbol", high_entropy=(0.0, 1), low_entropy=(0.0, 1000))
def test_stress(pct_null, repeats, symbol, object_version_store):
    print("Testing symbol " + symbol)
    df = generate_data(DATA_SIZE, NCOLS, pct_null, repeats)
    print("Generated data, starting write")
    write_to_arctic(df, symbol, object_version_store)
    start = time.time()
    test_df = object_version_store.read(symbol).data
    elapsed = time.time() - start
    print("arctic read time: " + str(elapsed))
    assert_frame_equal(test_df, df)


# This test is running only against LMDB because it is **very** slow, if ran against a persistent storage
@param_dict("pct_null", "repeats", "symbol", high_entropy=(0.0, 1), low_entropy=(0.0, 1000))
def test_stress_small_row(pct_null, repeats, symbol, lmdb_or_in_memory_version_store_tiny_segment):
    print("Testing symbol " + symbol)
    df = generate_data(MEGABYTE, NCOLS, pct_null, repeats)
    print("Generated data, starting write")
    write_to_arctic(df, symbol, lmdb_or_in_memory_version_store_tiny_segment)
    start = time.time()
    test_df = lmdb_or_in_memory_version_store_tiny_segment.read(symbol).data
    elapsed = time.time() - start
    print("arctic read time: " + str(elapsed))
    assert_frame_equal(test_df, df)
