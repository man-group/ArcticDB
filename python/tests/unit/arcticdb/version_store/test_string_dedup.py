"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import gc
import random
import sys

import numpy as np
import pandas as pd
import pytest

from datetime import datetime as dt

from arcticdb.util.test import random_ascii_strings
from tests.conftest import Marks

pytestmark = Marks.dedup.mark


def generate_dataframe(columns, number_of_rows, strings, index_start="2000-1-1"):
    data = {}
    for column in columns:
        data[column] = random.choices(strings, k=number_of_rows)
    idx = pd.date_range(index_start, periods=number_of_rows, freq="S")
    return pd.DataFrame(data, index=idx)


def getsize(df):
    seen_ids = set()
    size = 0
    for col in df.columns:
        data = df.loc[:, col]
        for _, val in data.items():
            obj_id = id(val)
            if obj_id not in seen_ids:
                size += sys.getsizeof(val)
                seen_ids.add(obj_id)
    return size


def distinct_string_objects(df):
    """How many distinct Python string objects the frame holds - what deduplication is supposed to reduce.

    getsize() cannot be used to compare two frames. It keeps only ids, not references, so when the strings are
    Arrow-backed each iteration materialises a temporary that is freed before the next one and its id is reused:
    it then sums a handful of arbitrary objects rather than all of them. Measuring the same frame repeatedly that
    way returned 1582, 888, 888, 888, 888 - the number tracks allocator state, not the data. Holding the values
    alive keeps the ids distinct and makes the count deterministic.
    """
    values = [val for col in df.columns for val in df.loc[:, col]]
    return len({id(val) for val in values})


def has_python_string_objects(df):
    """False when strings are Arrow-backed, where there are no per-row Python objects to deduplicate."""
    return all(df[col].dtype == object for col in df.columns)


# Use tiny segment to prove deduplication across segments
def test_string_dedup_basic(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_string_dedup_basic"
    original_df = generate_dataframe(["col1", "col2", "col3", "col4"], 1000, random_ascii_strings(100, 10))
    lib.write(symbol, original_df, dynamic_strings=True)
    read_df_with_dedup = lib.read(symbol, optimise_string_memory=True).data
    read_df_without_dedup = lib.read(symbol, optimise_string_memory=False).data
    assert np.array_equal(original_df, read_df_with_dedup)
    assert np.array_equal(original_df, read_df_without_dedup)
    if has_python_string_objects(read_df_with_dedup):
        assert distinct_string_objects(read_df_with_dedup) <= distinct_string_objects(read_df_without_dedup)


@pytest.mark.xfail(
    strict=True,
    reason="optimise_string_memory has been a no-op since 7df641fba (Release GIL on read, 2024-07-02): the read "
    "path stopped passing it through, DecodePathData::set_optimize_for_memory() now has no callers, and "
    "ReadOptions::optimise_string_memory_ is written but never read. Flip to a pass when the wiring is restored.",
)
def test_string_dedup_shares_string_objects(lmdb_version_store_tiny_segment):
    """With deduplication on, equal strings should be one Python object, not one per row."""
    lib = lmdb_version_store_tiny_segment
    symbol = "test_string_dedup_shares_string_objects"
    unique_strings = random_ascii_strings(100, 10)
    original_df = generate_dataframe(["col1", "col2", "col3", "col4"], 1000, unique_strings)
    lib.write(symbol, original_df, dynamic_strings=True)
    read_df_with_dedup = lib.read(symbol, optimise_string_memory=True).data
    assert distinct_string_objects(read_df_with_dedup) <= len(unique_strings)


# Test that dedup still works when writing fixed-width strings and appending dynamic strings, and vice-versa
def test_string_dedup_dynamic_schema(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    symbol = "test_string_dedup_dynamic_schema"
    unique_strings = random_ascii_strings(100, 10)
    original_df = generate_dataframe(["col1"], 1000, unique_strings, "2000-1-1")
    # This will be different to original_df, as the value in each row is chosen at random from the unique string pool
    append_df = generate_dataframe(["col1"], 1000, unique_strings, "2010-1-1")
    total_df = pd.concat((original_df, append_df))

    # Fixed width to dynamic
    lib.write(symbol, original_df, dynamic_strings=False)
    lib.append(symbol, append_df, dynamic_strings=True)
    read_df_with_dedup = lib.read(symbol, optimise_string_memory=True).data
    read_df_without_dedup = lib.read(symbol, optimise_string_memory=False).data
    assert np.array_equal(total_df, read_df_with_dedup)
    assert np.array_equal(total_df, read_df_without_dedup)

    # Dynamic to fixed width
    # Appending fixed-width to dynamic string columns currently broken, see AN-273
    # lib.write(symbol, original_df, dynamic_strings=True)
    # lib.append(symbol, append_df, dynamic_strings=False)
    # read_df_with_dedup = lib.read(symbol, optimise_string_memory=True).data
    # read_df_without_dedup = lib.read(symbol, optimise_string_memory=False).data
    # assert np.array_equal(total_df, read_df_with_dedup)
    # assert np.array_equal(total_df, read_df_without_dedup)


def test_string_dedup_nans(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_string_dedup_nans"
    # Throw a nan into the unique string pool
    unique_strings = random_ascii_strings(9, 10)
    unique_strings.append(np.nan)
    columns = ["col1", "col2", "col3", "col4"]
    original_df = generate_dataframe(columns, 1000, unique_strings)
    lib.write(symbol, original_df, dynamic_strings=True)
    read_df_with_dedup = lib.read(symbol, optimise_string_memory=True).data
    read_df_without_dedup = lib.read(symbol).data
    # Cannot use np.array_equal as equal_nan argument was added after version we are on (1.14.2 at time of writing)
    for column in columns:
        for idx, original_val in original_df.loc[:, column].items():
            read_val = read_df_with_dedup.loc[idx, column]
            if original_val == read_val:
                continue
            # isnan throws if you give it a string
            try:
                if np.isnan(original_val) and np.isnan(read_val):
                    continue
                else:
                    raise
            except TypeError as e:
                print("Differ at {}: '{}' vs '{}'\n{}".format(idx, original_val, read_val, e))
                assert False
    if has_python_string_objects(read_df_with_dedup):
        assert distinct_string_objects(read_df_with_dedup) <= distinct_string_objects(read_df_without_dedup)


@pytest.mark.skip("Used for profiling")
def test_string_dedup_performance(lmdb_version_store):
    lib = lmdb_version_store
    unique_strings = [1, 10, 100, 1_000]
    string_lengths = [1, 10, 100]
    number_of_rows = [1_000, 10_000, 200_000]
    # Uncomment for massive version
    # unique_strings = [1, 10, 100, 1_000, 10_000, 100_000]
    # string_lengths = [1, 10, 100, 1000]
    # number_of_rows = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]
    string_dedup = [True, False]

    results = pd.DataFrame(
        columns=[
            "Rows",
            "Unique strings",
            "String lengths",
            "Original memory (MB)",
            "Read memory (MB)",
            "Read memory with dedup (MB)",
        ]
    )

    for unique_string in unique_strings:
        for string_length in string_lengths:
            string_pool = random_ascii_strings(unique_string, string_length)
            for rows in number_of_rows:
                print("Unique strings:  {}".format(unique_string))
                print("String length:   {}".format(string_length))
                print("DataFrame rows:  {}".format(rows))
                df = generate_dataframe(["col1"], rows, string_pool)
                original_df_memory_mb = getsize(df) / (1024 * 1024)
                print("Original df memory (MB): {}".format(original_df_memory_mb))
                symbol = "{}_{}_{}".format(unique_string, string_length, rows)
                lib.write(symbol, df, dynamic_strings=True)
                del df
                gc.collect()
                results_row = {
                    "Rows": rows,
                    "Unique strings": unique_string,
                    "String lengths": string_length,
                    "Original memory (MB)": original_df_memory_mb,
                }
                # input("Press enter to continue")
                for dedup in string_dedup:
                    print("String dedup: {}".format(dedup))
                    start_time = dt.now()
                    read_df = lib.read(symbol, optimise_string_memory=dedup).data
                    print("Read time {}".format(dt.now() - start_time))
                    read_df_memory_mb = getsize(read_df) / (1024 * 1024)
                    print("Read df memory (MB):     {}".format(read_df_memory_mb))
                    if dedup:
                        results_row["Read memory with dedup (MB)"] = read_df_memory_mb
                    else:
                        results_row["Read memory (MB)"] = read_df_memory_mb
                    del read_df
                    gc.collect()
                results = results.append(results_row, ignore_index=True)
    # print(results)
    # results.to_csv("results.csv")
