"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random

import numpy as np
import pandas as pd
import pytest

from arcticdb.util.defrag_timeseries import _defrag_timeseries
from arcticdb.util.test import assert_frame_equal


def generic_defrag_test(lib, sym):
    dynamic_schema = lib.lib_cfg().lib_desc.version.write_options.dynamic_schema
    rows_per_slice = lib.lib_cfg().lib_desc.version.write_options.segment_row_size
    if rows_per_slice == 0:
        rows_per_slice = 100_000
    cols_per_slice = lib.lib_cfg().lib_desc.version.write_options.column_group_size
    if cols_per_slice == 0:
        cols_per_slice = 127

    expected = lib.read(sym).data
    total_rows = len(expected)
    total_columns = len(expected.columns)
    col_slices = (
        1 if dynamic_schema else total_columns // cols_per_slice + (1 if total_columns % cols_per_slice != 0 else 0)
    )
    _defrag_timeseries(lib, sym)
    assert_frame_equal(expected, lib.read(sym).data)
    num_segments = len(lib.read_index(sym))
    assert num_segments == col_slices * (
        (total_rows // rows_per_slice) + (1 if total_rows % rows_per_slice != 0 else 0)
    )


def test_defrag_timeseries_basic(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_defrag_timeseries_basic"
    df_0 = pd.DataFrame({"col": [0, 1]}, index=pd.date_range("2025-01-01", periods=2))
    df_1 = pd.DataFrame({"col": [2, 3]}, index=pd.date_range("2025-01-03", periods=2))
    lib.write(sym, df_0)
    lib.append(sym, df_1)
    generic_defrag_test(lib, sym)


def test_defrag_timeseries_col_sliced(version_store_factory):
    lib = version_store_factory(column_group_size=2)
    sym = "test_defrag_timeseries_col_sliced"
    df_0 = pd.DataFrame({"col0": [0, 1], "col1": [2, 3], "col2": [4, 5]}, index=pd.date_range("2025-01-01", periods=2))
    df_1 = pd.DataFrame(
        {"col0": [6, 7], "col1": [8, 9], "col2": [10, 11]}, index=pd.date_range("2025-01-03", periods=2)
    )
    lib.write(sym, df_0)
    lib.append(sym, df_1)
    generic_defrag_test(lib, sym)


@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_defrag_timeseries_partially_compacted(version_store_factory, dynamic_schema):
    lib = version_store_factory(dynamic_schema=dynamic_schema, column_group_size=2, segment_row_size=10)
    sym = "test_defrag_timeseries_partially_compacted"
    index_0 = pd.date_range("2025-01-01", periods=37)
    df_0 = pd.DataFrame(
        {
            "col0": np.arange(len(index_0), dtype=np.int64),
            "col1": np.arange(len(index_0), 2 * len(index_0), dtype=np.float32),
            "col2": np.arange(2 * len(index_0), 3 * len(index_0), dtype=np.uint16),
        },
        index=index_0,
    )
    lib.write(sym, df_0)
    start_date = index_0[-1] + pd.Timedelta(days=1)
    for _ in range(10):
        num_rows = random.randint(1, 4)
        index = pd.date_range(start_date, periods=num_rows)
        df = pd.DataFrame(
            {
                "col0": np.arange(num_rows, dtype=np.int64),
                "col1": np.arange(num_rows, 2 * num_rows, dtype=np.float32),
                "col2": np.arange(2 * num_rows, 3 * num_rows, dtype=np.uint16),
            },
            index=index,
        )
        lib.append(sym, df)
        start_date += pd.Timedelta(days=num_rows)
    generic_defrag_test(lib, sym)
    index = lib.read_index(sym)
    assert (index["version_id"].iloc[:3] == 0).all()


@pytest.mark.parametrize("num_rows", [37, 40])
def test_defrag_timeseries_no_op(version_store_factory, num_rows):
    lib = version_store_factory(column_group_size=2, segment_row_size=10)
    sym = "test_defrag_timeseries_no_op"
    index = pd.date_range("2025-01-01", periods=num_rows)
    df = pd.DataFrame(
        {
            "col0": np.arange(len(index), dtype=np.int64),
            "col1": np.arange(len(index), 2 * len(index), dtype=np.float32),
            "col2": np.arange(2 * len(index), 3 * len(index), dtype=np.uint16),
        },
        index=index,
    )
    lib.write(sym, df)
    generic_defrag_test(lib, sym)
    assert lib.read(sym).version == 0


def test_defrag_timeseries_dynamic_schema(version_store_factory):
    rng = np.random.default_rng()
    lib = version_store_factory(dynamic_schema=True, column_group_size=2, segment_row_size=10)
    sym = "test_defrag_timeseries_dynamic_schema"
    index_0 = pd.date_range("2025-01-01", periods=37)
    df_0 = pd.DataFrame(
        {
            "col0": np.arange(len(index_0), dtype=np.int64),
            "col1": np.arange(len(index_0), 2 * len(index_0), dtype=np.float32),
            "col2": np.arange(2 * len(index_0), 3 * len(index_0), dtype=np.uint16),
        },
        index=index_0,
    )
    lib.write(sym, df_0)
    start_date = index_0[-1] + pd.Timedelta(days=1)
    col_set = ["col0", "col1", "col2", "col3", "col4", "col5"]
    for _ in range(10):
        num_rows = random.randint(1, 4)
        num_cols = random.randint(1, 6)
        cols = random.sample(col_set, num_cols)
        index = pd.date_range(start_date, periods=num_rows)
        df = pd.DataFrame(
            data=rng.integers(0, 1_000, size=(num_rows, num_cols)),
            columns=cols,
            index=index,
        )
        lib.append(sym, df)
        start_date += pd.Timedelta(days=num_rows)
    generic_defrag_test(lib, sym)
