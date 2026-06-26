"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.util.test import assert_frame_equal, query_stats_operation_count
from arcticdb.version_store.processing import QueryBuilder
import arcticdb.toolbox.query_stats as qs

pytestmark = pytest.mark.pipeline


sym = "sym"


@pytest.mark.parametrize(
    "query_expr",
    [
        lambda q: q["col"] == pd.NaT,
        lambda q: q["col"] != pd.NaT,
        lambda q: q["col"] > pd.Timestamp("2024-01-01"),
        lambda q: q["col"] < pd.Timestamp("2024-01-01"),
        lambda q: q["col"] >= pd.Timestamp("2024-01-01"),
        lambda q: q["col"] <= pd.Timestamp("2024-01-01"),
        lambda q: pd.Timestamp("2024-01-01") < q["col"],
    ],
    ids=["eq_nat", "ne_nat", "gt_ts", "lt_ts", "ge_ts", "le_ts", "ts_lt_col"],
)
def test_filter_nat_values(in_memory_version_store, column_stats_filtering_enabled_and_disabled, query_expr):
    lib = in_memory_version_store

    df0 = pd.DataFrame(
        {"col": [pd.Timestamp("2020-01-01"), pd.NaT]},
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame(
        {"col": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-06-01")]},
        index=pd.date_range("2000-01-03", periods=2),
    )

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats_experimental(sym)

    q = QueryBuilder()
    q = q[query_expr(q)]
    result = lib.read(sym, query_builder=q).data

    full_df = pd.concat([df0, df1])
    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)


@pytest.mark.parametrize(
    "query_expr",
    [
        lambda x: x["a"] == x["b"],
        lambda x: x["a"] != x["b"],
    ],
    ids=["eq", "ne"],
)
def test_filter_nat_col_col(in_memory_version_store, column_stats_filtering_enabled_and_disabled, query_expr):
    lib = in_memory_version_store

    df = pd.DataFrame(
        {
            "a": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.NaT,
                pd.NaT,
                pd.Timestamp("2024-01-05"),
            ],
            "b": [
                pd.Timestamp("2024-01-01"),
                pd.NaT,
                pd.NaT,
                pd.Timestamp("2024-01-04"),
                pd.Timestamp("2024-01-05"),
            ],
        },
        index=pd.date_range("2000-01-01", periods=5),
    )
    lib.write(sym, df)
    lib.create_column_stats_experimental(sym)

    q = QueryBuilder()
    q = q[query_expr(q)]
    result = lib.read(sym, query_builder=q).data

    expected = df[query_expr(df)]
    assert_frame_equal(expected, result)


@pytest.mark.parametrize(
    "query_expr, expected_reads",
    [
        (lambda x: x["a"] == x["b"], 1),
        (lambda x: x["a"] != x["b"], 2),
        (lambda x: x["a"] < x["b"], 1),
        (lambda x: x["a"] >= x["b"], 1),
        (lambda x: x["b"] == x["a"], 1),
        (lambda x: x["b"] != x["a"], 2),
        (lambda x: x["b"] < x["a"], 1),
        (lambda x: x["b"] >= x["a"], 1),
    ],
    ids=["eq", "ne", "lt", "ge", "eq-flipped", "ne-flipped", "lt-flipped", "ge-flipped"],
)
def test_filter_nat_col_col_all_nat_slice(
    in_memory_version_store, column_stats_filtering_enabled_and_disabled, clear_query_stats, query_expr, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame(
        {
            "a": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            "b": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03")],
        },
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame(
        {"a": [pd.NaT, pd.NaT], "b": [pd.NaT, pd.NaT]},
        index=pd.date_range("2000-01-03", periods=2),
    )
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats_experimental(sym)

    qs.enable()
    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = query_stats_operation_count(qs.get_query_stats(), "Memory_GetObject", "TABLE_DATA")

    full_df = pd.concat([df0, df1])
    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)
    if not column_stats_filtering_enabled_and_disabled:
        expected_reads = 2
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"


@pytest.mark.parametrize(
    "values",
    [
        [pd.NaT],
        [pd.NaT, pd.Timestamp("2024-01-02")],
        [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-04")],
    ],
    ids=["nat_only", "nat_and_ts", "ts_only"],
)
@pytest.mark.parametrize("method", ["isin", "isnotin"])
@pytest.mark.parametrize("column", ["col", "all_nat"])
def test_filter_nat_isin(in_memory_version_store, column_stats_filtering_enabled_and_disabled, method, values, column):
    lib = in_memory_version_store

    df = pd.DataFrame(
        {
            "col": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.NaT,
                pd.Timestamp("2024-01-04"),
                pd.NaT,
            ],
            "all_nat": pd.Series([pd.NaT] * 5, dtype="datetime64[ns]"),
        },
        index=pd.date_range("2000-01-01", periods=5),
    )
    lib.write(sym, df)
    lib.create_column_stats_experimental(sym)

    q = QueryBuilder()
    q = q[getattr(q[column], method)(values)]
    result = lib.read(sym, query_builder=q).data

    # NaT in the set is silently ignored, mirroring our NaN-in-set behaviour for floats.
    non_nat_values = [v for v in values if not pd.isna(v)]
    mask = df[column].isin(non_nat_values)
    expected = df[mask] if method == "isin" else df[~mask]
    assert_frame_equal(expected, result)


@pytest.mark.parametrize(
    "query_expr",
    [
        lambda q: q["col"] == pd.NaT,
        lambda q: q["col"] > pd.Timestamp("2024-01-02"),
        lambda q: q["col"] < pd.Timestamp("2024-01-02"),
    ],
    ids=[
        "eq_nat",
        "gt_ts",
        "lt_ts",
    ],  # ne has different behaviour to Pandas, test_filter_ne_dynamic_missing_column_drops_missing_rows
)
def test_filter_nat_dynamic_schema_missing_column(
    lmdb_version_store_dynamic_schema_v1, column_stats_filtering_enabled_and_disabled, query_expr
):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_filter_nat_dynamic_schema_missing_column"

    df0 = pd.DataFrame(
        {"col": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]},
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame({"other": [10, 20]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame(
        {"col": [pd.Timestamp("2024-01-05"), pd.NaT]},
        index=pd.date_range("2000-01-05", periods=2),
    )
    lib.write(symbol, df0)
    lib.append(symbol, df1)
    lib.append(symbol, df2)
    lib.create_column_stats_experimental(symbol)

    full_df = lib.read(symbol).data
    assert full_df.loc[df1.index, "col"].isna().all()

    q = QueryBuilder()
    q = q[query_expr(q)]
    result = lib.read(symbol, query_builder=q).data

    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)


@pytest.mark.parametrize(
    "query_expr",
    [
        lambda q: q["col"] == np.iinfo(np.int64).min,
        lambda q: q["col"] != np.iinfo(np.int64).min,
        lambda q: q["col"] < 0,
        lambda q: q["col"] > -10,
    ],
    ids=["eq_min", "ne_min", "lt_zero", "gt_minus_10"],
)
def test_int64_min_value_not_treated_as_nat(
    in_memory_version_store, column_stats_filtering_enabled_and_disabled, query_expr
):
    lib = in_memory_version_store

    int64_min = np.iinfo(np.int64).min
    df0 = pd.DataFrame({"col": np.array([int64_min, 0], dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col": np.array([5, 10], dtype=np.int64)}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame(
        {"col": np.array([int64_min, int64_min], dtype=np.int64)}, index=pd.date_range("2000-01-05", periods=2)
    )

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.create_column_stats_experimental(sym)

    q = QueryBuilder()
    q = q[query_expr(q)]
    result = lib.read(sym, query_builder=q).data

    full_df = pd.concat([df0, df1, df2])
    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)
