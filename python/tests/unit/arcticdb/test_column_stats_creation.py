"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import math

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from hypothesis import given, settings, strategies as st
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from arcticc.pb2.column_stats_pb2 import ColumnStatsHeader, ColumnStatsType
from google.protobuf.any_pb2 import Any as ProtobufAny

from arcticdb.exceptions import SchemaException, SortingException, StorageException, UserInputException, NoSuchVersionException
from arcticdb_ext.storage import KeyType
from arcticdb import QueryBuilder
from arcticdb.exceptions import ArcticNativeException
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked
from arcticdb.util.test import config_context

pytestmark = pytest.mark.pipeline

ADMISSION_KEY = "VersionStore.NumProcessingUnitsLive"


def write_many_slices(lib, sym, n_appends):
    """Writes a symbol of n_appends row slices, each two rows of col_0/col_1/col_2."""
    base = pd.Timestamp("2000-01-01")
    for i in range(n_appends):
        df = pd.DataFrame(
            {"col_0": [f"a{i}", f"b{i}"], "col_1": [2 * i, 2 * i + 1], "col_2": [i, i + 1]},
            index=pd.date_range(base + pd.Timedelta(days=2 * i), periods=2),
        )
        lib.write(sym, df) if i == 0 else lib.append(sym, df)


df0 = pd.DataFrame(
    {"col_0": ["a", "b"], "col_1": [1, 2], "col_2": [6, 5]}, index=pd.date_range("2000-01-01", periods=2)
)
df1 = pd.DataFrame(
    {"col_0": ["c", "d"], "col_1": [3, 4], "col_2": [8, 7]}, index=pd.date_range("2000-01-03", periods=2)
)
df2 = pd.DataFrame(
    {"col_0": ["e", "f"], "col_1": [5, 6], "col_2": [10, 9]}, index=pd.date_range("2000-01-05", periods=2)
)


def row_range_columns_to_pl(lib, sym):
    pdf = lib.read_index(sym).reset_index()
    return pl.from_pandas(pdf[["start_row", "end_row"]]).unique(maintain_order=True)


def generate_symbol(lib, sym):
    lib.write(sym, df0)
    lib.append(sym, df1)
    return row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df0["col_1"].min(), df1["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df0["col_1"].max(), df1["col_1"].max()]),
        pl.Series("v1_MIN(col_2)", [df0["col_2"].min(), df1["col_2"].min()]),
        pl.Series("v1_MAX(col_2)", [df0["col_2"].max(), df1["col_2"].max()]),
    )


def assert_stats_equal(received, expected, check_dtypes=False):
    assert isinstance(received, pa.Table)
    assert isinstance(expected, pl.DataFrame)
    received_pl = pl.from_arrow(received)
    # The C++ aggregator always emits v1_NAN_COUNT and v1_NULL_COUNT columns alongside MIN/MAX.
    # Tests that aren't exercising the count behaviour omit those columns from `expected`;
    # subselect `received` down to the expected columns so the comparison stays focused.
    missing = set(expected.columns) - set(received_pl.columns)
    assert not missing, f"Expected columns missing from received: {missing}"
    received_pl = received_pl.select(expected.columns)
    pl_assert_frame_equal(received_pl, expected, check_column_order=False, check_dtypes=check_dtypes)


def test_column_stats_basic_flow(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_basic_flow"
    expected_column_stats = generate_symbol(lib, sym)

    # Should just log a warning if no column stats exist
    lib.drop_column_stats_experimental(sym)

    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    lib.drop_column_stats_experimental(sym)
    with pytest.raises(StorageException):
        lib.get_column_stats_info_experimental(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats_experimental(sym)


def test_column_stats_infinity(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_infinity"
    df0 = pd.DataFrame({"col_1": [np.inf, 0.5]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [1.5, -np.inf]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [np.inf, -np.inf]}, index=pd.date_range("2000-01-05", periods=2))
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df0["col_1"].min(), df1["col_1"].min(), df2["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df0["col_1"].max(), df1["col_1"].max(), df2["col_1"].max()]),
    )

    lib.create_column_stats_experimental(sym)

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_nan_values(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(encoding_version=int(encoding_version), name=lib_name)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_nan_values"
    df0 = pd.DataFrame({"col_1": [1.0, 3.0]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [np.nan, 5.0]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5.0, np.nan]}, index=pd.date_range("2000-01-05", periods=2))
    df3 = pd.DataFrame({"col_1": [np.nan, np.nan]}, index=pd.date_range("2000-01-07", periods=2))
    df4 = pd.DataFrame({"col_1": [1.0, np.nan, 2.0]}, index=pd.date_range("2000-01-09", periods=3))
    df5 = pd.DataFrame({"col_1": [np.nan, 1.0, 2.0, np.nan]}, index=pd.date_range("2000-01-12", periods=4))
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.append(sym, df4)
    lib.append(sym, df5)

    # We store nan stats iff the whole block is nan
    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [1.0, 5.0, 5.0, np.nan, 1.0, 1.0]),
        pl.Series("v1_MAX(col_1)", [3.0, 5.0, 5.0, np.nan, 2.0, 2.0]),
    )

    lib.create_column_stats_experimental(sym)

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_nat_values(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(encoding_version=int(encoding_version), name=lib_name)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_nat_values"
    df0 = pd.DataFrame(
        {"col_1": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-06-01")]},
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame(
        {"col_1": [pd.NaT, pd.Timestamp("2025-01-01")]},
        index=pd.date_range("2000-01-03", periods=2),
    )
    df2 = pd.DataFrame(
        {"col_1": [pd.Timestamp("2025-01-01"), pd.NaT]},
        index=pd.date_range("2000-01-05", periods=2),
    )
    df3 = pd.DataFrame(
        {"col_1": [pd.NaT, pd.NaT]},
        index=pd.date_range("2000-01-07", periods=2),
    )
    df4 = pd.DataFrame(
        {"col_1": [pd.Timestamp("2024-01-01"), pd.NaT, pd.Timestamp("2024-06-01")]},
        index=pd.date_range("2000-01-09", periods=3),
    )
    df5 = pd.DataFrame(
        {"col_1": [pd.NaT, pd.Timestamp("2023-01-01"), pd.Timestamp("2023-06-01"), pd.NaT]},
        index=pd.date_range("2000-01-12", periods=4),
    )
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.append(sym, df4)
    lib.append(sym, df5)

    # We store NaT stats iff the whole block is NaT. The Arrow output path converts NaT to null,
    # so the public read_column_stats API surfaces None for the all-NaT block.
    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series(
            "v1_MIN(col_1)",
            [
                pd.Timestamp("2020-01-01").value,
                pd.Timestamp("2025-01-01").value,
                pd.Timestamp("2025-01-01").value,
                None,
                pd.Timestamp("2024-01-01").value,
                pd.Timestamp("2023-01-01").value,
            ],
            dtype=pl.Int64,
        ).cast(pl.Datetime("ns")),
        pl.Series(
            "v1_MAX(col_1)",
            [
                pd.Timestamp("2020-06-01").value,
                pd.Timestamp("2025-01-01").value,
                pd.Timestamp("2025-01-01").value,
                None,
                pd.Timestamp("2024-06-01").value,
                pd.Timestamp("2023-06-01").value,
            ],
            dtype=pl.Int64,
        ).cast(pl.Datetime("ns")),
    )

    lib.create_column_stats_experimental(sym)

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    nat_sentinel = np.iinfo(np.int64).min
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(keys) == 1
    raw_stats = lib_tool.read_to_dataframe(keys[0])
    assert raw_stats["v1_MIN(col_1)"].values.view("int64")[3] == nat_sentinel
    assert raw_stats["v1_MAX(col_1)"].values.view("int64")[3] == nat_sentinel


def test_column_stats_nan_and_null_counts(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(encoding_version=int(encoding_version), name=lib_name)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_nan_and_null_counts"

    # Each write/append produces a separate segment, so we get one row per dataframe in the stats.
    # Both NaN (float) and NaT (timestamp) are in-band sentinels and count toward v1_NAN_COUNT.
    df0 = pd.DataFrame(
        {"float_col": [1.0, 2.0], "ts_col": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-06-01")]},
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame(
        {"float_col": [np.nan, 5.0], "ts_col": [pd.NaT, pd.Timestamp("2021-01-01")]},
        index=pd.date_range("2000-01-03", periods=2),
    )
    df2 = pd.DataFrame(
        {"float_col": [np.nan, np.nan], "ts_col": [pd.NaT, pd.NaT]},
        index=pd.date_range("2000-01-05", periods=2),
    )
    df3 = pd.DataFrame(
        {"float_col": [1.0, np.nan, 2.0], "ts_col": [pd.Timestamp("2022-01-01"), pd.NaT, pd.Timestamp("2022-06-01")]},
        index=pd.date_range("2000-01-07", periods=3),
    )
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)

    lib.create_column_stats_experimental(sym)

    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(float_col)", [1.0, 5.0, np.nan, 1.0]),
        pl.Series("v1_MAX(float_col)", [2.0, 5.0, np.nan, 2.0]),
        pl.Series("v1_NAN_COUNT(float_col)", [0, 1, 2, 1], dtype=pl.UInt64),
        pl.Series("v1_NULL_COUNT(float_col)", [0, 0, 0, 0], dtype=pl.UInt64),
        pl.Series(
            "v1_MIN(ts_col)",
            [
                pd.Timestamp("2020-01-01").value,
                pd.Timestamp("2021-01-01").value,
                None,
                pd.Timestamp("2022-01-01").value,
            ],
            dtype=pl.Int64,
        ).cast(pl.Datetime("ns")),
        pl.Series(
            "v1_MAX(ts_col)",
            [
                pd.Timestamp("2020-06-01").value,
                pd.Timestamp("2021-01-01").value,
                None,
                pd.Timestamp("2022-06-01").value,
            ],
            dtype=pl.Int64,
        ).cast(pl.Datetime("ns")),
        pl.Series("v1_NAN_COUNT(ts_col)", [0, 1, 2, 1], dtype=pl.UInt64),
        pl.Series("v1_NULL_COUNT(ts_col)", [0, 0, 0, 0], dtype=pl.UInt64),
    )

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_nan_count_single_segment(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    """In-band sentinels (NaN in a float column, NaT in a timestamp column) count towards
    v1_NAN_COUNT. v1_NULL_COUNT is reserved for genuinely-missing rows (sparse-map gaps)."""
    lib = in_memory_store_factory(encoding_version=int(encoding_version), name=lib_name)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_nan_count_single_segment"
    df = pd.DataFrame(
        {
            "float_col": [1.0, np.nan, 2.0, np.nan, np.nan],
            "ts_col": [
                pd.Timestamp("2020-01-01"),
                pd.NaT,
                pd.Timestamp("2020-06-01"),
                pd.NaT,
                pd.Timestamp("2020-12-01"),
            ],
        },
        index=pd.date_range("2000-01-01", periods=5),
    )
    lib.write(sym, df)
    lib.create_column_stats_experimental(sym)

    cs = pl.from_arrow(lib.read_column_stats_experimental(sym))
    assert cs["v1_NAN_COUNT(float_col)"].to_list() == [3]
    assert cs["v1_NULL_COUNT(float_col)"].to_list() == [0]
    assert cs["v1_NAN_COUNT(ts_col)"].to_list() == [2]
    assert cs["v1_NULL_COUNT(ts_col)"].to_list() == [0]


def test_column_stats_null_count_sparse_floats(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    """sparsify_floats=True stores NaN floats as sparse-map gaps rather than dense NaN values.
    Those gaps are counted as nulls (v1_NULL_COUNT), not NaNs (v1_NAN_COUNT).

    The 6 rows span multiple segments (segment_row_size=3) with a different null count in each,
    so the per-segment null calculation is exercised rather than a single-segment case."""
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=3,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_null_count_sparse_floats"
    # Segment 0 (rows 0-2): [1.0, nan, 2.0] -> 1 null, min 1.0, max 2.0
    # Segment 1 (rows 3-5): [nan, nan, 4.0] -> 2 nulls, min 4.0, max 4.0
    df = pd.DataFrame({"col_1": [1.0, np.nan, 2.0, np.nan, np.nan, 4.0]}, index=pd.date_range("2000-01-01", periods=6))
    lib.write(sym, df, sparsify_floats=True)
    lib.create_column_stats_experimental(sym)

    cs = pl.from_arrow(lib.read_column_stats_experimental(sym)).sort("start_row")
    assert cs["v1_NULL_COUNT(col_1)"].to_list() == [1, 2]
    assert cs["v1_NAN_COUNT(col_1)"].to_list() == [0, 0]
    assert cs["v1_MIN(col_1)"].to_list() == [1.0, 4.0]
    assert cs["v1_MAX(col_1)"].to_list() == [2.0, 4.0]


def test_column_stats_arrow_nan_and_null_same_column(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    lib._cfg.write_options.segment_row_size = 100
    sym = "test_column_stats_arrow_nan_and_null_same_column"

    def table(values):
        return pa.table({"f": pa.array(values, pa.float64())})

    lib.write(sym, table([1.0, np.nan, None, 2.0]))  # nan=1, null=1, min=1, max=2
    lib.append(sym, table([np.nan, np.nan, None]))  # nan=2, null=1, all stored values are NaN
    lib.append(sym, table([None, None]))  # all null: nan=0, null=2, no min/max
    lib.append(sym, table([3.0, None, np.nan, 4.0, None]))  # nan=1, null=2, min=3, max=4

    lib.create_column_stats_experimental(sym)
    cs = pl.from_arrow(lib.read_column_stats_experimental(sym)).sort("start_row")

    assert cs["v1_NAN_COUNT(f)"].to_list() == [1, 2, 0, 1]
    assert cs["v1_NULL_COUNT(f)"].to_list() == [1, 1, 2, 2]

    mins = cs["v1_MIN(f)"].to_list()
    maxs = cs["v1_MAX(f)"].to_list()
    assert mins[0] == 1.0 and maxs[0] == 2.0
    assert math.isnan(mins[1]) and math.isnan(maxs[1])  # all-NaN block keeps NaN min/max
    assert mins[2] is None and maxs[2] is None  # all-null block has no min/max
    assert mins[3] == 3.0 and maxs[3] == 4.0


def _segment_values():
    element = st.one_of(
        st.floats(min_value=-1e9, max_value=1e9, allow_nan=False, allow_infinity=False),
        st.just(float("nan")),
        st.none(),
    )
    return st.lists(element, min_size=1, max_size=10)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(segments=st.lists(_segment_values(), min_size=1, max_size=6))
def test_column_stats_arrow_nan_null_counts_hypothesis(in_memory_version_store_arrow, segments):
    lib = in_memory_version_store_arrow
    lib._cfg.write_options.segment_row_size = 100
    lib.version_store.clear()
    sym = "test_column_stats_arrow_nan_null_counts_hypothesis"

    lib.write(sym, pa.table({"f": pa.array(segments[0], pa.float64())}))
    for seg in segments[1:]:
        lib.append(sym, pa.table({"f": pa.array(seg, pa.float64())}))

    lib.create_column_stats_experimental(sym)
    cs = pl.from_arrow(lib.read_column_stats_experimental(sym)).sort("start_row")

    # Each write/append is its own row-slice, so column stats rows line up with segments in index order.
    expected_nan = [sum(1 for v in seg if v is not None and np.isnan(v)) for seg in segments]
    expected_null = [sum(1 for v in seg if v is None) for seg in segments]

    assert cs["v1_NAN_COUNT(f)"].to_list() == expected_nan
    assert cs["v1_NULL_COUNT(f)"].to_list() == expected_null


def test_column_stats_as_of(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_as_of"
    expected_column_stats = generate_symbol(lib, sym)[[0]]
    expected_stats_info = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats_experimental(sym, as_of=0)
    assert lib.get_column_stats_info_experimental(sym, as_of=0) == expected_stats_info
    with pytest.raises(StorageException):
        lib.get_column_stats_info_experimental(sym)

    column_stats = lib.read_column_stats_experimental(sym, as_of=0)
    assert_stats_equal(column_stats, expected_column_stats)
    with pytest.raises(StorageException):
        lib.read_column_stats_experimental(sym)

    lib.drop_column_stats_experimental(sym, as_of=0)
    with pytest.raises(StorageException):
        lib.get_column_stats_info_experimental(sym, as_of=0)
    with pytest.raises(StorageException):
        lib.read_column_stats_experimental(sym, as_of=0)


def test_column_stats_as_of_version_doesnt_exist(
    in_memory_store_factory, lib_name, encoding_version, any_output_format
):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_as_of_version_doesnt_exist"
    generate_symbol(lib, sym)

    lib.delete_version(sym, 0)

    with pytest.raises(NoSuchVersionException):
        lib.create_column_stats_experimental(sym, as_of=0)
    with pytest.raises(NoSuchVersionException):
        lib.get_column_stats_info_experimental(sym, as_of=0)
    with pytest.raises(NoSuchVersionException):
        lib.read_column_stats_experimental(sym, as_of=0)


def test_column_stats_multiple_indexes_different_columns(
    in_memory_store_factory, lib_name, encoding_version, any_output_format
):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_multiple_indexes"
    expected_column_stats = generate_symbol(lib, sym)

    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_pickled_symbol(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_pickled_symbol"
    lib.write(sym, 1)
    assert lib.is_symbol_pickled(sym)

    with pytest.raises(SchemaException):
        lib.create_column_stats_experimental(sym)


def test_column_stats_duplicated_primary_index(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_duplicated_primary_index"

    total_df = pd.concat((df0, df1))
    lib.write(sym, total_df)
    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df0["col_1"].min(), df1["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df0["col_1"].max(), df1["col_1"].max()]),
        pl.Series("v1_MIN(col_2)", [df0["col_2"].min(), df1["col_2"].min()]),
        pl.Series("v1_MAX(col_2)", [df0["col_2"].max(), df1["col_2"].max()]),
    )

    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_dynamic_schema_missing_data(
    in_memory_store_factory, lib_name, encoding_version, any_output_format
):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        dynamic_schema=True,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_dynamic_schema_missing_data"

    df0 = pd.DataFrame({"col_1": [0.1, 0.2], "col_2": [0.3, 0.4]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_2": [0.5, 0.6]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [0.7, 0.8]}, index=pd.date_range("2000-01-05", periods=2))
    df3 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-07", periods=2))
    df4 = pd.DataFrame(
        {"col_1": [0.9, np.nan], "col_2": [np.nan, np.nan]}, index=pd.date_range("2000-01-09", periods=2)
    )
    df5 = pd.DataFrame({"col_5": np.array([10, 20], dtype=np.int64)}, index=pd.date_range("2000-01-11", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.append(sym, df4)
    lib.append(sym, df5)

    # Slices that are missing the column come back as null via the validity bitmap. df4["col_2"] is
    # all-NaN but the column is present so the stat is computed and stored as NaN, not null.
    # The count columns follow the same rule: a fully-missing column has no aggregator run for that
    # slice, so its NAN_COUNT/NULL_COUNT are null (not 0). Present columns count their NaNs in
    # NAN_COUNT and leave NULL_COUNT at 0 (NaN floats are stored densely here, not as sparse gaps).
    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series(
            "v1_MIN(col_1)",
            [df0["col_1"].min(), None, df2["col_1"].min(), None, df4["col_1"].min(), None],
        ),
        pl.Series(
            "v1_MAX(col_1)",
            [df0["col_1"].max(), None, df2["col_1"].max(), None, df4["col_1"].max(), None],
        ),
        pl.Series("v1_NAN_COUNT(col_1)", [0, None, 0, None, 1, None], dtype=pl.UInt64),
        pl.Series("v1_NULL_COUNT(col_1)", [0, None, 0, None, 0, None], dtype=pl.UInt64),
        pl.Series(
            "v1_MIN(col_2)",
            [df0["col_2"].min(), df1["col_2"].min(), None, None, df4["col_2"].min(), None],
        ),
        pl.Series(
            "v1_MAX(col_2)",
            [df0["col_2"].max(), df1["col_2"].max(), None, None, df4["col_2"].max(), None],
        ),
        pl.Series("v1_NAN_COUNT(col_2)", [0, 0, None, None, 2, None], dtype=pl.UInt64),
        pl.Series("v1_NULL_COUNT(col_2)", [0, 0, None, None, 0, None], dtype=pl.UInt64),
        pl.Series("v1_MIN(col_5)", [None, None, None, None, None, df5["col_5"].min()], dtype=pl.Int64),
        pl.Series("v1_MAX(col_5)", [None, None, None, None, None, df5["col_5"].max()], dtype=pl.Int64),
        pl.Series("v1_NAN_COUNT(col_5)", [None, None, None, None, None, 0], dtype=pl.UInt64),
        pl.Series("v1_NULL_COUNT(col_5)", [None, None, None, None, None, 0], dtype=pl.UInt64),
    )
    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {
        "col_1": {"MINMAX"},
        "col_2": {"MINMAX"},
        "col_5": {"MINMAX"},
    }

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def dynamic_schema_types_changing_dfs():
    df0 = pd.DataFrame(
        {
            "int_widening": np.arange(0, 2, dtype=np.uint8),
            "int_narrowing": np.arange(-1002, -1000, dtype=np.int16),
            "unsigned_to_wider_signed_int": np.arange(1000, 1002, dtype=np.uint16),
            "wider_signed_to_unsigned_int": np.arange(-1002, -1000, dtype=np.int32),
            "unsigned_to_signed_int_same_width": np.arange(1000, 1002, dtype=np.uint16),
            "int_to_float": np.arange(1000, 1002, dtype=np.uint16),
            "float_to_int": [1.5, 2.5],
        },
        index=pd.date_range("2000-01-01", periods=2),
    )

    df1 = pd.DataFrame(
        {
            "int_widening": np.arange(1000, 1002, dtype=np.uint16),
            "int_narrowing": np.arange(-2, 0, dtype=np.int8),
            "unsigned_to_wider_signed_int": np.arange(-1002, -1000, dtype=np.int32),
            "wider_signed_to_unsigned_int": np.arange(1000, 1002, dtype=np.uint16),
            "unsigned_to_signed_int_same_width": np.arange(-1002, -1000, dtype=np.int16),
            "int_to_float": [1.5, 2.5],
            "float_to_int": np.arange(1000, 1002, dtype=np.uint16),
        },
        index=pd.date_range("2000-01-03", periods=2),
    )
    return df0, df1


def assert_dynamic_schema_types_changing_schema(schema):
    assert schema["v1_MIN(int_widening)"] == pl.UInt16
    assert schema["v1_MAX(int_widening)"] == pl.UInt16

    assert schema["v1_MIN(int_narrowing)"] == pl.Int16
    assert schema["v1_MAX(int_narrowing)"] == pl.Int16

    assert schema["v1_MIN(unsigned_to_wider_signed_int)"] == pl.Int32
    assert schema["v1_MAX(unsigned_to_wider_signed_int)"] == pl.Int32

    assert schema["v1_MIN(wider_signed_to_unsigned_int)"] == pl.Int32
    assert schema["v1_MAX(wider_signed_to_unsigned_int)"] == pl.Int32

    assert schema["v1_MIN(unsigned_to_signed_int_same_width)"] == pl.Int32
    assert schema["v1_MAX(unsigned_to_signed_int_same_width)"] == pl.Int32

    assert schema["v1_MIN(int_to_float)"] == pl.Float64
    assert schema["v1_MAX(int_to_float)"] == pl.Float64

    assert schema["v1_MIN(float_to_int)"] == pl.Float64
    assert schema["v1_MAX(float_to_int)"] == pl.Float64


def test_column_stats_dynamic_schema_types_changing(
    in_memory_store_factory, lib_name, encoding_version, any_output_format
):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        dynamic_schema=True,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_dynamic_schema_types_changing"

    df0, df1 = dynamic_schema_types_changing_dfs()
    lib.write(sym, df0)
    lib.append(sym, df1)

    def int_minmax(name):
        return [
            pl.Series(f"v1_MIN({name})", [int(df0[name].min()), int(df1[name].min())]),
            pl.Series(f"v1_MAX({name})", [int(df0[name].max()), int(df1[name].max())]),
        ]

    def float_minmax(name):
        return [
            pl.Series(f"v1_MIN({name})", [float(df0[name].min()), float(df1[name].min())]),
            pl.Series(f"v1_MAX({name})", [float(df0[name].max()), float(df1[name].max())]),
        ]

    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        *int_minmax("int_widening"),
        *int_minmax("int_narrowing"),
        *int_minmax("unsigned_to_wider_signed_int"),
        *int_minmax("wider_signed_to_unsigned_int"),
        *int_minmax("unsigned_to_signed_int_same_width"),
        *float_minmax("int_to_float"),
        *float_minmax("float_to_int"),
    )
    expected_stats_info = {
        "int_widening": {"MINMAX"},
        "int_narrowing": {"MINMAX"},
        "unsigned_to_wider_signed_int": {"MINMAX"},
        "wider_signed_to_unsigned_int": {"MINMAX"},
        "unsigned_to_signed_int_same_width": {"MINMAX"},
        "int_to_float": {"MINMAX"},
        "float_to_int": {"MINMAX"},
    }
    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == expected_stats_info

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    assert_dynamic_schema_types_changing_schema(pl.from_arrow(column_stats).schema)


def test_column_stats_object_deleted_with_index_key(
    in_memory_store_factory, lib_name, encoding_version, any_output_format
):
    def clear():
        nonlocal expected_count
        lib.version_store.clear()
        expected_count = 0

    def create_stats():
        nonlocal expected_count
        lib.create_column_stats_experimental(sym)
        expected_count += 1

    def assert_column_stats_key_count():
        assert lib_tool.count_keys(KeyType.COLUMN_STATS) == expected_count

    def test_delete():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.delete(sym)
        expected_count = 0
        assert_column_stats_key_count()

    def test_delete_version():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.delete_version(sym, 0)
        expected_count = 1
        assert_column_stats_key_count()

    def test_delete_versions():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df2)
        create_stats()
        assert_column_stats_key_count()
        lib.delete_versions(sym, [0, 1])
        expected_count = 1
        assert_column_stats_key_count()

    def test_delete_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.delete_snapshot(snapshot_name)
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_add_to_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.write(sym, df1)
        lib.add_to_snapshot(snapshot_name, [sym])
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_remove_from_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.remove_from_snapshot(snapshot_name, [sym], [0])
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_prune_previous_kwarg():
        nonlocal expected_count
        for operation in ["write", "append", "update", "write_metadata"]:
            lib.write(sym, df0)
            create_stats()
            assert_column_stats_key_count()
            getattr(lib, operation)(sym, df1, prune_previous_version=True)
            expected_count = 0
            assert_column_stats_key_count()
            clear()

    def test_prune_previous_kwarg_batch_methods():
        nonlocal expected_count
        for operation in ["batch_write", "batch_append", "batch_write_metadata"]:
            lib.write(sym, df0)
            create_stats()
            assert_column_stats_key_count()
            getattr(lib, operation)([sym], [df1], prune_previous_version=True)
            expected_count = 0
            assert_column_stats_key_count()
            clear()

    def test_prune_previous_api():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.prune_previous_versions(sym)
        expected_count = 1
        assert_column_stats_key_count()

    lib = in_memory_store_factory(encoding_version=int(encoding_version), name=lib_name)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    lib_tool = lib.library_tool()
    sym = "test_column_stats_object_deleted_with_index_key"
    expected_count = 0

    for test in [
        test_delete,
        test_delete_version,
        test_delete_versions,
        test_delete_snapshot,
        test_add_to_snapshot,
        test_remove_from_snapshot,
        test_prune_previous_kwarg,
        test_prune_previous_kwarg_batch_methods,
        test_prune_previous_api,
    ]:
        test()
        clear()


def read_column_stats_header(lib, sym):
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(keys) == 1
    seg = lib_tool.read_to_segment_in_memory(keys[0])
    any_msg = ProtobufAny()
    any_msg.ParseFromString(seg.metadata())
    header = ColumnStatsHeader()
    assert any_msg.Unpack(header)
    return header


def header_stat_pairs(header):
    return {
        (data_col_offset, entry.type)
        for data_col_offset, entry_list in header.stats_by_column.items()
        for entry in entry_list.entries
    }


def header_stat_count(header):
    return sum(len(entry_list.entries) for entry_list in header.stats_by_column.values())


def header_all_entries(header):
    """Yield (data_col_offset, entry) for every stat entry in the header."""
    for data_col_offset, entry_list in header.stats_by_column.items():
        for entry in entry_list.entries:
            yield data_col_offset, entry


def header_offset_by_stat(header):
    """{(data_col_offset, stat type): stats_seg_offset}. Order-independent, unlike comparing the
    serialized header, since protobuf map ordering is unspecified."""
    return {
        (data_col_offset, entry.type): entry.stats_seg_offset for data_col_offset, entry in header_all_entries(header)
    }


def assert_header_offsets_match_field_names(lib, sym, header, col_name_by_offset):
    """Every entry's stats_seg_offset must point at the stats column named after the data column
    at its data_col_offset."""
    field_name_by_type = {
        ColumnStatsType.MIN_V1: "v1_MIN",
        ColumnStatsType.MAX_V1: "v1_MAX",
        ColumnStatsType.NAN_COUNT_V1: "v1_NAN_COUNT",
        ColumnStatsType.NULL_COUNT_V1: "v1_NULL_COUNT",
    }
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    fields = lib_tool.read_descriptor(keys[0]).fields()
    for data_col_offset, entry in header_all_entries(header):
        expected = f"{field_name_by_type[entry.type]}({col_name_by_offset[data_col_offset]})"
        assert fields[entry.stats_seg_offset].name == expected


def test_column_stats_header_metadata(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_header_metadata"
    generate_symbol(lib, sym)

    # Auto-discovery creates stats for all eligible columns (col_1 at offset 2, col_2 at offset 3).
    # MINMAX emits 4 stat entries per column: MIN, MAX, NAN_COUNT, NULL_COUNT.
    minmax_types = {
        ColumnStatsType.MIN_V1,
        ColumnStatsType.MAX_V1,
        ColumnStatsType.NAN_COUNT_V1,
        ColumnStatsType.NULL_COUNT_V1,
    }
    lib.create_column_stats_experimental(sym)
    header = read_column_stats_header(lib, sym)

    assert header.version == 1
    # if you change the structure, consider whether you need to change header.version too
    assert len(header.ListFields()) == 2
    assert header_stat_count(header) == 8
    assert header_stat_pairs(header) == {(2, t) for t in minmax_types} | {(3, t) for t in minmax_types}
    offsets = [entry.stats_seg_offset for _, entry in header_all_entries(header)]
    assert len(set(offsets)) == 8

    # Verify descriptor field names match the offsets
    assert_header_offsets_match_field_names(lib, sym, header, {2: "col_1", 3: "col_2"})


def test_column_stats_create_twice_is_idempotent(in_memory_store_factory, lib_name):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        name=lib_name,
    )
    sym = "test_column_stats_create_twice_is_idempotent"
    expected_column_stats = generate_symbol(lib, sym)

    lib.create_column_stats_experimental(sym)
    first_header = read_column_stats_header(lib, sym)
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected_column_stats)

    lib.create_column_stats_experimental(sym)
    second_header = read_column_stats_header(lib, sym)

    assert header_offset_by_stat(second_header) == header_offset_by_stat(first_header)
    assert second_header.version == first_header.version
    assert header_stat_count(second_header) == 8
    assert len({entry.stats_seg_offset for _, entry in header_all_entries(second_header)}) == 8
    assert_header_offsets_match_field_names(lib, sym, second_header, {2: "col_1", 3: "col_2"})
    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected_column_stats)


def test_column_stats_duplicated_column_names(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_basic_flow_duplicated_column_names"
    df = pd.DataFrame([[7, 1, 6], [8, 2, 5]], index=pd.date_range("2000-01-01", periods=2))
    df.columns = ["col_0", "col_1", "col_1"]

    lib.write(sym, df)
    # Check we're testing the right thing
    saved_col_names = lib.get_info(sym)["normalization_metadata"].df.common.col_names
    assert len(saved_col_names) == 3
    assert "col_0" in saved_col_names
    assert "__col_col_1__1" in saved_col_names
    assert "__col_col_1__2" in saved_col_names

    # Duplicated source column names make column stats ambiguous - the C++ layer rejects the request.
    with pytest.raises(UserInputException):
        lib.create_column_stats_experimental(sym)
    stats_keys = lib.library_tool().find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert not stats_keys


@pytest.mark.parametrize("index_name", ("index", "some-other-name"))
def test_column_stats_col_called_index(
    in_memory_store_factory, lib_name, encoding_version, any_output_format, index_name
):
    """Check some edge cases where the data column's name matches the index column. 'index' is used as an internal
    name for un-named indexes, so using it can expose some bugs."""
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_col_called_index"
    col_name = index_name or "index"
    df = pd.DataFrame({col_name: [0, 1]}, index=pd.date_range("2000-01-01", periods=2))
    lib.write(sym, df)

    lib.create_column_stats_experimental(sym)
    stats_keys = lib.library_tool().find_keys_for_symbol(KeyType.COLUMN_STATS, sym)
    assert len(stats_keys) == 1

    if index_name == "index":
        expected_title = "__col_index__0"
    else:
        expected_title = index_name
    res = lib.read_column_stats_experimental(sym)
    expected = pl.DataFrame(
        {
            "start_row": pl.Series([0], dtype=pl.UInt64),
            "end_row": pl.Series([2], dtype=pl.UInt64),
            f"v1_MIN({expected_title})": [0],
            f"v1_MAX({expected_title})": [1],
        }
    )
    assert_stats_equal(res, expected)

    # Now do some filtering. We apply query builder filters to the index column first!
    # We should make sure we haven't messed them up.
    if index_name == "index":
        q = QueryBuilder()
        q = q[q[index_name] == pd.Timestamp("2000-01-01")]
        res = lib.read(sym, query_builder=q).data
        assert res.shape == (1, 1)
        assert res.values == [[0]]


@pytest.mark.parametrize(
    "index_level_name, stored_col_name",
    [
        pytest.param("level", "__idx__level", id="named_level"),
        pytest.param(None, "__fkidx__1", id="unnamed_level"),
    ],
)
def test_column_stats_multiindex(
    in_memory_store_factory, lib_name, encoding_version, any_output_format, index_level_name, stored_col_name
):
    """Column stats on a multiindex DataFrame: auto-discovery picks up data columns and the inner
    index level; the outer/primary index is excluded (already pruned by the index)."""
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_multiindex"

    # Two writes to get multiple segments. With segment_row_size=2, each write produces one segment.
    dt1 = pd.Timestamp("2000-01-01")
    dt2 = pd.Timestamp("2000-01-01T12:00")
    dt3 = pd.Timestamp("2000-01-02")
    dt4 = pd.Timestamp("2000-01-02T12:00")
    mi0 = pd.MultiIndex.from_arrays(
        [[dt1, dt2], [10, 20]],
        names=["datetime", index_level_name],
    )
    df0 = pd.DataFrame({"val": [100, 200]}, index=mi0)

    mi1 = pd.MultiIndex.from_arrays(
        [[dt3, dt4], [30, 40]],
        names=["datetime", index_level_name],
    )
    df1 = pd.DataFrame({"val": [300, 400]}, index=mi1)

    lib.write(sym, df0)
    lib.append(sym, df1)

    # Check we're testing the right thing
    saved_col_names = lib.get_info(sym)["normalization_metadata"].df.common.col_names
    assert len(saved_col_names) == 2
    assert stored_col_name in saved_col_names
    assert "val" in saved_col_names

    full_df = pd.concat([df0, df1])

    # Auto-discovery picks up the data column and the inner index level (stored name), but not
    # the outer/primary index.
    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"val": {"MINMAX"}, stored_col_name: {"MINMAX"}}

    # Read and verify the stats
    stats = lib.read_column_stats_experimental(sym)
    expected = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(val)", [100, 300]),
        pl.Series("v1_MAX(val)", [200, 400]),
        pl.Series(f"v1_MIN({stored_col_name})", [10, 30]),
        pl.Series(f"v1_MAX({stored_col_name})", [20, 40]),
    )
    assert_stats_equal(stats, expected)

    # QueryBuilder filter on the data column
    q = QueryBuilder()
    q = q[q["val"] >= 300]
    result = lib.read(sym, query_builder=q).data
    expected_filtered = full_df[full_df["val"] >= 300]
    pd.testing.assert_frame_equal(result, expected_filtered)

    # QueryBuilder filter on the inner index level (by user-facing name) still returns correct
    # results now that stats are built over it.
    if index_level_name is not None:
        q = QueryBuilder()
        q = q[q[index_level_name] >= 30]
        result = lib.read(sym, query_builder=q).data
        expected_filtered = full_df[full_df.index.get_level_values(index_level_name) >= 30]
        pd.testing.assert_frame_equal(result, expected_filtered)

    # Drop all stats
    lib.drop_column_stats_experimental(sym)
    with pytest.raises(StorageException):
        lib.get_column_stats_info_experimental(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats_experimental(sym)


@pytest.mark.parametrize(
    "series_name, stored_col_name",
    [
        pytest.param("myval", "myval", id="named"),
        pytest.param(None, "0", id="unnamed"),
    ],
)
def test_column_stats_series(
    in_memory_store_factory, lib_name, encoding_version, any_output_format, series_name, stored_col_name
):
    """Column stats on a datetime-indexed pd.Series. A Series is normalized with input_type == "series"
    (not "df"); the outer/primary index must still be excluded (it is already pruned by the index
    mechanism) and only the single value column gets MINMAX stats."""
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_series"
    s0 = pd.Series([1, 2], index=pd.date_range("2000-01-01", periods=2), name=series_name)
    s1 = pd.Series([3, 4], index=pd.date_range("2000-01-03", periods=2), name=series_name)
    lib.write(sym, s0)
    lib.append(sym, s1)

    lib.create_column_stats_experimental(sym)
    # The outer datetime index is NOT included; only the value column.
    assert lib.get_column_stats_info_experimental(sym) == {stored_col_name: {"MINMAX"}}

    stats = lib.read_column_stats_experimental(sym)
    expected = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series(f"v1_MIN({stored_col_name})", [1, 3]),
        pl.Series(f"v1_MAX({stored_col_name})", [2, 4]),
    )
    assert_stats_equal(stats, expected)

    lib.drop_column_stats_experimental(sym)
    with pytest.raises(StorageException):
        lib.get_column_stats_info_experimental(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats_experimental(sym)


def test_column_stats_series_rangeindex(in_memory_store_factory, lib_name, encoding_version, any_output_format):
    """A RangeIndex pd.Series has no physically-stored index, so the single value column gets stats and
    nothing is treated as an index to exclude."""
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_series_rangeindex"
    lib.write(sym, pd.Series([1, 2], name="myval"))
    lib.append(sym, pd.Series([3, 4], name="myval"))

    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"myval": {"MINMAX"}}


def test_column_stats_string_indexed_symbol(in_memory_store_factory, lib_name):
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        name=lib_name,
    )
    sym = "test_column_stats_string_indexed_symbol"
    df0 = pd.DataFrame({"col_1": [1, 2]}, index=["a", "b"])
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=["c", "d"])
    lib.write(sym, df0)
    lib.append(sym, df1)
    # We don't support stats over strings yet, so no stats over the string index itself
    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df0["col_1"].min(), df1["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df0["col_1"].max(), df1["col_1"].max()]),
    )

    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}}

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)


@pytest.mark.parametrize(
    "index_level_name, stored_col_name",
    [
        pytest.param("lvl", "__idx__lvl", id="named_level"),
        pytest.param(None, "__fkidx__1", id="unnamed_level"),
    ],
)
def test_column_stats_series_multiindex(
    in_memory_store_factory, lib_name, encoding_version, any_output_format, index_level_name, stored_col_name
):
    """Column stats on a MultiIndex pd.Series: the outer/primary index is excluded, while the inner
    index level and the value column both get MINMAX stats (mirrors the multiindex DataFrame case)."""
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_series_multiindex"

    dt1 = pd.Timestamp("2000-01-01")
    dt2 = pd.Timestamp("2000-01-01T12:00")
    dt3 = pd.Timestamp("2000-01-02")
    dt4 = pd.Timestamp("2000-01-02T12:00")
    mi0 = pd.MultiIndex.from_arrays([[dt1, dt2], [10, 20]], names=["datetime", index_level_name])
    s0 = pd.Series([100, 200], index=mi0, name="val")
    mi1 = pd.MultiIndex.from_arrays([[dt3, dt4], [30, 40]], names=["datetime", index_level_name])
    s1 = pd.Series([300, 400], index=mi1, name="val")

    lib.write(sym, s0)
    lib.append(sym, s1)

    # The inner index level and the value column get stats, but not the outer/primary index.
    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"val": {"MINMAX"}, stored_col_name: {"MINMAX"}}

    stats = lib.read_column_stats_experimental(sym)
    expected = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(val)", [100, 300]),
        pl.Series("v1_MAX(val)", [200, 400]),
        pl.Series(f"v1_MIN({stored_col_name})", [10, 30]),
        pl.Series(f"v1_MAX({stored_col_name})", [20, 40]),
    )
    assert_stats_equal(stats, expected)


def test_column_stats_create_tiny_thread_pool(
    in_memory_store_factory, lib_name, encoding_version, any_output_format, tiny_thread_pool
):
    """Simple test with tiny thread pool to check against deadlocks from the parallel load of the index key
    and the column stats key."""
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_create_tiny_thread_pool"
    expected_column_stats = generate_symbol(lib, sym)

    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}

    column_stats = lib.read_column_stats_experimental(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_drop_tiny_thread_pool(
    in_memory_store_factory, lib_name, encoding_version, any_output_format, tiny_thread_pool
):
    """Simple test with tiny thread pool to check against deadlocks from the parallel load of the index key
    and the column stats key."""
    lib = in_memory_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_drop_tiny_thread_pool"
    generate_symbol(lib, sym)

    lib.create_column_stats_experimental(sym)
    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}

    lib.drop_column_stats_experimental(sym)
    with pytest.raises(StorageException):
        lib.get_column_stats_info_experimental(sym)


# k=0 is the kill switch: it admits every processing unit at once, disabling the memory bound.
@pytest.mark.parametrize("k", [0, 1, 2, 1000])
def test_column_stats_create_independent_of_admission_ceiling(
    version_store_factory, lib_name, encoding_version, any_output_format, k
):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}_{k}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_admission_ceiling"
    expected_column_stats = generate_symbol(lib, sym)

    with config_context(ADMISSION_KEY, k):
        lib.create_column_stats_experimental(sym)

    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected_column_stats)


@pytest.mark.parametrize("k", [1, 2])
def test_column_stats_create_admission_tiny_thread_pool(
    version_store_factory, lib_name, encoding_version, any_output_format, tiny_thread_pool, k
):
    """Test against deadlock with a small admission limit and single thread pools."""
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}_{k}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_admission_tiny_thread_pool"
    n_appends = 10
    write_many_slices(lib, sym, n_appends)

    with config_context(ADMISSION_KEY, k):
        lib.create_column_stats_experimental(sym)

    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    # One stats row per row slice.
    assert lib.read_column_stats_experimental(sym).num_rows == n_appends


def test_column_stats_create_single_unit(
    version_store_factory, lib_name, encoding_version, any_output_format, tiny_thread_pool
):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        encoding_version=int(encoding_version),
        lmdb_config={"map_size": 2**30},
        name=lib_name + f"_{encoding_version.name}",
    )
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_single_unit"
    df = pd.DataFrame({"col_1": [1.0, 3.0]}, index=pd.date_range("2000-01-01", periods=2))
    lib.write(sym, df)
    expected_column_stats = row_range_columns_to_pl(lib, sym).with_columns(
        pl.Series("v1_MIN(col_1)", [df["col_1"].min()]),
        pl.Series("v1_MAX(col_1)", [df["col_1"].max()]),
    )

    with config_context(ADMISSION_KEY, 1):
        lib.create_column_stats_experimental(sym)

    assert_stats_equal(lib.read_column_stats_experimental(sym), expected_column_stats)


def test_column_stats_duplicate_index_values_across_slice_boundary(
    in_memory_version_store_tiny_segment, column_stats_filtering_enabled
):
    lib = in_memory_version_store_tiny_segment
    sym = "test_column_stats_duplicate_index_values_across_slice_boundary"
    ts = pd.Timestamp("2000-01-01")
    lib.write(sym, pd.DataFrame({"col_1": [1, 2, 3, 4]}, index=[ts] * 4))

    # both share one (start_index, end_index)
    index_df = lib.read_index(sym).reset_index()
    assert len(index_df) == 2
    assert index_df["start_index"].nunique() == 1
    assert index_df["end_index"].nunique() == 1

    lib.create_column_stats_experimental(sym)

    column_stats = lib.read_column_stats_experimental(sym)
    assert column_stats.num_rows == 2
    expected_column_stats = pl.DataFrame(
        {
            "start_row": [0, 2],
            "end_row": [2, 4],
            "v1_MIN(col_1)": [1, 3],
            "v1_MAX(col_1)": [2, 4],
        }
    )
    assert_stats_equal(column_stats, expected_column_stats)

    # Both rows can be used for pruning
    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    result = lib.read(sym, query_builder=q).data
    pd.testing.assert_frame_equal(result, pd.DataFrame({"col_1": [3, 4]}, index=[ts] * 2))


def nine_row_symbol_df():
    # col_2's NaNs are distributed one/two/three per row slice (0,3)/(3,6)/(6,9)
    return pd.DataFrame(
        {
            "col_1": list(range(9)),
            "col_2": [10.0, 11.0, np.nan, np.nan, np.nan, 14.0, np.nan, np.nan, np.nan],
        },
        index=pd.date_range("2000-01-01", periods=9),
    )


def write_nine_row_symbol(lib, sym):
    df = nine_row_symbol_df()
    lib.write(sym, df)
    return df


def eleven_row_symbol_df():
    # At segment_row_size=3 the slices are (0,3)/(3,6)/(6,9)/(9,11), the last one ragged. col_2's
    # NaNs are one/two/three/none per slice, and (6,9) is entirely NaN so its min and max are NaN.
    return pd.DataFrame(
        {
            "col_1": list(range(11)),
            "col_2": [10.0, 11.0, np.nan, np.nan, np.nan, 14.0, np.nan, np.nan, np.nan, 19.0, 20.0],
        },
        index=pd.date_range("2000-01-01", periods=11),
    )


def write_eleven_row_symbol(lib, sym):
    df = eleven_row_symbol_df()
    lib.write(sym, df)
    return df


def jan(day):
    return pd.Timestamp(f"2000-01-{day:02d}")


def expected_row_range_stats(df, expected_slices):
    def slice_stats(col):
        values_by_slice = [df[col].iloc[s:e] for s, e in expected_slices]
        return (
            [values.min() for values in values_by_slice],
            [values.max() for values in values_by_slice],
            [int(values.isna().sum()) for values in values_by_slice],
        )

    col_1_min, col_1_max, col_1_nan = slice_stats("col_1")
    col_2_min, col_2_max, col_2_nan = slice_stats("col_2")
    null_counts = [0] * len(expected_slices)

    return pl.DataFrame(
        {
            "start_row": pl.Series([s for s, _ in expected_slices], dtype=pl.UInt64),
            "end_row": pl.Series([e for _, e in expected_slices], dtype=pl.UInt64),
            "v1_MIN(col_1)": pl.Series([int(v) for v in col_1_min], dtype=pl.Int64),
            "v1_MAX(col_1)": pl.Series([int(v) for v in col_1_max], dtype=pl.Int64),
            "v1_NAN_COUNT(col_1)": pl.Series(col_1_nan, dtype=pl.UInt64),
            "v1_NULL_COUNT(col_1)": pl.Series(null_counts, dtype=pl.UInt64),
            "v1_MIN(col_2)": pl.Series(col_2_min, dtype=pl.Float64),
            "v1_MAX(col_2)": pl.Series(col_2_max, dtype=pl.Float64),
            "v1_NAN_COUNT(col_2)": pl.Series(col_2_nan, dtype=pl.UInt64),
            "v1_NULL_COUNT(col_2)": pl.Series(null_counts, dtype=pl.UInt64),
        }
    )


@pytest.mark.parametrize(
    "row_range, expected_slices",
    [
        ((0, 3), [(0, 3)]),
        ((0, 4), [(0, 3), (3, 6)]),
        ((4, 5), [(3, 6)]),
        ((6, 9), [(6, 9)]),
        ((-9, -6), [(0, 3)]),
        ((None, 3), [(0, 3)]),
        ((6, None), [(6, 9)]),
        ((None, None), [(0, 3), (3, 6), (6, 9)]),
        ((None, -6), [(0, 3)]),
    ],
)
def test_column_stats_create_row_range(in_memory_store_factory, lib_name, row_range, expected_slices):
    lib = in_memory_store_factory(segment_row_size=3, name=lib_name)
    sym = "test_column_stats_create_row_range"
    df = write_nine_row_symbol(lib, sym)

    lib.create_column_stats_experimental(sym, row_range=row_range)

    assert_stats_equal(
        lib.read_column_stats_experimental(sym), expected_row_range_stats(df, expected_slices), check_dtypes=True
    )


@pytest.mark.parametrize(
    "date_range, expected_slices",
    [
        ((pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")), [(0, 3)]),
        ((pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-04")), [(0, 3), (3, 6)]),
        ((pd.Timestamp("2000-01-05"), pd.Timestamp("2000-01-05")), [(3, 6)]),
        ((pd.Timestamp("2000-01-07"), pd.Timestamp("2000-01-09")), [(6, 9)]),
    ],
)
def test_column_stats_create_date_range(in_memory_store_factory, lib_name, date_range, expected_slices):
    lib = in_memory_store_factory(segment_row_size=3, name=lib_name)
    sym = "test_column_stats_create_date_range"
    df = write_nine_row_symbol(lib, sym)

    lib.create_column_stats_experimental(sym, date_range=date_range)

    assert_stats_equal(
        lib.read_column_stats_experimental(sym), expected_row_range_stats(df, expected_slices), check_dtypes=True
    )


# We create an 11 row symbol with slices:
# [0, 3) , [3, 6), [6, 9), [9, 11)
# We check that after creating stats with a series of row ranges of the form [A, B)
# we get stats over the set of segments covering the union of those row ranges
# In particular you can end up calculating stats for up to N rows outside a row_range boundary,
# where N is the row slice size minus one.
@pytest.mark.parametrize(
    "row_ranges, expected_slices",
    [
        ([(0, 4), (3, 5)], [(0, 3), (3, 6)]),
        # Leave a hole over [6, 9)
        ([(0, 4), (9, 10)], [(0, 3), (3, 6), (9, 11)]),
        # Same, in the other order, checks sorting in the stats segment
        ([(9, 10), (0, 4)], [(0, 3), (3, 6), (9, 11)]),
        # Fill the hole
        ([(0, 4), (9, 10), (6, 7)], [(0, 3), (3, 6), (6, 9), (9, 11)]),
        # Repeating a range replaces its rows rather than duplicating them
        ([(0, 4), (0, 4)], [(0, 3), (3, 6)]),
        # You need all segments to cover (2,10)
        ([(0, 4), (2, 10)], [(0, 3), (3, 6), (6, 9), (9, 11)]),
        # [3, 6) is recalculated, [0, 3) is kept, [6, 9) is new
        ([(0, 4), (5, 9)], [(0, 3), (3, 6), (6, 9)]),
        # read_index output, one row_range per row-slice
        ([(0, 3), (3, 6), (6, 9), (9, 11)], [(0, 3), (3, 6), (6, 9), (9, 11)]),
        # read_index output coalesced into fewer, larger row_ranges
        ([(0, 6), (6, 11)], [(0, 3), (3, 6), (6, 9), (9, 11)]),
    ],
)
def test_column_stats_create_incrementally_by_row_range(in_memory_store_factory, lib_name, row_ranges, expected_slices):
    lib = in_memory_store_factory(segment_row_size=3, name=lib_name)
    sym = "sym"
    df = write_eleven_row_symbol(lib, sym)

    for row_range in row_ranges:
        lib.create_column_stats_experimental(sym, row_range=row_range)

    assert_stats_equal(
        lib.read_column_stats_experimental(sym), expected_row_range_stats(df, expected_slices), check_dtypes=True
    )


# As above, but by date range. The 11 row symbol is indexed 2000-01-01 to 2000-01-11, so the slices
# span [Jan 1, Jan 3], [Jan 4, Jan 6], [Jan 7, Jan 9], [Jan 10, Jan 11]. Unlike row ranges, date
# ranges are inclusive at both ends.
@pytest.mark.parametrize(
    "date_ranges, expected_slices",
    [
        ([(jan(1), jan(4)), (jan(4), jan(5))], [(0, 3), (3, 6)]),
        # Leave a hole over [6, 9)
        ([(jan(1), jan(4)), (jan(10), jan(11))], [(0, 3), (3, 6), (9, 11)]),
        # Same, in the other order, checks sorting in the stats segment
        ([(jan(10), jan(11)), (jan(1), jan(4))], [(0, 3), (3, 6), (9, 11)]),
        # Fill the hole
        ([(jan(1), jan(4)), (jan(10), jan(11)), (jan(7), jan(8))], [(0, 3), (3, 6), (6, 9), (9, 11)]),
        # Repeating a range replaces its rows rather than duplicating them
        ([(jan(1), jan(4)), (jan(1), jan(4))], [(0, 3), (3, 6)]),
        # You need all segments to cover [Jan 3, Jan 10]
        ([(jan(1), jan(4)), (jan(3), jan(10))], [(0, 3), (3, 6), (6, 9), (9, 11)]),
        # [3, 6) is recalculated, [0, 3) is kept, [6, 9) is new
        ([(jan(1), jan(4)), (jan(6), jan(9))], [(0, 3), (3, 6), (6, 9)]),
    ],
)
def test_column_stats_create_incrementally_by_date_range(
    in_memory_store_factory, lib_name, date_ranges, expected_slices
):
    lib = in_memory_store_factory(segment_row_size=3, name=lib_name)
    sym = "sym"
    df = write_eleven_row_symbol(lib, sym)

    for date_range in date_ranges:
        lib.create_column_stats_experimental(sym, date_range=date_range)

    assert_stats_equal(
        lib.read_column_stats_experimental(sym), expected_row_range_stats(df, expected_slices), check_dtypes=True
    )


def test_column_stats_create_dynamic_schema_partial_create_uses_descriptor_type(in_memory_store_factory, lib_name):
    """A range covering only the first row slice must still type MIN/MAX from the version's descriptor,
    not from that slice's own dtype"""
    lib = in_memory_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=True, name=lib_name)
    sym = "test_column_stats_create_dynamic_schema_partial_create_uses_descriptor_type"

    df0, df1 = dynamic_schema_types_changing_dfs()
    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats_experimental(sym, row_range=(0, 2))

    assert_dynamic_schema_types_changing_schema(pl.from_arrow(lib.read_column_stats_experimental(sym)).schema)


def test_column_stats_create_partial_coverage_with_column_slicing(in_memory_store_factory, lib_name):
    lib = in_memory_store_factory(column_group_size=2, segment_row_size=3, name=lib_name)
    sym = "test_column_stats_create_partial_coverage_with_column_slicing"
    df = pd.DataFrame(
        {f"col_{i}": np.arange(i * 100, i * 100 + 9) for i in range(1, 5)},
        index=pd.date_range("2000-01-01", periods=9),
    )
    lib.write(sym, df)

    lib.create_column_stats_experimental(sym, row_range=(0, 4))

    expected_slices = [(0, 3), (3, 6)]
    expected = pl.DataFrame(
        {
            "start_row": [s for s, _ in expected_slices],
            "end_row": [e for _, e in expected_slices],
            **{
                f"v1_MIN(col_{i})": [int(df[f"col_{i}"].iloc[s:e].min()) for s, e in expected_slices]
                for i in range(1, 5)
            },
            **{
                f"v1_MAX(col_{i})": [int(df[f"col_{i}"].iloc[s:e].max()) for s, e in expected_slices]
                for i in range(1, 5)
            },
        }
    )
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected)


def test_column_stats_create_empty_range_is_noop(in_memory_store_factory, lib_name):
    lib = in_memory_store_factory(segment_row_size=3, name=lib_name)
    sym = "test_column_stats_create_empty_range_is_noop"
    df = write_nine_row_symbol(lib, sym)

    lib.create_column_stats_experimental(sym, row_range=(2, 2))
    assert not lib.library_tool().find_keys_for_symbol(KeyType.COLUMN_STATS, sym)

    lib.create_column_stats_experimental(sym, row_range=(100, 105))
    assert not lib.library_tool().find_keys_for_symbol(KeyType.COLUMN_STATS, sym)

    lib.create_column_stats_experimental(sym, row_range=(0, 3))
    expected = expected_row_range_stats(df, [(0, 3)])
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected, check_dtypes=True)

    lib.create_column_stats_experimental(sym, row_range=(2, 2))
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected, check_dtypes=True)

    lib.create_column_stats_experimental(sym, row_range=(100, 105))
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected, check_dtypes=True)


@pytest.mark.parametrize(
    "reversed_range",
    [{"row_range": (5, 2)}, {"date_range": (jan(7), jan(2))}],
    ids=["row_range", "date_range"],
)
def test_column_stats_create_reversed_range_is_noop(in_memory_store_factory, lib_name, reversed_range):
    lib = in_memory_store_factory(segment_row_size=3, name=lib_name)
    sym = "test_column_stats_create_reversed_range_is_noop"
    df = write_nine_row_symbol(lib, sym)

    lib.create_column_stats_experimental(sym, **reversed_range)
    assert not lib.library_tool().find_keys_for_symbol(KeyType.COLUMN_STATS, sym)

    lib.create_column_stats_experimental(sym, row_range=(0, 3))
    expected = expected_row_range_stats(df, [(0, 3)])
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected, check_dtypes=True)

    lib.create_column_stats_experimental(sym, **reversed_range)
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected, check_dtypes=True)


def test_column_stats_create_dynamic_schema_preserves_stats_for_column_outside_the_range(
    in_memory_store_factory, lib_name
):
    """col_2 exists only in the second row slice, so recomputing only the first must carry its stats
    over from the stored segment rather than from anything the create just calculated."""
    lib = in_memory_store_factory(segment_row_size=3, dynamic_schema=True, name=lib_name)
    sym = "test_column_stats_create_dynamic_schema_preserves_stats_for_column_outside_the_range"
    lib.write(sym, pd.DataFrame({"col_1": [1, 2, 3]}, index=pd.date_range("2000-01-01", periods=3)))
    lib.append(sym, pd.DataFrame({"col_2": [4, 5, 6]}, index=pd.date_range("2000-01-04", periods=3)))

    expected = pl.DataFrame(
        {
            "start_row": pl.Series([0, 3], dtype=pl.UInt64),
            "end_row": pl.Series([3, 6], dtype=pl.UInt64),
            "v1_MIN(col_1)": pl.Series([1, None], dtype=pl.Int64),
            "v1_MAX(col_1)": pl.Series([3, None], dtype=pl.Int64),
            "v1_NAN_COUNT(col_1)": pl.Series([0, None], dtype=pl.UInt64),
            "v1_NULL_COUNT(col_1)": pl.Series([0, None], dtype=pl.UInt64),
            "v1_MIN(col_2)": pl.Series([None, 4], dtype=pl.Int64),
            "v1_MAX(col_2)": pl.Series([None, 6], dtype=pl.Int64),
            "v1_NAN_COUNT(col_2)": pl.Series([None, 0], dtype=pl.UInt64),
            "v1_NULL_COUNT(col_2)": pl.Series([None, 0], dtype=pl.UInt64),
        }
    )

    lib.create_column_stats_experimental(sym)
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected, check_dtypes=True)

    lib.create_column_stats_experimental(sym, row_range=(0, 3))
    assert_stats_equal(lib.read_column_stats_experimental(sym), expected, check_dtypes=True)
    assert lib.get_column_stats_info_experimental(sym) == {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}


def test_column_stats_create_date_range_and_row_range_both_specified_raises(in_memory_version_store_tiny_segment):
    lib = in_memory_version_store_tiny_segment
    sym = "test_column_stats_create_date_range_and_row_range_both_specified_raises"
    lib.write(sym, df0)

    with pytest.raises(ArcticNativeException):
        lib.create_column_stats_experimental(sym, date_range=(df0.index[0], df0.index[-1]), row_range=(0, 2))


def test_column_stats_create_date_range_non_timestamp_index_raises(in_memory_store_factory, lib_name):
    lib = in_memory_store_factory(segment_row_size=2, name=lib_name)
    sym = "test_column_stats_create_date_range_non_timestamp_index_raises"
    lib.write(sym, pd.Series([1, 2, 3, 4], name="myval"))

    with pytest.raises(UserInputException):
        lib.create_column_stats_experimental(sym, date_range=(pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-02")))


def test_column_stats_create_date_range_unsorted_raises(in_memory_store_factory, lib_name):
    lib = in_memory_store_factory(segment_row_size=2, name=lib_name)
    sym = "test_column_stats_create_date_range_unsorted_raises"
    unsorted_index = [pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-01")]
    lib.write(sym, pd.DataFrame({"col_1": [1, 2]}, index=unsorted_index))

    with pytest.raises(SortingException):
        lib.create_column_stats_experimental(sym, date_range=(pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")))
