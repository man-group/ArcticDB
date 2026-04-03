"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.extra.pandas import columns, data_frames

import polars as pl

from arcticdb import concat
from arcticdb.options import LibraryOptions, OutputFormat
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked
from arcticdb.util.test import assert_frame_equal_with_arrow_for_sparse
from arcticdb.version_store.processing import QueryBuilder

# In all tests in this file we test all string formats as part of a single table.
# Since arrow writes do not support writing categorical columns we write `cat_str_col` as `large_string`
# and then use `undictionarify_table` to convert it back to large string so we can compare.
STRING_FORMAT_PER_COLUMN = {
    "str_col": pa.string(),
    "large_str_col": pa.large_string(),
    "cat_str_col": pa.dictionary(pa.int32(), pa.large_string()),
}


def undictionarify_table(table):
    for i, name in enumerate(table.column_names):
        typ = table.column(i).type
        if pa.types.is_dictionary(typ):
            table = table.set_column(i, name, table.column(i).cast(typ.value_type))
    return table


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("use_query_builder", [True, False])
@pytest.mark.parametrize("row_range_start", list(range(15)))
@pytest.mark.parametrize("row_range_width", [0, 1, 2, 5, 7, 10, 14, 15])
def test_sparse_arrow_row_range(
    version_store_factory, dynamic_schema, use_query_builder, row_range_start, row_range_width
):
    lib = version_store_factory(segment_row_size=5, dynamic_schema=dynamic_schema)
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_sparse_arrow_row_range"
    # Three segments of 5 rows with complementary null patterns across dtypes
    str_data = [None, "a", None, "b", None, "c", None, None, None, None, None, None, "d", "e", "f"]
    table = pa.table(
        {
            "int_col": pa.array([1, None, 2, None, 3, 4, None, None, None, None, None, 5, 6, 7, 8], pa.int64()),
            "float_col": pa.array(
                [None, 1.0, None, 2.0, None, None, None, 3.0, None, None, 4.0, None, None, 5.0, None],
                pa.float64(),
            ),
            "bool_col": pa.array(
                [True, None, False, None, True, None, None, None, True, False, None, True, False, True, None],
                pa.bool_(),
            ),
            "str_col": pa.array(str_data, pa.string()),
            "large_str_col": pa.array(str_data, pa.large_string()),
            "cat_str_col": pa.array(str_data, pa.large_string()),
        }
    )
    lib.write(sym, table)
    row_range = (row_range_start, row_range_start + row_range_width)
    expected = table.slice(offset=row_range_start, length=row_range_width)
    if use_query_builder:
        q = QueryBuilder().row_range(row_range)
        received = lib.read(sym, query_builder=q, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
        received_pandas = lib.read(sym, query_builder=q, output_format="PANDAS").data
    else:
        received = lib.read(sym, row_range=row_range, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
        received_pandas = lib.read(sym, row_range=row_range, output_format="PANDAS").data
    assert expected.equals(undictionarify_table(received))
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("use_query_builder", [True, False])
@pytest.mark.parametrize("date_range_start", list(pd.date_range("2025-01-01", periods=15)))
@pytest.mark.parametrize("date_range_width", [0, 1, 2, 5, 7, 10, 14, 15])
def test_sparse_arrow_date_range(
    version_store_factory, dynamic_schema, use_query_builder, date_range_start, date_range_width
):
    lib = version_store_factory(segment_row_size=5, dynamic_schema=dynamic_schema)
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_sparse_arrow_date_range"
    # Three segments of 5 rows with complementary null patterns across dtypes
    dates = pd.date_range("2025-01-01", periods=15)
    str_data = [None, "a", None, "b", None, "c", None, None, None, None, None, None, "d", "e", "f"]
    table = pa.table(
        {
            "ts": pa.Array.from_pandas(dates, type=pa.timestamp("ns")),
            "int_col": pa.array([1, None, 2, None, 3, 4, None, None, None, None, None, 5, 6, 7, 8], pa.int64()),
            "float_col": pa.array(
                [None, 1.0, None, 2.0, None, None, None, 3.0, None, None, 4.0, None, None, 5.0, None],
                pa.float64(),
            ),
            "bool_col": pa.array(
                [True, None, False, None, True, None, None, None, True, False, None, True, False, True, None],
                pa.bool_(),
            ),
            "str_col": pa.array(str_data, pa.string()),
            "large_str_col": pa.array(str_data, pa.large_string()),
            "cat_str_col": pa.array(str_data, pa.large_string()),
        }
    )
    lib.write(sym, table, index_column="ts")
    date_range = (date_range_start, date_range_start + pd.Timedelta(days=date_range_width - 1))
    offset = (date_range_start - pd.Timestamp("2025-01-01")).days
    expected = table.slice(offset=offset, length=date_range_width)
    if use_query_builder:
        q = QueryBuilder().date_range(date_range)
        received = lib.read(sym, query_builder=q, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
        received_pandas = lib.read(sym, query_builder=q, output_format="PANDAS").data.reset_index()
    else:
        received = lib.read(sym, date_range=date_range, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
        received_pandas = lib.read(sym, date_range=date_range, output_format="PANDAS").data.reset_index()
    assert expected.equals(undictionarify_table(received))
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        columns(["float_col"], elements=st.floats(min_value=0, max_value=1000, allow_nan=False), fill=st.just(np.nan)),
    ),
    rows_per_slice=st.integers(2, 10),
    use_row_range=st.booleans(),
)
def test_sparse_arrow_hypothesis(lmdb_version_store_arrow, df, rows_per_slice, use_row_range):
    row_count = len(df)
    assume(row_count > 0)
    lib = lmdb_version_store_arrow
    lib._cfg.write_options.segment_row_size = rows_per_slice
    sym = "test_sparse_arrow_hypothesis"
    float_array = pa.Array.from_pandas(df["float_col"])
    # Derive int, bool, and str columns, propagating nulls from the float column
    vals = float_array.to_pylist()
    int_col = pa.array([None if v is None else round(v) for v in vals], pa.int64())
    bool_col = pa.array([None if v is None else v > 500.0 for v in vals], pa.bool_())
    str_data = [None if v is None else str(round(v)) for v in vals]
    str_col = pa.array(str_data, pa.string())
    large_str_col = pa.array(str_data, pa.large_string())
    cat_str_col = pa.array(str_data, pa.large_string())
    table = pa.table(
        {
            "float_col": float_array,
            "int_col": int_col,
            "bool_col": bool_col,
            "str_col": str_col,
            "large_str_col": large_str_col,
            "cat_str_col": cat_str_col,
        }
    )
    lib.write(sym, table)
    if use_row_range:
        row_range = (row_count // 3, (2 * row_count) // 3)
        expected = table.slice(offset=row_range[0], length=row_range[1] - row_range[0])
        received = lib.read(sym, row_range=row_range, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
        received_pandas = lib.read(sym, row_range=row_range, output_format="PANDAS").data
    else:
        expected = table
        received = lib.read(sym, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
        received_pandas = lib.read(sym, output_format="PANDAS").data
    assert expected.equals(undictionarify_table(received))
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


@pytest.mark.parametrize("column_group_size", [1, 2, 3])
@pytest.mark.parametrize("use_row_range", [False, True])
@pytest.mark.parametrize("index_column", [None, "ts"])
@pytest.mark.parametrize(
    "columns",
    [
        ["a", "c"],
        ["b"],
    ],
)
def test_sparse_column_selection(version_store_factory, column_group_size, use_row_range, index_column, columns):
    lib = version_store_factory(column_group_size=column_group_size)
    lib.set_output_format("pyarrow")
    lib._set_allow_arrow_input()
    sym = "test_sparse_col_selection"
    table = pa.table(
        {
            "a": pa.array([1, None, 3, None, 5], pa.int64()),
            "b": pa.array([None, 2.0, None, 4.0, None], pa.float64()),
            "c": pa.array([True, None, False, None, True], pa.bool_()),
        }
    )
    if index_column is not None:
        ts = pa.Array.from_pandas(pd.date_range("2025-01-01", periods=5), type=pa.timestamp("ns"))
        table = table.append_column(index_column, ts)
    lib.write(sym, table, index_column=index_column)
    row_range = (1, 4) if use_row_range else None
    # The index column should always be included in the result
    expected_columns = ([index_column] if index_column is not None else []) + columns
    expected_table = table.select(expected_columns)
    if use_row_range:
        expected_table = expected_table.slice(offset=1, length=3)
    received = lib.read(sym, columns=columns, row_range=row_range).data
    assert expected_table.equals(received)
    received_pandas = lib.read(sym, columns=columns, row_range=row_range, output_format="PANDAS").data
    if index_column is not None:
        received_pandas = received_pandas.reset_index()
    assert_frame_equal_with_arrow_for_sparse(expected_table, received_pandas)


class TestSparseArrowQueryBuilder:
    sym = "TestSparseArrowQueryBuilder"

    @pytest.fixture(autouse=True)
    def setup(self, lmdb_version_store_arrow):
        lib = lmdb_version_store_arrow
        self.table = pa.table(
            {
                "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=8), type=pa.timestamp("ns")),
                "int_col": pa.array([1, None, 3, None, 5, None, 7, None], pa.int64()),
                "float_col": pa.array([None, 2.0, None, 4.0, None, 6.0, None, 8.0], pa.float64()),
            }
        )
        lib.write(self.sym, self.table, index_column="ts")

    def test_filter_isnull(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q[q["int_col"].isnull()]
        received = lmdb_version_store_arrow.read(self.sym, query_builder=q).data
        mask = pc.is_null(self.table.column("int_col"))
        expected = self.table.filter(mask)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym, query_builder=q, output_format="PANDAS"
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)

    def test_filter_notnull(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q[q["int_col"].notnull()]
        received = lmdb_version_store_arrow.read(self.sym, query_builder=q).data
        mask = pc.is_valid(self.table.column("int_col"))
        expected = self.table.filter(mask)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym, query_builder=q, output_format="PANDAS"
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)

    def test_filter_equals(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q[q["int_col"] == 3]
        received = lmdb_version_store_arrow.read(self.sym, query_builder=q).data
        mask = pc.equal(self.table.column("int_col"), 3)
        expected = self.table.filter(mask)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym, query_builder=q, output_format="PANDAS"
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)

    def test_filter_greater_than(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q[q["int_col"] > 3]
        received = lmdb_version_store_arrow.read(self.sym, query_builder=q).data
        mask = pc.greater(self.table.column("int_col"), 3)
        expected = self.table.filter(mask)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym, query_builder=q, output_format="PANDAS"
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)

    @pytest.mark.parametrize(
        "values",
        [
            pytest.param([1, 5, 7], id="no_none"),
            pytest.param(
                [1, 5, 7, None],
                id="with_none",
                marks=pytest.mark.xfail(
                    reason="QueryBuilder with isin None can't be constructed (monday: 11460233570)",
                    raises=Exception,
                ),
            ),
        ],
    )
    def test_filter_isin(self, lmdb_version_store_arrow, values):
        q = QueryBuilder()
        q = q[q["int_col"].isin(values)]
        received = lmdb_version_store_arrow.read(self.sym, query_builder=q).data
        mask = pc.is_in(self.table.column("int_col"), pa.array(values, pa.int64()))
        expected = self.table.filter(mask)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym, query_builder=q, output_format="PANDAS"
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)

    def test_filter_combined_columns(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q[(q["int_col"].notnull()) & (q["float_col"].notnull())]
        received = lmdb_version_store_arrow.read(self.sym, query_builder=q).data
        assert len(received) == 0
        received_pandas = lmdb_version_store_arrow.read(self.sym, query_builder=q, output_format="PANDAS").data
        assert len(received_pandas) == 0

    def test_project_multiple_cols(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q.apply("sum", q["int_col"] + 10)
        q = q.apply("product", q["int_col"] * q["float_col"])
        received = lmdb_version_store_arrow.read(self.sym, query_builder=q).data
        sum = pc.add(self.table.column("int_col"), 10)
        product = pc.multiply(self.table.column("int_col"), self.table.column("float_col"))
        expected = self.table.append_column("sum", sum)
        expected = expected.append_column("product", product)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym, query_builder=q, output_format="PANDAS"
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)

    def test_filter_then_project(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q[q["float_col"].notnull()]
        q = q.apply("doubled", q["float_col"] * 2)
        received = lmdb_version_store_arrow.read(self.sym, query_builder=q).data
        mask = pc.is_valid(self.table.column("float_col"))
        filtered = self.table.filter(mask)
        doubled = pc.multiply(filtered.column("float_col"), 2)
        expected = filtered.append_column("doubled", doubled)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym, query_builder=q, output_format="PANDAS"
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)

    def test_date_range_with_filter(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q[q["int_col"].notnull()]
        received = lmdb_version_store_arrow.read(
            self.sym,
            date_range=(pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-06")),
            query_builder=q,
        ).data
        sliced = self.table.slice(1, 5)
        mask = pc.is_valid(sliced.column("int_col"))
        expected = sliced.filter(mask)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym,
            date_range=(pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-06")),
            query_builder=q,
            output_format="PANDAS",
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)

    def test_row_range_with_filter(self, lmdb_version_store_arrow):
        q = QueryBuilder()
        q = q[q["float_col"].notnull()]
        received = lmdb_version_store_arrow.read(
            self.sym,
            row_range=(2, 6),
            query_builder=q,
        ).data
        sliced = self.table.slice(2, 4)
        mask = pc.is_valid(sliced.column("float_col"))
        expected = sliced.filter(mask)
        assert expected.equals(received)
        received_pandas = lmdb_version_store_arrow.read(
            self.sym,
            row_range=(2, 6),
            query_builder=q,
            output_format="PANDAS",
        ).data.reset_index()
        assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


@pytest.mark.parametrize("write_sparse", [True, False])
@pytest.mark.parametrize("append_sparse", [True, False])
def test_sparse_append_roundtrip(lmdb_version_store_arrow, write_sparse, append_sparse):
    lib = lmdb_version_store_arrow
    sym = "test_sparse_append_roundtrip"
    if write_sparse:
        str_data_1 = [None, "b", None]
        t1 = pa.table(
            {
                "int_col": pa.array([1, None, 3], pa.int64()),
                "float_col": pa.array([None, 2.0, None], pa.float64()),
                "bool_col": pa.array([True, None, False], pa.bool_()),
                "str_col": pa.array(str_data_1, pa.string()),
                "large_str_col": pa.array(str_data_1, pa.large_string()),
                "cat_str_col": pa.array(str_data_1, pa.large_string()),
            }
        )
    else:
        str_data_1 = ["a", "b", "c"]
        t1 = pa.table(
            {
                "int_col": pa.array([1, 2, 3], pa.int64()),
                "float_col": pa.array([1.0, 2.0, 3.0], pa.float64()),
                "bool_col": pa.array([True, False, True], pa.bool_()),
                "str_col": pa.array(str_data_1, pa.string()),
                "large_str_col": pa.array(str_data_1, pa.large_string()),
                "cat_str_col": pa.array(str_data_1, pa.large_string()),
            }
        )
    if append_sparse:
        str_data_2 = ["d", None, "f"]
        t2 = pa.table(
            {
                "int_col": pa.array([None, 5, None], pa.int64()),
                "float_col": pa.array([4.0, None, 6.0], pa.float64()),
                "bool_col": pa.array([None, True, None], pa.bool_()),
                "str_col": pa.array(str_data_2, pa.string()),
                "large_str_col": pa.array(str_data_2, pa.large_string()),
                "cat_str_col": pa.array(str_data_2, pa.large_string()),
            }
        )
    else:
        str_data_2 = ["d", "e", "f"]
        t2 = pa.table(
            {
                "int_col": pa.array([4, 5, 6], pa.int64()),
                "float_col": pa.array([4.0, 5.0, 6.0], pa.float64()),
                "bool_col": pa.array([False, True, False], pa.bool_()),
                "str_col": pa.array(str_data_2, pa.string()),
                "large_str_col": pa.array(str_data_2, pa.large_string()),
                "cat_str_col": pa.array(str_data_2, pa.large_string()),
            }
        )
    lib.write(sym, t1)
    lib.append(sym, t2)
    received = lib.read(sym, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
    expected = pa.concat_tables([t1, t2])
    assert expected.equals(undictionarify_table(received))
    received_pandas = lib.read(sym, output_format="PANDAS").data
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


def test_sparse_append_all_nulls(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_sparse_append_all_nulls"
    t1 = pa.table({"col": pa.array([1, 2, 3], pa.int64())})
    t2 = pa.table({"col": pa.array([None, None, None], pa.int64())})
    lib.write(sym, t1)
    lib.append(sym, t2)
    received = lib.read(sym).data
    expected = pa.concat_tables([t1, t2])
    assert expected.equals(received)
    received_pandas = lib.read(sym, output_format="PANDAS").data
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


@pytest.mark.parametrize("write_sparse", [True, False])
@pytest.mark.parametrize("update_sparse", [True, False])
@pytest.mark.parametrize(
    "date_range",
    [
        pytest.param(None, id="no_date_range"),
        pytest.param((pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-05")), id="wider"),
        pytest.param(
            (pd.Timestamp("2025-01-03 12:00:00"), pd.Timestamp("2025-01-04")),
            id="narrower",
        ),
    ],
)
def test_sparse_update_roundtrip(lmdb_version_store_arrow, write_sparse, update_sparse, date_range):
    lib = lmdb_version_store_arrow
    sym = "test_sparse_update_roundtrip"
    dates = pd.date_range("2025-01-01", periods=6)
    update_dates = pd.date_range("2025-01-03", periods=2)  # replaces rows 2 and 3
    if write_sparse:
        write_str_data = [None, "b", None, "d", None, "f"]
        write_table = pa.table(
            {
                "ts": pa.Array.from_pandas(dates, type=pa.timestamp("ns")),
                "int_col": pa.array([1, None, 3, None, 5, 6], pa.int64()),
                "float_col": pa.array([None, 2.0, None, 4.0, None, None], pa.float64()),
                "bool_col": pa.array([True, None, False, None, True, False], pa.bool_()),
                "str_col": pa.array(write_str_data, pa.string()),
                "large_str_col": pa.array(write_str_data, pa.large_string()),
                "cat_str_col": pa.array(write_str_data, pa.large_string()),
            }
        )
    else:
        write_str_data = ["a", "b", "c", "d", "e", "f"]
        write_table = pa.table(
            {
                "ts": pa.Array.from_pandas(dates, type=pa.timestamp("ns")),
                "int_col": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
                "float_col": pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], pa.float64()),
                "bool_col": pa.array([True, False, True, False, True, False], pa.bool_()),
                "str_col": pa.array(write_str_data, pa.string()),
                "large_str_col": pa.array(write_str_data, pa.large_string()),
                "cat_str_col": pa.array(write_str_data, pa.large_string()),
            }
        )
    if update_sparse:
        update_str_data = ["C", None]
        update_table = pa.table(
            {
                "ts": pa.Array.from_pandas(update_dates, type=pa.timestamp("ns")),
                "int_col": pa.array([None, 40], pa.int64()),
                "float_col": pa.array([30.0, None], pa.float64()),
                "bool_col": pa.array([None, True], pa.bool_()),
                "str_col": pa.array(update_str_data, pa.string()),
                "large_str_col": pa.array(update_str_data, pa.large_string()),
                "cat_str_col": pa.array(update_str_data, pa.large_string()),
            }
        )
    else:
        update_str_data = ["C", "D"]
        update_table = pa.table(
            {
                "ts": pa.Array.from_pandas(update_dates, type=pa.timestamp("ns")),
                "int_col": pa.array([30, 40], pa.int64()),
                "float_col": pa.array([30.0, 40.0], pa.float64()),
                "bool_col": pa.array([False, True], pa.bool_()),
                "str_col": pa.array(update_str_data, pa.string()),
                "large_str_col": pa.array(update_str_data, pa.large_string()),
                "cat_str_col": pa.array(update_str_data, pa.large_string()),
            }
        )
    lib.write(sym, write_table, index_column="ts")
    lib.update(sym, update_table, index_column="ts", date_range=date_range)

    # Compute expected
    start = date_range[0] if date_range is not None else update_dates[0]
    end = date_range[1] if date_range is not None else update_dates[-1]
    write_ts = write_table.column("ts").to_pylist()
    update_ts = update_table.column("ts").to_pylist()
    write_before = write_table.filter(pa.array([t < start for t in write_ts]))
    write_after = write_table.filter(pa.array([t > end for t in write_ts]))
    applied_update = update_table.filter(pa.array([start <= t <= end for t in update_ts]))
    expected = pa.concat_tables([write_before, applied_update, write_after])

    received = lib.read(sym, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
    assert expected.equals(undictionarify_table(received))
    received_pandas = lib.read(sym, output_format="PANDAS").data.reset_index()
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


def test_sparse_update_all_nulls(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_sparse_update_all_nulls"
    dates = pd.date_range("2025-01-01", periods=4)
    write_table = pa.table(
        {
            "ts": pa.Array.from_pandas(dates, type=pa.timestamp("ns")),
            "col": pa.array([1, 2, 3, 4], pa.int64()),
        }
    )
    lib.write(sym, write_table, index_column="ts")
    update_dates = pd.date_range("2025-01-02", periods=2)
    update_table = pa.table(
        {
            "ts": pa.Array.from_pandas(update_dates, type=pa.timestamp("ns")),
            "col": pa.array([None, None], pa.int64()),
        }
    )
    lib.update(sym, update_table, index_column="ts")
    received = lib.read(sym).data
    expected = pa.table(
        {
            "ts": pa.Array.from_pandas(dates, type=pa.timestamp("ns")),
            "col": pa.array([1, None, None, 4], pa.int64()),
        }
    )
    assert expected.equals(received)
    received_pandas = lib.read(sym, output_format="PANDAS").data.reset_index()
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


def test_sparse_dynamic_schema_combined(version_store_factory):
    lib = version_store_factory(dynamic_schema=True, dynamic_strings=True)
    lib.set_output_format(OutputFormat.PYARROW)
    lib._set_allow_arrow_input()
    sym = "test_sparse_dynschema_combined"
    # Segment 1: int_col (sparse) + bool_col (sparse), no str_col
    t1 = pa.table(
        {
            "int_to_float_col": pa.array([1, None, 3], pa.int64()),
            "bool_col": pa.array([True, None, False], pa.bool_()),
        }
    )
    # Segment 2: int_col as float64 (sparse, upgrades int64 from segment 1) + str_col (sparse), no bool_col
    str_data_2 = ["d", None, "f"]
    t2 = pa.table(
        {
            "int_to_float_col": pa.array([None, 5.0, None], pa.float64()),
            "str_col": pa.array(str_data_2, pa.string()),
            "large_str_col": pa.array(str_data_2, pa.large_string()),
            "cat_str_col": pa.array(str_data_2, pa.large_string()),
        }
    )
    # Segment 3: only bool_col
    t3 = pa.table(
        {
            "bool_col": pa.array([None, False], pa.bool_()),
        }
    )
    lib.write(sym, t1)
    lib.append(sym, t2)
    lib.append(sym, t3)
    received = lib.read(sym, arrow_string_format_per_column=STRING_FORMAT_PER_COLUMN).data
    str_data_expected = [None, None, None, "d", None, "f", None, None]
    expected = pa.table(
        {
            "int_to_float_col": pa.array([1.0, None, 3.0, None, 5.0, None, None, None], pa.float64()),
            "bool_col": pa.array([True, None, False, None, None, None, None, False], pa.bool_()),
            "str_col": pa.array(str_data_expected, pa.string()),
            "large_str_col": pa.array(str_data_expected, pa.large_string()),
            "cat_str_col": pa.array(str_data_expected, pa.large_string()),
        }
    )
    assert expected.equals(undictionarify_table(received))
    received_pandas = lib.read(sym, output_format="PANDAS").data
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


@pytest.mark.parametrize(
    "write_type, append_type, result_type",
    [
        # Signed to signed
        pytest.param(pa.int8(), pa.int64(), pa.int64(), id="int8_to_int64"),
        pytest.param(pa.int64(), pa.int8(), pa.int64(), id="int64_to_int8"),
        # Unsigned to unsigned
        pytest.param(pa.uint8(), pa.uint32(), pa.uint32(), id="uint8_to_uint32"),
        # Unsigned to signed
        pytest.param(pa.uint8(), pa.int16(), pa.int16(), id="uint8_to_int16"),
        # Int to float
        pytest.param(pa.int16(), pa.float32(), pa.float32(), id="int16_to_float32"),
        pytest.param(pa.int32(), pa.float64(), pa.float64(), id="int32_to_float64"),
        # Float to float
        pytest.param(pa.float32(), pa.float64(), pa.float64(), id="float32_to_float64"),
    ],
)
@pytest.mark.parametrize("row_range", [None, (2, 6)])
def test_sparse_dynamic_schema_type_upgrade(version_store_factory, write_type, append_type, result_type, row_range):
    lib = version_store_factory(dynamic_schema=True)
    lib.set_output_format(OutputFormat.PYARROW)
    lib._set_allow_arrow_input()
    sym = "test_sparse_type_upgrade"

    def make_array(values, nulls, typ):
        cast = float if pa.types.is_floating(typ) else int
        return pa.array([None if n else cast(v) for v, n in zip(values, nulls)], type=typ)

    t1 = pa.table({"col": make_array([1, 2, 3, 4], [False, True, False, True], write_type)})
    t2 = pa.table({"col": make_array([10, 20, 30, 40], [True, False, True, False], append_type)})

    lib.write(sym, t1)
    lib.append(sym, t2)

    expected = pa.table(
        {
            "col": make_array(
                [1, 2, 3, 4, 10, 20, 30, 40], [False, True, False, True, True, False, True, False], result_type
            )
        }
    )
    if row_range is not None:
        expected = expected.slice(offset=row_range[0], length=row_range[1] - row_range[0])

    received = lib.read(sym, row_range=row_range).data
    assert received.schema.field("col").type == result_type
    assert expected.equals(received)
    received_pandas = lib.read(sym, row_range=row_range, output_format="PANDAS").data
    assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)


def _assert_polars_equal(received, expected, sort_by=None, count_columns=None, sort_columns=False):
    if sort_columns:
        # ArcticDB and polars may return aggregated columns in different order
        cols = sorted(received.columns)
        received = received.select(cols)
        expected = expected.select(cols)
    if sort_by:
        received = received.sort(sort_by)
        expected = expected.sort(sort_by)
    if count_columns:
        # Polars groupby behaves differently to arcticdb groupby in two ways:
        # - Polars uses uint32 for counts where arcticdb uses uint64
        # - Polars returns 0 vs arcticdb returns null for group filled with only `null`s
        for col in count_columns:
            expected = expected.with_columns(
                pl.when(pl.col(col) == 0).then(None).otherwise(pl.col(col)).cast(pl.UInt64).alias(col)
            )
    assert received.equals(expected), (
        f"DataFrames differ:\nreceived dtypes: {received.dtypes}\nexpected dtypes: {expected.dtypes}\n"
        f"received:\n{received}\nexpected:\n{expected}"
    )


def _check_query_result(lib, sym, q, expected, sort_by=None, count_columns=None, **read_kwargs):
    received = lib.read(sym, query_builder=q, **read_kwargs).data
    _assert_polars_equal(received, expected, sort_by=sort_by, count_columns=count_columns, sort_columns=True)
    received_pd = lib.read(sym, query_builder=q, output_format="PANDAS", **read_kwargs).data
    if sort_by:
        received_pd = received_pd.sort_index().reset_index()
        expected_pyarrow = expected.sort(sort_by).to_arrow()
    else:
        received_pd = received_pd.reset_index()
        expected_pyarrow = expected.to_arrow()
    assert_frame_equal_with_arrow_for_sparse(expected_pyarrow, received_pd, check_dtype=False, check_like=True)


class TestSparseArrowGroupBy:
    sym = "test_sparse_groupby"

    @pytest.fixture(autouse=True)
    def setup(self, version_store_factory):
        self.lib = version_store_factory(segment_row_size=3)
        self.lib.set_output_format(OutputFormat.POLARS)
        self.lib._set_allow_arrow_input()
        self.table = pa.table(
            {
                "group_str": pa.array(["a", "a", "b", "b", "b", "a", "all_null", "all_null"], pa.large_string()),
                "group_int": pa.array([1, 1, 2, 2, 2, 1, 3, 3], pa.int64()),
                "int_col": pa.array([10, None, 30, None, 50, None, None, None], pa.int64()),
                "float_col": pa.array([None, 2.0, None, 4.0, None, 6.0, None, None], pa.float64()),
                "bool_col": pa.array([True, None, False, None, True, None, None, None], pa.bool_()),
            }
        )
        self.lib.write(self.sym, self.table)
        self.pldf = pl.from_arrow(self.table)

    @pytest.mark.parametrize("agg_op", ["sum", "mean", "min", "max", "count"])
    @pytest.mark.parametrize("group_col", ["group_str", "group_int"])
    @pytest.mark.parametrize("agg_col", ["int_col", "float_col", "bool_col"])
    def test_single_agg(self, agg_op, group_col, agg_col):
        q = QueryBuilder().groupby(group_col).agg({agg_col: agg_op})
        expected = self.pldf.group_by(group_col).agg(getattr(pl.col(agg_col), agg_op)())
        count_columns = [agg_col] if agg_op == "count" else None
        _check_query_result(self.lib, self.sym, q, expected, sort_by=group_col, count_columns=count_columns)

    @pytest.mark.parametrize("group_col", ["group_str", "group_int"])
    def test_named_aggs(self, group_col):
        agg_dict = {
            "int_sum": ("int_col", "sum"),
            "int_max": ("int_col", "max"),
            "float_mean": ("float_col", "mean"),
            "float_count": ("float_col", "count"),
        }
        q = QueryBuilder().groupby(group_col).agg(agg_dict)
        exprs = [getattr(pl.col(col), op)().alias(name) for name, (col, op) in agg_dict.items()]
        expected = self.pldf.group_by(group_col).agg(exprs)
        count_columns = [name for name, (_, op) in agg_dict.items() if op == "count"]
        _check_query_result(self.lib, self.sym, q, expected, sort_by=group_col, count_columns=count_columns)


@pytest.mark.xfail(
    reason="Resample rejects sparse columns. (monday ref: 11679866800)",
    raises=Exception,
)
class TestSparseArrowResample:
    sym = "test_sparse_resample"

    @pytest.fixture(autouse=True)
    def setup(self, version_store_factory):
        self.lib = version_store_factory(segment_row_size=4)
        self.lib.set_output_format(OutputFormat.POLARS)
        self.lib._set_allow_arrow_input()
        dates = pd.date_range("2025-01-01", periods=12, freq="h")
        # Bucket 2 (hours 6-8) is fully null for both columns when resampled at 3h
        self.table = pa.table(
            {
                "ts": pa.Array.from_pandas(dates, type=pa.timestamp("ns")),
                "int_col": pa.array([1, None, 3, None, 5, None, None, None, None, None, 11, None], pa.int64()),
                "float_col": pa.array(
                    [None, 2.0, None, 4.0, None, 6.0, None, None, None, 10.0, None, 12.0], pa.float64()
                ),
            }
        )
        self.lib.write(self.sym, self.table, index_column="ts")
        self.pldf = pl.from_arrow(self.table).sort("ts")

    @pytest.mark.parametrize("agg_op", ["sum", "mean", "min", "max", "first", "last", "count"])
    @pytest.mark.parametrize("agg_col", ["int_col", "float_col"])
    @pytest.mark.parametrize("rule", ["3h", "6h"])
    def test_single_agg(self, agg_op, agg_col, rule):
        q = QueryBuilder().resample(rule).agg({agg_col: agg_op})
        expected = self.pldf.group_by_dynamic("ts", every=rule).agg(getattr(pl.col(agg_col), agg_op)())
        _check_query_result(self.lib, self.sym, q, expected)

    @pytest.mark.parametrize("rule", ["3h", "6h"])
    def test_named_aggs(self, rule):
        agg_dict = {
            "int_sum": ("int_col", "sum"),
            "float_max": ("float_col", "max"),
            "int_count": ("int_col", "count"),
            "float_first": ("float_col", "first"),
        }
        q = QueryBuilder().resample(rule).agg(agg_dict)
        exprs = [getattr(pl.col(col), op)().alias(name) for name, (col, op) in agg_dict.items()]
        expected = self.pldf.group_by_dynamic("ts", every=rule).agg(exprs)
        _check_query_result(self.lib, self.sym, q, expected)

    @pytest.mark.parametrize("closed", ["left", "right"])
    @pytest.mark.parametrize("label", ["left", "right"])
    def test_closed_label(self, closed, label):
        q = QueryBuilder().resample("3h", closed=closed, label=label).agg({"int_col": "sum"})
        expected = self.pldf.group_by_dynamic("ts", every="3h", closed=closed, label=label).agg(pl.col("int_col").sum())
        _check_query_result(self.lib, self.sym, q, expected)

    def test_with_date_range(self):
        start, end = pd.Timestamp("2025-01-01 03:00"), pd.Timestamp("2025-01-01 08:00")
        q = QueryBuilder().resample("3h").agg({"int_col": "sum"})
        sliced = self.pldf.filter(pl.col("ts").is_between(start, end, closed="both"))
        expected = sliced.group_by_dynamic("ts", every="3h").agg(pl.col("int_col").sum())
        _check_query_result(self.lib, self.sym, q, expected, date_range=(start, end))


class TestSparseArrowConcat:
    @pytest.fixture(autouse=True)
    def setup(self, lmdb_library_factory):
        self.lib = lmdb_library_factory(LibraryOptions())
        self.lib._nvs.set_output_format(OutputFormat.POLARS)
        self.lib._nvs._set_allow_arrow_input()

    @pytest.mark.parametrize(
        "t1,t2",
        [
            (
                pa.table(
                    {
                        "int_col": pa.array([1, None, 3], pa.int64()),
                        "float_col": pa.array([None, 2.0, None], pa.float64()),
                    }
                ),
                pa.table(
                    {
                        "int_col": pa.array([None, 5, None], pa.int64()),
                        "float_col": pa.array([4.0, None, 6.0], pa.float64()),
                    }
                ),
            ),
            (
                pa.table(
                    {"a": pa.array([None, None, None], pa.int64()), "b": pa.array([1.0, None, 3.0], pa.float64())}
                ),
                pa.table({"a": pa.array([4, None, 6], pa.int64()), "b": pa.array([None, None, None], pa.float64())}),
            ),
            (
                pa.table(
                    {
                        "bool_col": pa.array([True, None, False], pa.bool_()),
                        "str_col": pa.array([None, "b", None], pa.large_string()),
                    }
                ),
                pa.table(
                    {
                        "bool_col": pa.array([None, True, None], pa.bool_()),
                        "str_col": pa.array(["d", None, "f"], pa.large_string()),
                    }
                ),
            ),
        ],
        ids=["int_float", "all_sparse", "bool_string"],
    )
    def test_basic_concat(self, t1, t2):
        self.lib.write("sym1", t1)
        self.lib.write("sym2", t2)
        received = concat(self.lib.read_batch(["sym1", "sym2"], lazy=True)).collect().data
        expected = pl.concat([pl.from_arrow(t1), pl.from_arrow(t2)])
        _assert_polars_equal(received, expected)

    @pytest.mark.parametrize("join", ["inner", "outer"])
    @pytest.mark.parametrize(
        "type1,type2",
        [
            pytest.param(pa.float64(), pa.float64(), id="same_type"),
            pytest.param(pa.int32(), pa.int64(), id="int32_and_int64"),
            pytest.param(pa.int16(), pa.float32(), id="int16_and_float32"),
            pytest.param(pa.int64(), pa.float32(), id="int64_and_float32"),
        ],
    )
    def test_different_columns(self, join, type1, type2):
        t1 = pa.table({"a": pa.array([1, None], pa.int64()), "b": pa.array([None, 2], type1)})
        t2 = pa.table({"b": pa.array([3, None], type2), "c": pa.array([None, 4], pa.int64())})
        self.lib.write("s1", t1)
        self.lib.write("s2", t2)
        received = concat(self.lib.read_batch(["s1", "s2"], lazy=True), join).collect().data
        pl1 = pl.from_arrow(t1)
        pl2 = pl.from_arrow(t2)
        if join == "outer":
            expected = pl.concat([pl1, pl2], how="diagonal_relaxed")
        else:
            common = set(t1.column_names) & set(t2.column_names)
            expected = pl.concat([pl1.select(common), pl2.select(common)], how="vertical_relaxed")
        _assert_polars_equal(received, expected)

    def test_concat_with_index(self):
        dates1 = pd.date_range("2025-01-01", periods=3, freq="h")
        dates2 = pd.date_range("2025-01-01T03:00:00", periods=3, freq="h")
        t1 = pa.table(
            {
                "ts": pa.Array.from_pandas(dates1, type=pa.timestamp("ns")),
                "val": pa.array([1, None, 3], pa.int64()),
            }
        )
        t2 = pa.table(
            {
                "ts": pa.Array.from_pandas(dates2, type=pa.timestamp("ns")),
                "val": pa.array([None, 5, None], pa.int64()),
            }
        )
        self.lib.write("s1", t1, index_column="ts")
        self.lib.write("s2", t2, index_column="ts")
        received = concat(self.lib.read_batch(["s1", "s2"], lazy=True)).collect().data
        expected = pl.concat([pl.from_arrow(t1), pl.from_arrow(t2)])
        _assert_polars_equal(received, expected)

    @pytest.mark.xfail(
        reason="Resample rejects sparse columns: sorted_aggregation.cpp 'Cannot aggregate column as it is sparse'",
        raises=Exception,
    )
    def test_concat_with_resample(self):
        dates1 = pd.date_range("2025-01-01", periods=6, freq="h")
        dates2 = pd.date_range("2025-01-01T06:00:00", periods=6, freq="h")
        t1 = pa.table(
            {
                "ts": pa.Array.from_pandas(dates1, type=pa.timestamp("ns")),
                "val": pa.array([1, None, 3, None, 5, None], pa.int64()),
            }
        )
        t2 = pa.table(
            {
                "ts": pa.Array.from_pandas(dates2, type=pa.timestamp("ns")),
                "val": pa.array([None, 8, None, 10, None, 12], pa.int64()),
            }
        )
        self.lib.write("r1", t1, index_column="ts")
        self.lib.write("r2", t2, index_column="ts")
        received = (
            concat(self.lib.read_batch(["r1", "r2"], lazy=True)).resample("3h").agg({"val": "sum"}).collect().data
        )
        combined = pl.concat([pl.from_arrow(t1), pl.from_arrow(t2)]).sort("ts")
        expected = combined.group_by_dynamic("ts", every="3h").agg(pl.col("val").sum())
        _assert_polars_equal(received, expected, sort_by="ts")
