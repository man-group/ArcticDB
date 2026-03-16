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

from arcticdb.options import OutputFormat
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
@pytest.mark.parametrize(
    "columns",
    [
        ["a", "c"],
        pytest.param(
            ["b"],
            marks=pytest.mark.xfail(
                reason="Column selection for Arrow-written data always includes the first column even when not requested (monday: 11432991054)"
            ),
        ),
    ],
)
def test_sparse_column_selection(version_store_factory, column_group_size, use_row_range, columns):
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
    lib.write(sym, table)
    row_range = (1, 4) if use_row_range else None
    expected_table = table.select(columns)
    if use_row_range:
        expected_table = expected_table.slice(offset=1, length=3)
    received = lib.read(sym, columns=columns, row_range=row_range).data
    assert expected_table.equals(received)
    received_pandas = lib.read(sym, columns=columns, row_range=row_range, output_format="PANDAS").data
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
            marks=pytest.mark.xfail(
                reason="update with date_range narrower than update table fails (monday: 18053263894)"
            ),
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
    # TODO: Reading as pandas populates the missing values of Segment 1 `int_to_float_col` with 0.0 instead of NaN (monday: 11458614876)
    # received_pandas = lib.read(sym, output_format="PANDAS").data
    # assert_frame_equal_with_arrow_for_sparse(expected, received_pandas)
