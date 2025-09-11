import pandas as pd
import numpy as np
import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.extra.numpy import unsigned_integer_dtypes, integer_dtypes, floating_dtypes
from hypothesis.extra.pandas import columns, data_frames

from arcticdb.util.test import assert_frame_equal, assert_frame_equal_with_arrow, stringify_dictionary_encoded_columns
from arcticdb.exceptions import SchemaException
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.options import OutputFormat
import pyarrow as pa
import pyarrow.compute as pc
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    ENDIANNESS,
    supported_string_dtypes,
    dataframe_strategy,
    column_strategy,
)
from arcticdb.util.test import get_sample_dataframe, make_dynamic
from arcticdb_ext.storage import KeyType
from tests.util.mark import WINDOWS


def test_basic(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


def test_basic_with_index(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


def test_basic_small_slices(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


def test_basic_small_slices_with_index(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    df = pd.DataFrame({"x": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


def test_double_columns(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


def test_bool_columns(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": [i % 3 == 0 for i in range(10)], "y": [i % 2 == 0 for i in range(10)]})
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


def test_column_filtering(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    table = lib.read("arrow", columns=["y"]).data
    df = df.drop("x", axis=1)
    assert_frame_equal_with_arrow(table, df)


@pytest.mark.parametrize(
    "dynamic_strings",
    [True, pytest.param(False, marks=pytest.mark.xfail(reason="Arrow fixed strings are not normalized correctly"))],
)
def test_strings_basic(lmdb_version_store_arrow, dynamic_strings):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": ["mene", "mene", "tekel", "upharsin"]})
    lib.write("arrow", df, dynamic_strings=dynamic_strings)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


@pytest.mark.parametrize("row_range", [None, (2, 3), (2, 4), (2, 5), (2, 6), (3, 4), (3, 5), (3, 6)])
def test_strings_with_nones_and_nans(lmdb_version_store_tiny_segment, row_range):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    # lmdb_version_store_tiny_segment has 2 rows per segment
    # This column is constructed so that every 2-element permutation of strings, Nones, and NaNs are tested
    df = pd.DataFrame(
        {
            "x": [
                "a",
                "b",
                "c",
                None,
                None,
                "d",
                "e",
                np.nan,
                np.nan,
                "f",
                None,
                None,
                None,
                np.nan,
                np.nan,
                None,
                np.nan,
                np.nan,
            ]
        }
    )
    lib.write("arrow", df, dynamic_strings=True)
    table = lib.read("arrow", row_range=row_range).data
    expected = lib.read("arrow", row_range=row_range, output_format=OutputFormat.PANDAS).data
    assert_frame_equal_with_arrow(table, expected)


@pytest.mark.skipif(WINDOWS, reason="Fixed-width string columns not supported on Windows")
def test_fixed_width_strings(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_fixed_width_strings"
    df0 = pd.DataFrame({"my_column": ["hello", "goodbye"]})
    lib.write(sym, df0, dynamic_strings=False)
    with pytest.raises(SchemaException) as e:
        lib.read(sym)
    assert "my_column" in str(e.value) and "Arrow" in str(e.value)


@pytest.mark.parametrize(
    "dynamic_strings",
    [True, pytest.param(False, marks=pytest.mark.xfail(reason="Arrow fixed strings are not normalized correctly"))],
)
def test_strings_multiple_segments_and_columns(lmdb_version_store_tiny_segment, dynamic_strings):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    df = pd.DataFrame(
        {
            "x": [f"x_{i//2}" for i in range(100)],
            "x_copy": [f"x_{i//2}" for i in range(100)],
            "y": [f"y_{i}" for i in range(100)],
            "z": [f"z_{i//5}" for i in range(100)],
        }
    )
    lib.write("arrow", df, dynamic_strings=dynamic_strings)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


# TODO: Fix unicode strings on windows
@pytest.mark.skipif(WINDOWS, reason="Unicode arrow strings fail on windows")
def test_all_types(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    # sample dataframe contains all dtypes + unicode strings
    df = get_sample_dataframe()
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert_frame_equal_with_arrow(table, df)


@pytest.mark.parametrize("date_range_start", [0, 1, 2, 3, 4, 5, 6])
@pytest.mark.parametrize("date_range_width", [0, 1, 2, 3])
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_date_range_corner_cases(version_store_factory, date_range_start, date_range_width, dynamic_schema):
    lib = version_store_factory(segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    df = pd.DataFrame(
        data={"col1": np.arange(7), "col2": np.arange(7), "col3": np.arange(7)},
        index=pd.date_range(pd.Timestamp(0), freq="ns", periods=7),
    )
    sym = "test_date_range_corner_cases"
    lib.write(sym, df)

    query_start_ts = pd.Timestamp(date_range_start)
    query_end_ts = pd.Timestamp(date_range_start + date_range_width)

    date_range = (query_start_ts, query_end_ts)
    expected_df = lib.read(sym, date_range=date_range, output_format=OutputFormat.PANDAS).data
    data_closed_table = lib.read(sym, date_range=date_range).data
    assert_frame_equal_with_arrow(expected_df, data_closed_table)


def test_date_range_between_index_values(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    df = pd.DataFrame(data={"col1": np.arange(2)}, index=[pd.Timestamp(0), pd.Timestamp(10)])
    sym = "test_date_range_between_index_values"
    lib.write(sym, df)

    date_range = (pd.Timestamp(4), pd.Timestamp(5))
    expected_df = lib.read(sym, date_range=date_range, output_format=OutputFormat.PANDAS).data
    data_closed_table = lib.read(sym, date_range=date_range).data
    assert_frame_equal_with_arrow(expected_df, data_closed_table)


@pytest.mark.parametrize("date_range_start", [-5, 10])
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_date_range_empty_result(version_store_factory, date_range_start, dynamic_schema):
    lib = version_store_factory(segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    df = pd.DataFrame(
        data={"col1": np.arange(7), "col2": np.arange(7), "col3": [f"{i}" for i in range(7)]},
        index=pd.date_range(pd.Timestamp(0), freq="ns", periods=7),
    )
    sym = "test_date_range_empty_result"
    lib.write(sym, df)

    query_start_ts = pd.Timestamp(date_range_start)
    query_end_ts = pd.Timestamp(date_range_start + 1)

    date_range = (query_start_ts, query_end_ts)
    expected_df = lib.read(sym, date_range=date_range, output_format=OutputFormat.PANDAS).data
    data_closed_table = lib.read(sym, date_range=date_range).data
    assert_frame_equal_with_arrow(expected_df, data_closed_table)


@pytest.mark.parametrize("segment_row_size", [1, 2, 10, 100])
@pytest.mark.parametrize("start_offset,end_offset", [(2, 3), (3, 75), (4, 32), (0, 99), (7, 56)])
def test_date_range(version_store_factory, segment_row_size, start_offset, end_offset):
    lib = version_store_factory(segment_row_size=segment_row_size, dynamic_strings=True)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(
        {"numeric": np.arange(100), "strings": [f"{i}" for i in range(100)]},
        index=pd.date_range(initial_timestamp, periods=100),
    )
    sym = "arrow_date_test"
    lib.write(sym, df)

    query_start_ts = initial_timestamp + pd.DateOffset(start_offset)
    query_end_ts = initial_timestamp + pd.DateOffset(end_offset)

    date_range = (query_start_ts, query_end_ts)
    data_closed_table = lib.read(sym, date_range=date_range).data
    df = data_closed_table.to_pandas()
    assert query_start_ts == df.index[0]
    assert query_end_ts == df.index[-1]
    assert df["numeric"].iloc[0] == start_offset
    assert df["numeric"].iloc[-1] == end_offset
    assert df["strings"].iloc[0] == f"{start_offset}"
    assert df["strings"].iloc[-1] == f"{end_offset}"


@pytest.mark.parametrize("segment_row_size", [1, 2, 10, 100])
@pytest.mark.parametrize("start_date,end_date", [(1, 1), (1, 4), (1, 5), (3, 7), (6, 7), (6, 10), (7, 7)])
def test_date_range_with_duplicates(version_store_factory, segment_row_size, start_date, end_date):
    lib = version_store_factory(segment_row_size=segment_row_size)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    index_with_duplicates = (
        [pd.Timestamp(2025, 1, 1)] * 10
        + [pd.Timestamp(2025, 1, 2)] * 13
        + [pd.Timestamp(2025, 1, 5)] * 5
        + [pd.Timestamp(2025, 1, 6)] * 1
        + [pd.Timestamp(2025, 1, 7)] * 25
    )
    size = len(index_with_duplicates)
    df = pd.DataFrame(data=np.arange(size, dtype=np.int64), index=index_with_duplicates, columns=["x"])
    sym = "arrow_date_test"
    lib.write(sym, df)

    query_start_ts = pd.Timestamp(2025, 1, start_date)
    query_end_ts = pd.Timestamp(2025, 1, end_date)

    date_range = (query_start_ts, query_end_ts)
    arrow_table = lib.read(sym, date_range=date_range).data
    expected_df = df[(df.index >= query_start_ts) & (df.index <= query_end_ts)]
    assert_frame_equal_with_arrow(arrow_table, expected_df)


@pytest.mark.parametrize("row_range_start", [0, 1, 2, 3, 4, 5, 6])
@pytest.mark.parametrize("row_range_width", [0, 1, 2, 3])
@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("index", [None, pd.date_range(pd.Timestamp(0), freq="ns", periods=7)])
def test_row_range_corner_cases(version_store_factory, row_range_start, row_range_width, dynamic_schema, index):
    lib = version_store_factory(segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    df = pd.DataFrame(data={"col1": np.arange(7), "col2": np.arange(7), "col3": np.arange(7)}, index=index)
    sym = "test_row_range_corner_cases"
    lib.write(sym, df)

    row_range = (row_range_start, row_range_start + row_range_width + 1)
    expected_df = lib.read(sym, row_range=row_range, output_format=OutputFormat.PANDAS).data
    data_closed_table = lib.read(sym, row_range=row_range).data
    assert_frame_equal_with_arrow(expected_df, data_closed_table)


@pytest.mark.parametrize("row_range_start", [-10, 10])
@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("index", [None, pd.date_range(pd.Timestamp(0), freq="ns", periods=7)])
def test_row_range_empty_result(version_store_factory, row_range_start, dynamic_schema, index):
    lib = version_store_factory(segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    df = pd.DataFrame(
        data={"col1": np.arange(7), "col2": np.arange(7), "col3": [f"{i}" for i in range(7)]}, index=index
    )
    sym = "test_row_range_empty_result"
    lib.write(sym, df)

    row_range = (row_range_start, row_range_start + 1)
    expected_df = lib.read(sym, row_range=row_range, output_format=OutputFormat.PANDAS).data
    data_closed_table = lib.read(sym, row_range=row_range).data
    assert_frame_equal_with_arrow(expected_df, data_closed_table)


@pytest.mark.parametrize("segment_row_size", [1, 2, 10, 100])
@pytest.mark.parametrize("start_offset,end_offset", [(2, 4), (3, 76), (4, 33), (0, 100), (7, 57)])
def test_row_range(version_store_factory, segment_row_size, start_offset, end_offset):
    lib = version_store_factory(segment_row_size=segment_row_size, dynamic_strings=True)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(
        {"numeric": np.arange(100), "strings": [f"{i}" for i in range(100)]},
        index=pd.date_range(initial_timestamp, periods=100),
    )
    sym = "arrow_date_test"
    lib.write(sym, df)

    row_range = (start_offset, end_offset)
    data_closed_table = lib.read(sym, row_range=row_range).data
    df = data_closed_table.to_pandas()

    start_ts = initial_timestamp + pd.DateOffset(start_offset)
    end_ts = initial_timestamp + pd.DateOffset(end_offset - 1)
    assert start_ts == df.index[0]
    assert end_ts == df.index[-1]
    assert df["numeric"].iloc[0] == start_offset
    assert df["numeric"].iloc[-1] == end_offset - 1
    assert df["strings"].iloc[0] == f"{start_offset}"
    assert df["strings"].iloc[-1] == f"{end_offset - 1}"


def test_with_querybuilder(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    q = QueryBuilder()
    q = q[q["x"] < 5]
    lib.write("arrow", df)
    table = lib.read("arrow", query_builder=q).data
    expected = df[df["x"] < 5]
    assert_frame_equal_with_arrow(table, expected)


def test_arrow_layout(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    lib_tool = lib.library_tool()
    num_rows = 100
    df = pd.DataFrame(
        data={"int": np.arange(num_rows, dtype=np.int64), "str": [f"x_{i//3}" for i in range(num_rows)]},
        index=pd.date_range(pd.Timestamp(0), periods=num_rows),
    )
    lib.write("sym", df, dynamic_strings=True)
    data_keys = lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")
    assert len(data_keys) == num_rows // 2

    arrow_table = lib.read("sym").data
    batches = arrow_table.to_batches()
    assert len(batches) == num_rows // 2
    for record_batch in batches:
        index_arr, int_arr, str_arr = record_batch.columns
        assert index_arr.type == pa.timestamp("ns")
        assert int_arr.type == pa.int64()
        assert str_arr.type == pa.dictionary(pa.int32(), pa.large_string())


@pytest.mark.parametrize(
    "first_type",
    [
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.float32(),
        pa.float64(),
    ],
)
@pytest.mark.parametrize(
    "second_type",
    [
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.float32(),
        pa.float64(),
    ],
)
def test_arrow_dynamic_schema_changing_types(lmdb_version_store_dynamic_schema_v1, first_type, second_type):
    if (pa.types.is_uint64(first_type) and pa.types.is_signed_integer(second_type)) or (
        pa.types.is_uint64(second_type) and pa.types.is_signed_integer(first_type)
    ):
        pytest.skip("Unsupported ArcticDB type combination")
    lib = lmdb_version_store_dynamic_schema_v1
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "test_arrow_dynamic_schema_changing_types"
    write_table = pa.table({"col": pa.array([0], first_type)})
    append_table = pa.table({"col": pa.array([1], second_type)})
    # TODO: Remove to_pandas() when we support writing Arrow structures directly
    lib.write(sym, write_table.to_pandas())
    lib.append(sym, append_table.to_pandas())
    expected = pa.concat_tables([write_table, append_table], promote_options="permissive")
    received = lib.read(sym).data
    assert expected.equals(received)


@pytest.mark.parametrize("rows_per_column", [1, 7, 8, 9, 100_000])
@pytest.mark.parametrize("segment_row_size", [1, 2, 100_000])
def test_arrow_dynamic_schema_missing_columns_numeric(version_store_factory, rows_per_column, segment_row_size):
    if rows_per_column == 100_000 and segment_row_size != 100_000:
        pytest.skip("Slow to write and doesn't tell us anything the other variants do not")
    lib = version_store_factory(segment_row_size=segment_row_size, dynamic_schema=True)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "test_arrow_dynamic_schema_missing_columns_numeric"
    write_table = pa.table({"col1": pa.array([1] * rows_per_column, pa.int64())})
    append_table = pa.table({"col2": pa.array([2] * rows_per_column, pa.int32())})
    # TODO: Remove to_pandas() when we support writing Arrow structures directly
    lib.write(sym, write_table.to_pandas())
    lib.append(sym, append_table.to_pandas())
    expected = pa.concat_tables([write_table, append_table], promote_options="permissive")
    received = lib.read(sym).data
    assert expected.equals(received)


@pytest.mark.parametrize("rows_per_column", [1, 7, 8, 9, 100_000])
def test_arrow_dynamic_schema_missing_columns_strings(lmdb_version_store_dynamic_schema_v1, rows_per_column):
    lib = lmdb_version_store_dynamic_schema_v1
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "test_arrow_dynamic_schema_missing_columns_strings"
    write_table = pa.table({"col1": pa.array(["hello"] * rows_per_column, pa.string())})
    append_table = pa.table({"col2": pa.array(["goodbye"] * rows_per_column, pa.string())})
    # TODO: Remove to_pandas() when we support writing Arrow structures directly
    lib.write(sym, write_table.to_pandas())
    lib.append(sym, append_table.to_pandas())
    expected = pa.concat_tables([write_table, append_table], promote_options="permissive")
    received = lib.read(sym).data
    received = stringify_dictionary_encoded_columns(received, string_type=pa.string())
    assert expected.equals(received)


# Omit uint64 as it cannot be combined with signed int types in calls to append
# Omit int64 as pyarrow refuses to concatenate arrays together if an int is greater than 2^53
@st.composite
def combinable_numeric_dtypes(draw):
    return draw(
        st.one_of(
            st.one_of(
                unsigned_integer_dtypes(endianness=ENDIANNESS, sizes=[8, 16, 32]),
                integer_dtypes(endianness=ENDIANNESS, sizes=[8, 16, 32]),
                floating_dtypes(endianness=ENDIANNESS, sizes=[32, 64]),
            )
        )
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df_0=dataframe_strategy(
        [
            column_strategy("numeric_1", combinable_numeric_dtypes()),
            column_strategy("numeric_2", combinable_numeric_dtypes()),
            column_strategy("string_1", supported_string_dtypes()),
            column_strategy("string_2", supported_string_dtypes()),
        ]
    ),
    df_1=dataframe_strategy(
        [
            column_strategy("numeric_1", combinable_numeric_dtypes()),
            column_strategy("numeric_2", combinable_numeric_dtypes()),
            column_strategy("string_1", supported_string_dtypes()),
            column_strategy("string_2", supported_string_dtypes()),
        ]
    ),
    df_2=dataframe_strategy(
        [
            column_strategy("numeric_1", combinable_numeric_dtypes()),
            column_strategy("numeric_2", combinable_numeric_dtypes()),
            column_strategy("string_1", supported_string_dtypes()),
            column_strategy("string_2", supported_string_dtypes()),
        ]
    ),
    columns_0=st.lists(
        st.sampled_from(["numeric_1", "numeric_2", "string_1", "string_2"]), min_size=1, max_size=4, unique=True
    ),
    columns_1=st.lists(
        st.sampled_from(["numeric_1", "numeric_2", "string_1", "string_2"]), min_size=1, max_size=4, unique=True
    ),
    columns_2=st.lists(
        st.sampled_from(["numeric_1", "numeric_2", "string_1", "string_2"]), min_size=1, max_size=4, unique=True
    ),
)
def test_arrow_dynamic_schema_missing_columns_hypothesis(
    lmdb_version_store_dynamic_schema_v1, df_0, df_1, df_2, columns_0, columns_1, columns_2
):
    assume(len(df_0) and len(df_1) and len(df_2))
    lib = lmdb_version_store_dynamic_schema_v1
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "test_arrow_dynamic_schema_missing_columns_hypothesis"
    df_0 = df_0[columns_0]
    df_1 = df_1[columns_1]
    df_2 = df_2[columns_2]
    lib.write(sym, df_0)
    lib.append(sym, df_1)
    lib.append(sym, df_2)
    expected = pa.concat_tables(
        [pa.Table.from_pandas(df_0), pa.Table.from_pandas(df_1), pa.Table.from_pandas(df_2)],
        promote_options="permissive",
    )
    received = lib.read(sym).data
    received = stringify_dictionary_encoded_columns(received, string_type=pa.string())
    assert expected.equals(received)


# TODO: Extend these tests to other types when Arrow write support is complete
@pytest.mark.parametrize("type", [pa.float32(), pa.float64()])
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_arrow_sparse_floats_basic(version_store_factory, type, dynamic_schema):
    lib = version_store_factory(dynamic_schema=dynamic_schema)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "test_arrow_sparse_floats_basic"
    table = pa.table({"col": pa.array([1, None, None, 2, None], type)})
    assert table.column("col").null_count == 3
    df = table.to_pandas()
    assert df["col"].isna().sum() == 3
    lib.write(sym, df, sparsify_floats=True)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize("type", [pa.float32(), pa.float64()])
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_arrow_sparse_floats_row_sliced(version_store_factory, type, dynamic_schema):
    lib = version_store_factory(segment_row_size=2, dynamic_schema=dynamic_schema)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "test_arrow_sparse_floats_row_sliced"
    table_0 = pa.table({"col": pa.array([1, None], type)})
    table_1 = pa.table({"col": pa.array([2, 3], type)})
    table_2 = pa.table({"col": pa.array([None, 4], type)})
    df_0 = table_0.to_pandas()
    df_1 = table_1.to_pandas()
    df_2 = table_2.to_pandas()
    df = pd.concat([df_0, df_1, df_2])
    df.index = pd.RangeIndex(0, 6)
    lib.write(sym, df, sparsify_floats=True)
    expected = pa.concat_tables([table_0, table_1, table_2])
    received = lib.read(sym).data
    assert expected.equals(received)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("date_range_start", list(pd.date_range("2025-01-01", periods=14)))
@pytest.mark.parametrize("date_range_width", list(range(0, 15)))
def test_arrow_sparse_floats_date_range(version_store_factory, dynamic_schema, date_range_start, date_range_width):
    lib = version_store_factory(segment_row_size=5, dynamic_schema=dynamic_schema)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "test_arrow_sparse_floats_date_range"
    table_0 = pa.table({"col": pa.array([1, None, 2, None, 3], pa.float64())})
    table_1 = pa.table({"col": pa.array([4, None, None, None, None], pa.float64())})
    table_2 = pa.table({"col": pa.array([None, 5, 6, 7, 8], pa.float64())})
    df_0 = table_0.to_pandas()
    df_1 = table_1.to_pandas()
    df_2 = table_2.to_pandas()
    df = pd.concat([df_0, df_1, df_2])
    df.index = pd.date_range("2025-01-01", periods=15)
    lib.write(sym, df, sparsify_floats=True)
    date_range = (date_range_start, date_range_start + pd.Timedelta(days=date_range_width))
    expected = pa.concat_tables([table_0, table_1, table_2]).slice(
        offset=(date_range_start - pd.Timestamp("2025-01-01")).days, length=date_range_width + 1
    )
    received = lib.read(sym, date_range=date_range).data
    assert expected["col"].equals(received["col"])


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("row_range_start", list(range(14)))
@pytest.mark.parametrize("row_range_width", list(range(0, 15)))
def test_arrow_sparse_floats_row_range(version_store_factory, dynamic_schema, row_range_start, row_range_width):
    lib = version_store_factory(segment_row_size=5, dynamic_schema=dynamic_schema)
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "test_arrow_sparse_floats_row_range"
    table_0 = pa.table({"col": pa.array([1, None, 2, None, 3], pa.float64())})
    table_1 = pa.table({"col": pa.array([4, None, None, None, None], pa.float64())})
    table_2 = pa.table({"col": pa.array([None, 5, 6, 7, 8], pa.float64())})
    df_0 = table_0.to_pandas()
    df_1 = table_1.to_pandas()
    df_2 = table_2.to_pandas()
    df = pd.concat([df_0, df_1, df_2])
    df.index = pd.RangeIndex(0, 15)
    lib.write(sym, df, sparsify_floats=True)
    row_range = (row_range_start, row_range_start + row_range_width)
    expected = pa.concat_tables([table_0, table_1, table_2]).slice(
        offset=row_range[0], length=row_range[1] - row_range[0]
    )
    received = lib.read(sym, row_range=row_range).data
    assert expected.equals(received)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        columns(["col"], elements=st.floats(min_value=0, max_value=1000, allow_nan=False), fill=st.just(np.nan)),
    ),
    rows_per_slice=st.integers(2, 10),
    use_row_range=st.booleans(),
)
def test_arrow_sparse_floats_hypothesis(lmdb_version_store_arrow, df, rows_per_slice, use_row_range):
    row_count = len(df)
    assume(row_count > 0)
    # Cannot use version_store_factory as it will complain that the library name is the same
    lib = lmdb_version_store_arrow
    lib._cfg.write_options.segment_row_size = rows_per_slice
    sym = "test_arrow_sparse_floats_hypothesis"
    # Each row slice must have at least one non-NaN value for sparsify_floats to work, so add one if there aren't any
    row_slices = []
    num_row_slices = (row_count + (rows_per_slice - 1)) // rows_per_slice
    for i in range(num_row_slices):
        row_slice = df[i * rows_per_slice : (i + 1) * rows_per_slice]
        if row_slice["col"].notna().sum() == 0:
            row_slice["col"][i * rows_per_slice] = 100
        row_slices.append(row_slice)
    adjusted_df = pd.concat(row_slices)
    lib.write(sym, adjusted_df, sparsify_floats=True)
    if use_row_range:
        row_range = (row_count // 3, (2 * row_count) // 3)
        expected = pa.concat_tables([pa.Table.from_pandas(row_slice) for row_slice in row_slices]).slice(
            offset=row_range[0], length=row_range[1] - row_range[0]
        )
        received = lib.read(sym, row_range=row_range).data
    else:
        expected = pa.concat_tables([pa.Table.from_pandas(row_slice) for row_slice in row_slices])
        received = lib.read(sym).data
    assert expected.equals(received)


@pytest.mark.parametrize("type_to_drop", [pa.int64(), pa.float64(), pa.large_string()])
def test_arrow_dynamic_schema_filtered_column(lmdb_version_store_dynamic_schema_v1, type_to_drop):
    lib = lmdb_version_store_dynamic_schema_v1
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "sym"
    column_to_drop = (
        pa.array(["a", "b"], type_to_drop) if type_to_drop == pa.large_string() else pa.array([1, 2], type_to_drop)
    )
    table_1 = pa.table({"col": pa.array([0, 1])})
    table_2 = pa.table({"col": pa.array([5, 6]), "col_to_drop": column_to_drop})
    table_3 = pa.table({"col": pa.array([2, 3])})
    # TODO: Remove to_pandas() when we support writing Arrow structures directly
    lib.write(sym, table_1.to_pandas())
    lib.append(sym, table_2.to_pandas())
    lib.append(sym, table_3.to_pandas())
    expected = pa.concat_tables([table_1, table_2, table_3], promote_options="permissive")
    expected = expected.filter(pa.compute.field("col") < 5)
    q = QueryBuilder()
    q = q[q["col"] < 5]
    received = stringify_dictionary_encoded_columns(lib.read(sym, query_builder=q).data)
    assert expected.equals(received)


def test_project_dynamic_schema(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "sym"
    table_1 = pa.table({"a": pa.array([1, 2])})
    table_2 = pa.table({"a": pa.array([3, 4]), "b": pa.array([1, 2])})
    table_3 = pa.table({"b": pa.array([3, 4])})
    lib.write(sym, table_1.to_pandas())
    lib.append(sym, table_2.to_pandas())
    lib.append(sym, table_3.to_pandas())
    q = QueryBuilder()
    q = q.apply("c", q["a"] * q["b"] + 10)
    received = lib.read(sym, query_builder=q).data
    expected = pa.concat_tables([table_1, table_2, table_3], promote_options="permissive")
    expected_new_col = pc.add(pc.multiply(expected.column("a"), expected.column("b")), 10)
    expected = expected.append_column("c", expected_new_col)
    assert expected.equals(received)


def test_project_dynamic_schema_complex(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "sym"
    df = pd.DataFrame(
        {
            "int_col_1": np.arange(0, 10, dtype=np.int16),
            "int_col_2": np.arange(10, 20, dtype=np.int32),
            "float_col": np.arange(20, 30, dtype=np.float64),
        }
    )
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(sym, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.apply("new_float_1", q["int_col_1"] / q["float_col"] + 1)
    q = q.apply("new_float_2", q["int_col_2"] * q["new_float_1"])

    table = lib.read(sym, query_builder=q).data
    expected = lib.read(sym, query_builder=q, output_format=OutputFormat.PANDAS).data
    assert_frame_equal_with_arrow(table, expected)


def test_aggregation_empty_slices(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "sym"
    df_1 = pd.DataFrame(
        {
            "group_col": [chr(ord("a") + i) for i in range(5)],
            "mean_col": np.arange(0, 5, dtype=np.float64),
            "sum_col": np.arange(0, 5, dtype=np.float64),
            "min_col": np.arange(0, 5, dtype=np.float64),
            "max_col": np.arange(0, 5, dtype=np.float64),
            "count_col": np.arange(0, 5, dtype=np.float64),
        }
    )
    df_2 = pd.DataFrame(
        {
            "group_col": [chr(ord("a") + i + 10) for i in range(5)],
        }
    )
    lib.write(sym, df_1, dynamic_strings=True)
    lib.append(sym, df_2, dynamic_strings=True)

    q = QueryBuilder()
    q.groupby("group_col").agg(
        {
            "mean_col": "mean",
            "sum_col": "sum",
            "min_col": "min",
            "max_col": "max",
            "count_col": "count",
        }
    )

    table = lib.read(sym, query_builder=q).data
    # sum_col is correctly filled with 0s instead of nulls
    assert pc.count(table.column("sum_col"), mode="only_null").as_py() == 0
    assert pc.count(table.column("mean_col"), mode="only_null").as_py() == 5
    assert pc.count(table.column("min_col"), mode="only_null").as_py() == 5
    assert pc.count(table.column("max_col"), mode="only_null").as_py() == 5
    assert pc.count(table.column("count_col"), mode="only_null").as_py() == 5
    expected = lib.read(sym, query_builder=q, output_format=OutputFormat.PANDAS).data
    assert_frame_equal_with_arrow(table, expected)


def test_resample_empty_slices(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    lib.set_output_format(OutputFormat.EXPERIMENTAL_ARROW)
    sym = "sym"
    def gen_df(start, num_rows, with_columns=True):
        data = {}
        if with_columns:
            data = {
                "mean_col": np.arange(start, start+num_rows, dtype=np.float64),
                "sum_col": np.arange(start, start+num_rows, dtype=np.float64),
                "min_col": np.arange(start, start+num_rows, dtype=np.float64),
                "max_col": np.arange(start, start+num_rows, dtype=np.float64),
                "count_col": np.arange(start, start+num_rows, dtype=np.float64),
            }
        index = pd.date_range(pd.Timestamp(2025, 1, start), periods=num_rows)
        return pd.DataFrame(data, index=index)

    slices = [
        gen_df(1, 3),
        gen_df(4, 2, False), # We expect an entirely missing slice 4th-5th
        gen_df(6, 3),
        gen_df(9, 5, False), # We expect two missing slices 10th-11th and 12th-13th
        gen_df(14, 2),
        gen_df(16, 2, False), # We expect one missing slice 16th-17th
        # TODO: If we don't finish with an append with columns our normalization metadata will be broken
        gen_df(18, 1)
    ]
    for df_slice in slices:
        lib.append(sym, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q.resample("2d").agg({
        "mean_col": "mean",
        "sum_col": "sum",
        "min_col": "min",
        "max_col": "max",
        "count_col": "count",
    })

    table = lib.read(sym, query_builder=q).data
    # sum_col is correctly filled with 0s instead of nulls
    assert pc.count(table.column("sum_col"), mode="only_null").as_py() == 0
    # We expect 4 entirely empty buckets
    assert pc.count(table.column("mean_col"), mode="only_null").as_py() == 4
    assert pc.count(table.column("min_col"), mode="only_null").as_py() == 4
    assert pc.count(table.column("max_col"), mode="only_null").as_py() == 4
    assert pc.count(table.column("count_col"), mode="only_null").as_py() == 4
    expected = lib.read(sym, query_builder=q, output_format=OutputFormat.PANDAS).data
    assert_frame_equal_with_arrow(table, expected)
