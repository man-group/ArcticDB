from arcticdb_ext.version_store import OutputFormat
import pandas as pd
import numpy as np
import pytest

from pandas.testing import assert_frame_equal
from arcticdb.version_store.processing import QueryBuilder
import pyarrow as pa
from arcticdb.util.test import get_sample_dataframe
from arcticdb_ext.storage import KeyType
from tests.util.mark import WINDOWS


def test_basic(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


# TODO: Do this fix during normalization in frontend PR
def fix_timeseries_index(df, set_index=False):
    df["index"] = df["index"].apply(lambda x : pd.Timestamp(x))
    df["index"] = df["index"].astype("datetime64[ns]")
    if set_index:
        df = df.set_index("index")
    return df


def test_basic_with_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lib.write("arrow", df)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = fix_timeseries_index(vit.data.to_pandas())
    assert_frame_equal(result, df.reset_index())


def test_basic_small_slices(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


def test_basic_small_slices_with_index(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    df = pd.DataFrame({"x": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lib.write("arrow", df)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = fix_timeseries_index(vit.data.to_pandas())
    assert_frame_equal(result, df.reset_index())


def test_double_columns(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


def test_bool_columns(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": [i%3 == 0 for i in range(10)], "y": [i%2 == 0 for i in range(10)]})
    lib.write("arrow", df)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


def test_column_filtering(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    vit = lib.read("arrow", columns=['y'], _output_format=OutputFormat.ARROW)
    df = df.drop('x', axis=1)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


def convert_pandas_categorical_to_str(df):
    categorical_cols = df.select_dtypes(include=['category']).columns
    df[categorical_cols] = df[categorical_cols].astype(str)
    return df


@pytest.mark.parametrize("dynamic_strings", [
    True,
    pytest.param(False, marks=pytest.mark.xfail(reason="Arrow fixed strings are not normalized correctly"))
])
def test_strings_basic(lmdb_version_store_v1, dynamic_strings):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": ["mene", "mene", "tekel", "upharsin"]})
    lib.write("arrow", df, dynamic_strings=dynamic_strings)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = convert_pandas_categorical_to_str(vit.data.to_pandas())
    assert_frame_equal(result, df)


@pytest.mark.parametrize("dynamic_strings", [
    True,
    pytest.param(False, marks=pytest.mark.xfail(reason="Arrow fixed strings are not normalized correctly"))
])
def test_strings_multiple_segments_and_columns(lmdb_version_store_tiny_segment, dynamic_strings):
    lib = lmdb_version_store_tiny_segment
    df = pd.DataFrame({
        "x": [f"x_{i//2}" for i in range(100)],
        "x_copy": [f"x_{i//2}" for i in range(100)],
        "y": [f"y_{i}" for i in range(100)],
        "z": [f"z_{i//5}" for i in range(100)],
    })
    lib.write("arrow", df, dynamic_strings=dynamic_strings)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = convert_pandas_categorical_to_str(vit.data.to_pandas())
    assert_frame_equal(result, df)


# TODO: Fix unicode strings on windows
@pytest.mark.skipif(WINDOWS, reason="Unicode arrow strings fail on windows")
def test_all_types(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    # sample dataframe contains all dtypes + unicode strings
    df = get_sample_dataframe()
    lib.write("arrow", df)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = convert_pandas_categorical_to_str(vit.data.to_pandas())
    assert_frame_equal(result, df)


@pytest.mark.parametrize("date_range_start", [0, 1, 2, 3, 4, 5, 6])
@pytest.mark.parametrize("date_range_width", [0, 1, 2, 3])
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_date_range_corner_cases(version_store_factory, date_range_start, date_range_width, dynamic_schema):
    lib = version_store_factory(segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema)
    df = pd.DataFrame(data={"col1": np.arange(7), "col2": np.arange(7), "col3": np.arange(7)}, index=pd.date_range(pd.Timestamp(0), freq="ns", periods=7))
    sym = "test_date_range_corner_cases"
    lib.write(sym, df)

    query_start_ts = pd.Timestamp(date_range_start)
    query_end_ts = pd.Timestamp(date_range_start + date_range_width)

    date_range = (query_start_ts, query_end_ts)
    expected_df = lib.read(sym, date_range=date_range, _output_format=OutputFormat.PANDAS).data
    expected_df.index.name = "index"
    data_closed_table = lib.read(sym, date_range=date_range, _output_format=OutputFormat.ARROW).data
    received_df = fix_timeseries_index(data_closed_table.to_pandas(), set_index=True)
    assert_frame_equal(expected_df, received_df)


def test_date_range_between_index_values(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    df = pd.DataFrame(data={"col1": np.arange(2)}, index=[pd.Timestamp(0), pd.Timestamp(10)])
    sym = "test_date_range_between_index_values"
    lib.write(sym, df)

    date_range = (pd.Timestamp(4), pd.Timestamp(5))
    expected_df = lib.read(sym, date_range=date_range, _output_format=OutputFormat.PANDAS).data
    expected_df.index.name = "index"
    data_closed_table = lib.read(sym, date_range=date_range, _output_format=OutputFormat.ARROW).data
    received_df = fix_timeseries_index(data_closed_table.to_pandas(), set_index=True)
    assert_frame_equal(expected_df, received_df)


@pytest.mark.parametrize("date_range_start", [-5, 10])
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_date_range_empty_result(version_store_factory, date_range_start, dynamic_schema):
    lib = version_store_factory(segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema)
    df = pd.DataFrame(data={"col1": np.arange(7), "col2": np.arange(7), "col3": [f"{i}" for i in range(7)]}, index=pd.date_range(pd.Timestamp(0), freq="ns", periods=7))
    sym = "test_date_range_empty_result"
    lib.write(sym, df)

    query_start_ts = pd.Timestamp(date_range_start)
    query_end_ts = pd.Timestamp(date_range_start + 1)

    date_range = (query_start_ts, query_end_ts)
    expected_df = lib.read(sym, date_range=date_range, _output_format=OutputFormat.PANDAS).data
    expected_df.index.name = "index"
    data_closed_table = lib.read(sym, date_range=date_range, _output_format=OutputFormat.ARROW).data
    received_df = fix_timeseries_index(data_closed_table.to_pandas(), set_index=True)
    received_df = convert_pandas_categorical_to_str(received_df)
    assert_frame_equal(expected_df, received_df)


@pytest.mark.parametrize("segment_row_size", [1, 2, 10, 100])
@pytest.mark.parametrize("start_offset,end_offset", [(2, 3), (3, 75), (4, 32), (0, 99), (7, 56)])
def test_date_range(version_store_factory, segment_row_size, start_offset, end_offset):
    lib = version_store_factory(segment_row_size=segment_row_size)
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(100), index=pd.date_range(initial_timestamp, periods=100), columns=['x'])
    sym = "arrow_date_test"
    lib.write(sym, df)

    query_start_ts = initial_timestamp + pd.DateOffset(start_offset)
    query_end_ts = initial_timestamp + pd.DateOffset(end_offset)

    date_range = (query_start_ts, query_end_ts)
    data_closed_table = lib.read(sym, date_range=date_range, _output_format=OutputFormat.ARROW).data
    df = fix_timeseries_index(data_closed_table.to_pandas(), set_index=True)
    assert query_start_ts == df.index[0]
    assert query_end_ts == df.index[-1]
    assert df['x'].iloc[0] == start_offset
    assert df['x'].iloc[-1] == end_offset

@pytest.mark.parametrize("segment_row_size", [1, 2, 10, 100])
@pytest.mark.parametrize("start_date,end_date", [(1, 1), (1, 4), (1, 5), (3, 7), (6, 7), (6, 10), (7, 7)])
def test_date_range_with_duplicates(version_store_factory, segment_row_size, start_date, end_date):
    lib = version_store_factory(segment_row_size=segment_row_size)
    index_with_duplicates = (
            [ pd.Timestamp(2025, 1, 1) ] * 10 +
            [ pd.Timestamp(2025, 1, 2) ] * 13 +
            [ pd.Timestamp(2025, 1, 5) ] * 5 +
            [ pd.Timestamp(2025, 1, 6) ] * 1 +
            [ pd.Timestamp(2025, 1, 7) ] * 25
    )
    size = len(index_with_duplicates)
    df = pd.DataFrame(data=np.arange(size, dtype=np.int64), index=index_with_duplicates, columns=['x'])
    sym = "arrow_date_test"
    lib.write(sym, df)

    query_start_ts = pd.Timestamp(2025, 1, start_date)
    query_end_ts = pd.Timestamp(2025, 1, end_date)

    date_range = (query_start_ts, query_end_ts)
    arrow_table = lib.read(sym, date_range=date_range, _output_format=OutputFormat.ARROW).data
    read_df = fix_timeseries_index(arrow_table.to_pandas())
    expected_df = df[(df.index >= query_start_ts) & (df.index <= query_end_ts)].reset_index()
    assert_frame_equal(read_df, expected_df)


@pytest.mark.parametrize("row_range_start", [0, 1, 2, 3, 4, 5, 6])
@pytest.mark.parametrize("row_range_width", [0, 1, 2, 3])
@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("index", [None, pd.date_range(pd.Timestamp(0), freq="ns", periods=7)])
def test_row_range_corner_cases(version_store_factory, row_range_start, row_range_width, dynamic_schema, index):
    lib = version_store_factory(segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema)
    df = pd.DataFrame(data={"col1": np.arange(7), "col2": np.arange(7), "col3": np.arange(7)}, index=index)
    sym = "test_row_range_corner_cases"
    lib.write(sym, df)

    row_range = (row_range_start, row_range_start + row_range_width + 1)
    expected_df = lib.read(sym, row_range=row_range, _output_format=OutputFormat.PANDAS).data
    data_closed_table = lib.read(sym, row_range=row_range, _output_format=OutputFormat.ARROW).data
    received_df = data_closed_table.to_pandas()
    if index is not None:
        expected_df.index.name = "index"
        received_df = fix_timeseries_index(received_df, set_index=True)
    assert_frame_equal(expected_df, received_df)


@pytest.mark.parametrize("row_range_start", [-10, 10])
@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("index", [None, pd.date_range(pd.Timestamp(0), freq="ns", periods=7)])
def test_row_range_empty_result(version_store_factory, row_range_start, dynamic_schema, index):
    lib = version_store_factory(segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema)
    df = pd.DataFrame(data={"col1": np.arange(7), "col2": np.arange(7), "col3": [f"{i}" for i in range(7)]}, index=index)
    sym = "test_row_range_empty_result"
    lib.write(sym, df)

    row_range = (row_range_start, row_range_start + 1)
    expected_df = lib.read(sym, row_range=row_range, _output_format=OutputFormat.PANDAS).data
    data_closed_table = lib.read(sym, row_range=row_range, _output_format=OutputFormat.ARROW).data
    received_df = data_closed_table.to_pandas()
    if index is not None:
        expected_df.index.name = "index"
        received_df = fix_timeseries_index(received_df, set_index=True)
    received_df = convert_pandas_categorical_to_str(received_df)
    assert_frame_equal(expected_df, received_df)


@pytest.mark.parametrize("segment_row_size", [1, 2, 10, 100])
@pytest.mark.parametrize("start_offset,end_offset", [(2, 4), (3, 76), (4, 33), (0, 100), (7, 57)])
def test_row_range(version_store_factory, segment_row_size, start_offset, end_offset):
    lib = version_store_factory(segment_row_size=segment_row_size)
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(100), index=pd.date_range(initial_timestamp, periods=100), columns=['x'])
    sym = "arrow_date_test"
    lib.write(sym, df)

    row_range = (start_offset, end_offset)
    data_closed_table = lib.read(sym, row_range=row_range, _output_format=OutputFormat.ARROW).data
    df = fix_timeseries_index(data_closed_table.to_pandas(), set_index=True)

    start_ts = initial_timestamp + pd.DateOffset(start_offset)
    end_ts = initial_timestamp + pd.DateOffset(end_offset-1)
    assert start_ts == df.index[0]
    assert end_ts == df.index[-1]
    assert df['x'].iloc[0] == start_offset
    assert df['x'].iloc[-1] == end_offset-1


def test_with_querybuilder(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    q = QueryBuilder()
    q = q[q["x"] < 5]
    lib.write("arrow", df)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW, query_builder=q)
    expected = df[df["x"] < 5]
    result = vit.data.to_pandas()
    assert_frame_equal(result, expected)


def test_dynamic_schema(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    df1 = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})

    lib.write("arrow", df1)
    df2 = pd.DataFrame({"y": np.arange(20.0, 30.0), "z": np.arange(10.0, 20.0)})
    lib.append("arrow", df2)
    vit = lib.read("arrow", _output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    expected = pd.concat([df1, df2])
    expected.reset_index(drop=True, inplace=True)
    assert_frame_equal(result.astype(float).fillna(0), expected.fillna(0))


def test_dynamic_schema_column_change(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    df1 = pd.DataFrame({"x": np.arange(10)})

    lib.write("arrow", df1)
    df2 = pd.DataFrame({"x": np.arange(20.0, 30.0, dtype=np.float64)})
    lib.append("arrow", df2)
    arrow_table = lib.read("arrow", _output_format=OutputFormat.ARROW).data
    batches = arrow_table.to_batches()
    assert len(batches) == 2
    for record_batch in batches:
        x_arr = record_batch.columns[0]
        assert x_arr.type == pa.float64()
    result = arrow_table.to_pandas()
    expected = pd.concat([df1, df2])
    expected.reset_index(drop=True, inplace=True)
    assert_frame_equal(result.astype(float).fillna(0), expected.fillna(0))


def test_arrow_layout(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    lib_tool = lib.library_tool()
    num_rows = 100
    df = pd.DataFrame(data={"int": np.arange(num_rows, dtype=np.int64), "str": [f"x_{i//3}" for i in range(num_rows)]},
                      index=pd.date_range(pd.Timestamp(0), periods=num_rows))
    lib.write("sym", df, dynamic_strings=True)
    data_keys = lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")
    assert len(data_keys) == num_rows//2

    arrow_table = lib.read("sym", _output_format=OutputFormat.ARROW).data
    batches = arrow_table.to_batches()
    assert len(batches) == num_rows//2
    for record_batch in batches:
        index_arr, int_arr, str_arr = record_batch.columns
        assert index_arr.type == pa.int64()
        assert int_arr.type == pa.int64()
        assert str_arr.type == pa.dictionary(pa.int32(), pa.large_string())