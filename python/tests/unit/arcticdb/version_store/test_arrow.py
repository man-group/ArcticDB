from arcticdb_ext.version_store import OutputFormat
import pandas as pd
import numpy as np
import pytest

from pandas.testing import assert_frame_equal
from arcticdb.version_store.processing import QueryBuilder
import pyarrow as pa
from arcticdb.util.test import get_sample_dataframe


def test_basic(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


# TODO: Do this fix during normalization
def fix_timeseries_index(df):
    df["index"] = df["index"].apply(lambda x : pd.Timestamp(x))
    return df


def test_basic_with_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = fix_timeseries_index(vit.data.to_pandas())
    assert_frame_equal(result, df.reset_index())


def test_basic_small_slices(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


def test_basic_small_slices_with_index(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    df = pd.DataFrame({"x": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = fix_timeseries_index(vit.data.to_pandas())
    assert_frame_equal(result, df.reset_index())


def test_double_columns(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


def test_bool_columns(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": [i%3 == 0 for i in range(10)], "y": [i%2 == 0 for i in range(10)]})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


def test_column_filtering(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    vit = lib.read("arrow", columns=['y'], output_format=OutputFormat.ARROW)
    df = df.drop('x', axis=1)
    result = vit.data.to_pandas()
    assert_frame_equal(result, df)


def convert_dict_strings_to_array(table: pa.Table) -> pa.Table:
    new_columns = []
    new_fields = []

    for i, col in enumerate(table.columns):
        field = table.field(i)
        if pa.types.is_dictionary(col.type) and pa.types.is_string(col.type.value_type):
            new_columns.append(col.cast(pa.string()))
            new_fields.append(field.with_type(pa.string()))
        else:
            new_columns.append(col)
            new_fields.append(field)

    return pa.Table.from_arrays(
        new_columns,
        schema=pa.schema(new_fields)
    )


@pytest.mark.parametrize("dynamic_strings", [
    True,
    pytest.param(False, marks=pytest.mark.xfail(reason="Arrow fixed strings are not normalized correctly"))
])
def test_strings_basic(lmdb_version_store_v1, dynamic_strings):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": ["mene", "mene", "tekel", "upharsin"]})
    lib.write("arrow", df, dynamic_strings=dynamic_strings)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = convert_dict_strings_to_array(vit.data).to_pandas()
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
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = convert_dict_strings_to_array(vit.data).to_pandas()
    assert_frame_equal(result, df)


def test_all_types(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    # sample dataframe contains all dtypes + unicode strings
    df = get_sample_dataframe()
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = convert_dict_strings_to_array(vit.data).to_pandas()
    assert_frame_equal(result, df)


@pytest.mark.parametrize("start_offset,end_offset", [(2, 3), (3, 75), (4, 32), (0, 99), (7, 56)])
def test_date_range(lmdb_version_store_v1, start_offset, end_offset):
    lib = lmdb_version_store_v1
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(100), index=pd.date_range(initial_timestamp, periods=100), columns=['x'])
    sym = "arrow_date_test"
    lib.write(sym, df)

    query_start_ts = initial_timestamp + pd.DateOffset(start_offset)
    query_end_ts = initial_timestamp + pd.DateOffset(end_offset)

    date_range = (query_start_ts, query_end_ts)
    data_closed_table = lib.read(sym, date_range=date_range, output_format=OutputFormat.ARROW).data
    df = data_closed_table.to_pandas()
    df = df.set_index('index')
    assert query_start_ts == pd.Timestamp(df.index[0])
    assert query_end_ts == pd.Timestamp(df.index[-1])
    assert df['x'].iloc[0] == start_offset
    assert df['x'].iloc[-1] == end_offset


def test_with_querybuilder(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    q = QueryBuilder()
    q = q[q["x"] < 5]
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW, query_builder=q)
    expected = df[df["x"] < 5]
    result = vit.data.to_pandas()
    assert_frame_equal(result, expected)


def test_dynamic_schema(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    df1 = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})

    lib.write("arrow", df1)
    df2 = pd.DataFrame({"y": np.arange(20.0, 30.0), "z": np.arange(10.0, 20.0)})
    lib.append("arrow", df2)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    expected = pd.concat([df1, df2])
    expected.reset_index(drop=True, inplace=True)
    assert_frame_equal(result.astype(float).fillna(0), expected.fillna(0))


def test_dynamic_schema_column_change(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    df1 = pd.DataFrame({"x": np.arange(10)})

    lib.write("arrow", df1)
    df2 = pd.DataFrame({"x": np.arange(20.0, 30.0)})
    lib.append("arrow", df2)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.data.to_pandas()
    expected = pd.concat([df1, df2])
    expected.reset_index(drop=True, inplace=True)
    assert_frame_equal(result.astype(float).fillna(0), expected.fillna(0))