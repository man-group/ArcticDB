from arcticdb_ext.version_store import OutputFormat
import pandas as pd
import numpy as np
import pytest

from pandas.testing import assert_frame_equal
from arcticdb.version_store.processing import QueryBuilder
import pyarrow as pa

def test_basic(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.to_pandas()
    assert_frame_equal(result, df)


def test_basic_small_slices(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.to_pandas()
    assert_frame_equal(result, df)


def test_double_columns(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.to_pandas()
    assert_frame_equal(result, df)


def test_column_filtering(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    vit = lib.read("arrow", columns=['y'], output_format=OutputFormat.ARROW)
    df = df.drop('x', axis=1)
    result = vit.to_pandas()
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


def test_strings(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": ["mene", "mene", "tekel", "upharsin"]})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = convert_dict_strings_to_array(vit).to_pandas()

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
    data_closed_table = lib.read(sym, date_range=date_range, output_format=OutputFormat.ARROW)
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
    result = vit.to_pandas()
    assert_frame_equal(result, expected)


def test_dynamic_schema(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    df1 = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})

    lib.write("arrow", df1)
    df2 = pd.DataFrame({"y": np.arange(20.0, 30.0), "z": np.arange(10.0, 20.0)})
    lib.append("arrow", df2)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.to_pandas()
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
    result = vit.to_pandas()
    expected = pd.concat([df1, df2])
    expected.reset_index(drop=True, inplace=True)
    assert_frame_equal(result.astype(float).fillna(0), expected.fillna(0))



