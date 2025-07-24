from arcticdb_ext.version_store import OutputFormat
import pandas as pd
import numpy as np
import pyarrow as pa

from arcticdb.util.test import assert_frame_equal_with_arrow

def test_index_with_name(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=pd.date_range(pd.Timestamp(2025, 1, 1), periods=10)
    )
    df.index.name = "some_random_index"
    lib.write("arrow", df)
    table = lib.read("arrow", _output_format=OutputFormat.ARROW).data
    assert table.column_names[0] == "some_random_index"
    assert_frame_equal_with_arrow(table, df)

def test_index_with_timezone(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=pd.date_range(pd.Timestamp(2025, 1, 1, tz="America/New_York"), periods=10)
    )
    lib.write("arrow", df)
    table = lib.read("arrow", _output_format=OutputFormat.ARROW).data
    assert table.schema.field(0).type == pa.timestamp("ns", "America/New_York")
    assert_frame_equal_with_arrow(table, df)

def test_multi_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index = [
            [chr(ord('a') + i//5) for i in range(10)],
            [i%5 for i in range(10)],
        ]
    )
    df.index.names = ["index1", "index2"]
    lib.write("arrow", df)
    table = lib.read("arrow", _output_format=OutputFormat.ARROW).data
    assert table.schema.field(0).name == "index1"
    assert table.schema.field(0).type == pa.dictionary(pa.int32(), pa.large_string())
    assert table.schema.field(1).name == "index2"
    assert table.schema.field(1).type == pa.int64()
    assert_frame_equal_with_arrow(table, df)


def test_multi_index_with_tz(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index = [
            [chr(ord('a') + i//5) for i in range(10)],
            [pd.Timestamp(2025, 1, 1+i%5, tz="America/Los_Angeles") for i in range(10)],
        ]
    )
    df.index.names = ["index1", "index2"]
    lib.write("arrow", df)
    table = lib.read("arrow", _output_format=OutputFormat.ARROW).data
    assert table.schema.field(0).name == "index1"
    assert table.schema.field(0).type == pa.dictionary(pa.int32(), pa.large_string())
    assert table.schema.field(1).name == "index2"
    assert table.schema.field(1).type == pa.timestamp("ns", "America/Los_Angeles")
    assert_frame_equal_with_arrow(table, df)


def test_duplicate_column_name(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(np.arange(30, dtype=np.float64).reshape(10, 3), columns=["x", "y", "x"])
    lib.write("arrow", df)
    table = lib.read("arrow", _output_format=OutputFormat.ARROW).data
    assert table.schema.field(0).name == "__col_x__0"
    assert table.schema.field(0).type == pa.float64()
    assert table.schema.field(1).name == "y"
    assert table.schema.field(1).type == pa.float64()
    assert table.schema.field(2).name == "__col_x__2"
    assert table.schema.field(2).type == pa.float64()
    assert_frame_equal_with_arrow(table, df)

def test_int_column_name(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(np.arange(30, dtype=np.float64).reshape(10, 3), columns=[1, 2, "x"])
    lib.write("arrow", df)
    table = lib.read("arrow", _output_format=OutputFormat.ARROW).data
    expected_df = lib.read("arrow", _output_format=OutputFormat.PANDAS).data
    assert table.schema.field(0).name == "1"
    assert table.schema.field(0).type == pa.float64()
    assert table.schema.field(1).name == "2"
    assert table.schema.field(1).type == pa.float64()
    assert table.schema.field(2).name == "x"
    assert table.schema.field(2).type == pa.float64()
    assert_frame_equal_with_arrow(table, expected_df)
