from arcticdb.options import OutputFormat
import pandas as pd
import numpy as np
import pyarrow as pa
import pytest

from arcticdb.util.test import assert_frame_equal_with_arrow
from arcticdb.exceptions import ArcticDbNotYetImplemented
from pandas import RangeIndex


def test_index_with_name(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": np.arange(10)}, index=pd.date_range(pd.Timestamp(2025, 1, 1), periods=10))
    df.index.name = "some_random_index"
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.column_names[0] == "some_random_index"
    assert_frame_equal_with_arrow(table, df)


def test_index_with_timezone(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=pd.date_range(pd.Timestamp(year=2025, month=1, day=1, tz="America/New_York"), periods=10),
    )
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).type == pa.timestamp("ns", "America/New_York")
    assert_frame_equal_with_arrow(table, df)


def test_basic_range_index(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    table = lib.read("arrow").data
    # With a range index we don't include it as an arrow column
    assert table.column_names == ["x"]
    # But when converted to pandas the RangeIndex is reconstructed
    assert_frame_equal_with_arrow(table, df)


def test_custom_range_index(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame({"x": np.arange(10)}, index=RangeIndex(start=13, step=3, stop=13 + 10 * 3))
    lib.write("arrow", df)
    table = lib.read("arrow").data
    # With a range index we don't include it as an arrow column
    assert table.column_names == ["x"]
    # But when converted to pandas the RangeIndex is reconstructed
    assert_frame_equal_with_arrow(table, df)


def test_multi_index(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=[
            [chr(ord("a") + i // 5) for i in range(10)],
            [i % 5 for i in range(10)],
        ],
    )
    df.index.names = ["index1", "index2"]
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "index1"
    assert table.schema.field(0).type == pa.large_string()
    assert table.schema.field(1).name == "index2"
    assert table.schema.field(1).type == pa.int64()
    assert_frame_equal_with_arrow(table, df)


def test_multi_index_names(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=[
            [chr(ord("a") + i // 5) for i in range(10)],
            [i % 2 for i in range(10)],
            [i % 3 for i in range(10)],
            [i % 4 for i in range(10)],
        ],
    )
    print(df.index.names)
    df.index.names = [None, "index", None, "another_index"]
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "index"
    assert table.schema.field(1).name == "__idx__index"
    assert table.schema.field(2).name == "__fkidx__2"
    assert table.schema.field(3).name == "another_index"
    assert_frame_equal_with_arrow(table, df)


def test_multi_index_names_with_first_set(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=[
            [chr(ord("a") + i // 5) for i in range(10)],
            [i % 2 for i in range(10)],
            [i % 3 for i in range(10)],
            [i % 4 for i in range(10)],
        ],
    )
    print(df.index.names)
    df.index.names = ["some_index", "some_index", None, "another_index"]
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "some_index"
    assert table.schema.field(1).name == "__idx__some_index"
    assert table.schema.field(2).name == "__fkidx__2"
    assert table.schema.field(3).name == "another_index"
    assert_frame_equal_with_arrow(table, df)


def test_multi_index_names_pandas(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=[
            [chr(ord("a") + i // 5) for i in range(10)],
            [i % 5 for i in range(10)],
        ],
    )
    print(df.index.names)
    df.index.names = [None, "index"]
    lib.write("sym", df)
    result_df = lib.read("sym").data
    assert_frame_equal_with_arrow(result_df, df)


def test_multi_index_with_tz(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=[
            [chr(ord("a") + i // 5) for i in range(10)],
            [pd.Timestamp(year=2025, month=1, day=1 + i % 5, tz="America/Los_Angeles") for i in range(10)],
        ],
    )
    df.index.names = ["index1", "index2"]
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "index1"
    assert table.schema.field(0).type == pa.large_string()
    assert table.schema.field(1).name == "index2"
    assert table.schema.field(1).type == pa.timestamp("ns", "America/Los_Angeles")
    assert_frame_equal_with_arrow(table, df)


def test_multi_index_no_name_multiple_tz(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=[
            [pd.Timestamp(year=2025, month=1, day=1 + i // 5, tz="Asia/Hong_Kong") for i in range(10)],
            [pd.Timestamp(year=2025, month=1, day=1 + i % 5, tz="America/Los_Angeles") for i in range(10)],
        ],
    )
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "index"
    assert table.schema.field(0).type == pa.timestamp("ns", "Asia/Hong_Kong")
    assert table.schema.field(1).name == "__fkidx__1"
    assert table.schema.field(1).type == pa.timestamp("ns", "America/Los_Angeles")
    assert_frame_equal_with_arrow(table, df)


def test_duplicate_column_name(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(np.arange(30, dtype=np.float64).reshape(10, 3), columns=["x", "y", "x"])
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "__col_x__0"
    assert table.schema.field(0).type == pa.float64()
    assert table.schema.field(1).name == "y"
    assert table.schema.field(1).type == pa.float64()
    assert table.schema.field(2).name == "__col_x__2"
    assert table.schema.field(2).type == pa.float64()
    assert_frame_equal_with_arrow(table, df)


def test_int_column_name(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(np.arange(30, dtype=np.float64).reshape(10, 3), columns=[1, 2, "x"])
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "1"
    assert table.schema.field(0).type == pa.float64()
    assert table.schema.field(1).name == "2"
    assert table.schema.field(1).type == pa.float64()
    assert table.schema.field(2).name == "x"
    assert table.schema.field(2).type == pa.float64()
    expected_df = lib.read("arrow", output_format=OutputFormat.PANDAS).data
    assert_frame_equal_with_arrow(table, expected_df)


def test_index_duplicate_name(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"same_as_index": np.arange(10, dtype=np.int64)}, index=pd.date_range(pd.Timestamp(2025, 1, 1), periods=10)
    )
    df.index.name = "same_as_index"
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "same_as_index"
    assert table.schema.field(0).type == pa.timestamp("ns")
    assert table.schema.field(1).name == "__col_same_as_index__0"
    assert table.schema.field(1).type == pa.int64()
    assert_frame_equal_with_arrow(table, df)


def test_index_no_name_duplicate(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    df = pd.DataFrame(
        {"index": np.arange(10, dtype=np.int64)}, index=pd.date_range(pd.Timestamp(2025, 1, 1), periods=10)
    )
    lib.write("arrow", df)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "index"
    assert table.schema.field(0).type == pa.timestamp("ns")
    assert table.schema.field(1).name == "__col_index__0"
    assert table.schema.field(1).type == pa.int64()
    assert_frame_equal_with_arrow(table, df)


def test_series_basic(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    series = pd.Series(np.arange(10, dtype=np.int64), name="x", index=pd.RangeIndex(start=3, step=5, stop=3 + 10 * 5))
    lib.write("arrow", series)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "x"
    assert table.schema.field(0).type == pa.int64()
    assert_frame_equal_with_arrow(table, pd.DataFrame(series))


def test_series_with_index(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    series = pd.Series(
        np.arange(10, dtype=np.int64),
        name="x",
        index=pd.date_range(pd.Timestamp(year=2025, month=1, day=1, tz="Europe/London"), periods=10),
    )
    lib.write("arrow", series)
    table = lib.read("arrow").data
    assert table.schema.field(0).name == "index"
    assert table.schema.field(0).type == pa.timestamp("ns", "Europe/London")
    assert table.schema.field(1).name == "x"
    assert table.schema.field(1).type == pa.int64()
    assert_frame_equal_with_arrow(table, pd.DataFrame(series))


def test_read_pickled(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "sym"
    obj = {"a": ["b", "c"], "x": 122.3}
    lib.write(sym, obj)
    result = lib.read(sym).data
    assert obj == result


def test_custom_normalizer(custom_thing_with_registered_normalizer, lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "sym"
    obj = custom_thing_with_registered_normalizer
    lib.write(sym, obj)
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.read(sym).data
