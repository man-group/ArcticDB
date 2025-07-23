from arcticdb_ext.version_store import OutputFormat
import pandas as pd
import numpy as np
import pyarrow as pa

from arcticdb.util.test import assert_frame_equal

def convert_arrow_to_pandas_and_remove_categoricals(table):
    new_columns = []
    new_fields = []
    metadata = table.schema.metadata

    for i, col in enumerate(table.columns):
        field = table.field(i)
        if isinstance(field.type, pa.DictionaryType):
            col = pa.compute.cast(col, field.type.value_type)
            field = field.with_type(field.type.value_type)
        new_columns.append(col)
        new_fields.append(field)

    new_table = pa.Table.from_arrays(
        new_columns,
        schema=pa.schema(new_fields).with_metadata(metadata)
    )
    return new_table.to_pandas()

def test_index_with_name(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=pd.date_range(pd.Timestamp(2025, 1, 1), periods=10)
    )
    df.index.name = "some_random_index"
    lib.write("arrow", df)
    table = lib.read("arrow", output_format=OutputFormat.ARROW).data
    assert table.column_names[0] == "some_random_index"
    assert_frame_equal(table.to_pandas(), df)

def test_index_with_timezone(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=pd.date_range(pd.Timestamp(2025, 1, 1, tz="America/New_York"), periods=10)
    )
    lib.write("arrow", df)
    table = lib.read("arrow", output_format=OutputFormat.ARROW).data
    assert table.schema.field(0).type == pa.timestamp("ns", "America/New_York")
    assert_frame_equal(table.to_pandas(), df)

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
    table = lib.read("arrow", output_format=OutputFormat.ARROW).data
    assert table.schema.field(0).name == "index1"
    assert table.schema.field(0).type == pa.dictionary(pa.int32(), pa.large_string())
    assert table.schema.field(1).name == "index2"
    assert table.schema.field(1).type == pa.int64()
    result_df = convert_arrow_to_pandas_and_remove_categoricals(table)
    assert_frame_equal(result_df, df)


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
    table = lib.read("arrow", output_format=OutputFormat.ARROW).data
    assert table.schema.field(0).name == "index1"
    assert table.schema.field(0).type == pa.dictionary(pa.int32(), pa.large_string())
    assert table.schema.field(1).name == "index2"
    assert table.schema.field(1).type == pa.timestamp("ns", "America/Los_Angeles")
    result_df = convert_arrow_to_pandas_and_remove_categoricals(table)
    assert_frame_equal(result_df, df)
