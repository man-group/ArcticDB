from arcticdb_ext.version_store import OutputFormat
import pandas as pd
import numpy as np

from arcticdb.util.test import assert_frame_equal

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
    result_df = table.to_pandas()
    # TODO: Remove once better testing tools
    result_df = result_df.set_index("some_random_index")
    assert_frame_equal(result_df, df)

def test_index_with_timezone(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=pd.date_range(pd.Timestamp(2025, 1, 1, tz="America/New_York"), periods=10)
    )
    lib.write("arrow", df)
    table = lib.read("arrow", output_format=OutputFormat.ARROW).data
    result_df = table.to_pandas()
    # TODO: Remove once better testing tools
    result_df = result_df.set_index("index")
    df.index.name = "index"
    assert_frame_equal(result_df, df)

def test_multi_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=[[chr(i%5) for i in range(10)], [i%2 for i in range(10)]]
    )
    df.index.names = ["index1", "index2"]
    lib.write("arrow", df)
    table = lib.read("arrow", output_format=OutputFormat.ARROW).data
    result_df = table.to_pandas()
    # TODO: Remove once better testing tools
    result_df = result_df.set_index(["index1", "index2"])
    assert_frame_equal(result_df, df)
