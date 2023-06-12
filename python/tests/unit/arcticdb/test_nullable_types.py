import os
from pandas.testing import assert_frame_equal, assert_series_equal
import pandas as pd
import numpy as np

from arcticc.config import set_log_level
set_log_level("TRACE")

def test_numpy_array_single(lmdb_version_store):
    idx1 = np.arange(10)
    d1 = {"x": [None, None, np.array([7], dtype=np.int64), None, None, np.array([8], dtype=np.int64), None, None, np.array([8], dtype=np.int64), None]}
    expected = pd.DataFrame(data=d1, index=idx1)
    lmdb_version_store.write("array_small", expected)
    vit = lmdb_version_store.read("array_small")
    assert_frame_equal(expected, vit.data)


def test_numpy_array_small(lmdb_version_store):
    idx1 = np.arange(10)
    d1 = {"x": [None, None, np.array([7, 0, 9], dtype=np.int64), None, None, np.array([8, 7], dtype=np.int64), None, None, np.array([8, 1, 4, 9], dtype=np.int64), None]}
    expected = pd.DataFrame(data=d1, index=idx1)
    lmdb_version_store.write("array_small", expected)
    vit = lmdb_version_store.read("array_small")
    assert_frame_equal(expected, vit.data)


def test_numpy_array_double(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    idx1 = np.arange(10)
    d1 = {
        "x": [None, None, np.array([7, 0, 9], dtype=np.int64), None, None, np.array([8, 7], dtype=np.int64), None, None, np.array([8, 1, 4, 9], dtype=np.int64), None],
        "y": [np.array([1, 2, 3], dtype=np.int64), None, None, np.array([4, 5, 6], dtype=np.int64), None, np.array([1, 2], dtype=np.int64), None, None, None, np.array([4, 3, 2, 1], dtype=np.int64)]
    }
    expected = pd.DataFrame(data=d1, index=idx1)
    lib.write("array_small", expected)
    vit = lib.read("array_small")
    assert_frame_equal(expected, vit.data)


def test_numpy_array_twin(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    idx1 = np.arange(3)
    d1 = {
        "x": [None, None, np.array([1, 2, 3], dtype=np.int64)],
        "y": [np.array([1, 2, 3], dtype=np.int64), np.array([4, 5, 6], dtype=np.int64), None]
    }
    expected = pd.DataFrame(data=d1, index=idx1)
    lib.write("array_small", expected)
    vit = lib.read("array_small")
    assert_frame_equal(expected, vit.data)


def test_bbg_tick_data(mongo_version_store):
    lib = mongo_version_store
    current_path = os.getcwd()
    base_path = "{}/tests/artifacts".format(current_path)
    file = "20220714_224105_0x2045.parquet.00.005"
    file_path = "{}/{}".format(base_path, file)

    file = pq.ParquetFile(file_path)
    rg = file.read_row_group(1)
    df = rg.to_pandas()
    lib.write("nullable", df, dynamic_strings=True)
    #df.to_csv("/opt/arcticc/nogit/df.txt")
    vit = lib.read("nullable")
    #vit.data.to_csv("/opt/arcticc/nogit/ret.txt")
    assert_frame_equal(df, vit.data)
