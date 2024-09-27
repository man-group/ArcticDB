from arcticdb_ext.version_store import OutputFormat
import pandas as pd
import numpy as np

from pandas.testing import assert_frame_equal


def test_basic_roundtrip(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10)})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.to_pandas()
    print(vit)
    assert_frame_equal(result, df)


def test_double_columns_roundtrip(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10.0, 20.0)})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.to_pandas()
    print(vit)
    assert_frame_equal(result, df)


def test_strings_roundtrip(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"x": ["mene", "mene", "tekel", "upharsin"]})
    lib.write("arrow", df)
    vit = lib.read("arrow", output_format=OutputFormat.ARROW)
    result = vit.to_pandas()
    print(vit)
    assert_frame_equal(result, df)