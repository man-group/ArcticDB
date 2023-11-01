from arcticdb import Arctic
import pandas as pd
import pytest

from arcticdb.util.test import assert_frame_equal

# TODO: When RocksDB is enabled on all platforms can remove this import
from arcticdb_ext.storage import LibraryManager

@pytest.mark.skipif(
    not LibraryManager.rocksdb_support,
    reason="RocksDB is not supported on conda builds"
)
def test_rocksdb(tmpdir):
    ac = Arctic(f"rocksdb://{tmpdir}/test_rocks")
    ac.create_library("test_lib")
    lib = ac["test_lib"]
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    lib.write("symbol", df)
    assert_frame_equal(lib.read("symbol").data, df)
