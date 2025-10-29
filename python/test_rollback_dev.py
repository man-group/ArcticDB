import pytest

from arcticdb_ext.exceptions import StorageException
from arcticdb import Arctic
from arcticdb_ext.storage import KeyType
import pandas as pd
from arcticdb.options import LibraryOptions, EncodingVersion


@pytest.fixture
def lib():
    library_options = LibraryOptions(
        rows_per_segment=2,
        columns_per_segment=100,
    )

    endpoint = "lmdb://tmp/rollback"
    lib_name = "test_rollback3"
    arc = Arctic(endpoint)
    lib = arc.get_library(lib_name, create_if_missing=True, library_options=library_options)
    yield lib
    arc.delete_library(lib_name)


@pytest.mark.repeat(100)
def test_rollback_dev(lib):
    # lib = create_library("te`st", library_options=library_options)
    sym = "sym"

    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [7, 8, 9]})
    df2 = pd.DataFrame({"col6": [1, 2, 3], "col7": [4, 5, 6], "col8": [7, 8, 9]})
    # df.index = pd.date_range("1970-01-01", freq="D", periods=3)
    # df2.index = pd.date_range("1970-01-01", freq="D", periods=3)
    with pytest.raises(StorageException):
        lib._nvs.write("sym", [df, df2], recursive_normalizers=True)
    # print(lib.read("sym").data)
    lt = lib._nvs.library_tool()
    # keys = lt.find_keys_for_symbol(KeyType.MULTI_KEY, sym)
    # index_keys = lt.read_to_keys(keys[0])

    version_ref_count = lt.count_keys(KeyType.VERSION_REF)
    version_count = lt.count_keys(KeyType.VERSION)
    multi_count = lt.count_keys(KeyType.MULTI_KEY)
    index_count = lt.count_keys(KeyType.TABLE_INDEX)
    data_count = lt.count_keys(KeyType.TABLE_DATA)

    assert version_ref_count == 0
    assert version_count == 0
    assert multi_count == 0
    assert index_count == 0
    assert data_count == 0
