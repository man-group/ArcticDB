import numpy as np
import pytest
from arcticdb import QueryBuilder
from arcticdb_ext.storage import KeyType
import arcticdb.toolbox.query_stats as qs
import pandas as pd

from arcticdb.util.test import assert_frame_equal

# We create more data keys for rowcount and string indexes because these are just written like normal columns,
# in a TABLE_DATA key.
INDEXES_AND_EXPECTED_DATA_KEYS = [
    (pd.date_range("2025-01-01", periods=3), 6),
    (np.arange(0, 3, dtype=np.int64), 9),
    (["a", "b", "c"], 9),
    (pd.RangeIndex(3), 6),
]

INDEX_IDS = ["datetime", "rowcount", "string", "range"]


def create_df(index):
    df = pd.DataFrame(
        {
            "a": [0, 1, 2],
            "b": [0, 1, 2],
        }
    )
    df.index = index
    return df


def check_n_data_keys(lib, sym, expected_data_keys):
    lt = lib.library_tool()
    ks = lt.find_keys_for_symbol(KeyType.TABLE_DATA, sym)
    assert len(ks) == expected_data_keys


@pytest.fixture
def in_memory_version_store_single_element_segment(in_memory_store_factory):
    return in_memory_store_factory(column_group_size=1, segment_row_size=1)


@pytest.mark.parametrize("index_and_expected_data_keys", INDEXES_AND_EXPECTED_DATA_KEYS, ids=INDEX_IDS)
def test_column_slicing(
    in_memory_version_store_single_element_segment, index_and_expected_data_keys, clear_query_stats
):
    lib = in_memory_version_store_single_element_segment
    index, expected_total_data_keys = index_and_expected_data_keys
    df = create_df(index=index)

    lib.write("sym", df)
    check_n_data_keys(lib, "sym", expected_total_data_keys)

    qs.enable()

    lib.read("sym", columns=["a"])

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    # We prune off 3 keys by excluding "b"
    assert res["TABLE_DATA"]["count"] == expected_total_data_keys - 3


@pytest.mark.parametrize("index_and_expected_data_keys", INDEXES_AND_EXPECTED_DATA_KEYS, ids=INDEX_IDS)
def test_row_slicing(in_memory_version_store_single_element_segment, index_and_expected_data_keys, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    index, expected_total_data_keys = index_and_expected_data_keys
    df = create_df(index=index)

    lib.write("sym", df)
    check_n_data_keys(lib, "sym", expected_total_data_keys)

    qs.enable()

    lib.read("sym", row_range=(2, 3))

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == expected_total_data_keys / 3


@pytest.mark.parametrize("index_and_expected_data_keys", INDEXES_AND_EXPECTED_DATA_KEYS, ids=INDEX_IDS)
def test_row_slicing_just_index(
    in_memory_version_store_single_element_segment, index_and_expected_data_keys, clear_query_stats
):
    lib = in_memory_version_store_single_element_segment
    index, expected_total_data_keys = index_and_expected_data_keys
    df = create_df(index=index)

    lib.write("sym", df)
    check_n_data_keys(lib, "sym", expected_total_data_keys)

    qs.enable()

    result_df = lib.read("sym", row_range=(2, 3), columns=[], implement_read_index=True).data
    assert result_df.shape == (1, 0)

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    # RangeIndexes do not go through the normal read index path and incur an extra TABLE_DATA read.
    # This is due to the "count() > 0" check at the start of `build_col_read_query_filters`, which RangeIndex-s
    # (which are not physically stored) fail. We read every block within the row range for RangeIndex-s at the
    # moment.
    # Monday: 11524336137
    is_range_index = isinstance(index, pd.RangeIndex)
    assert res["TABLE_DATA"]["count"] == (2 if is_range_index else 1)


@pytest.mark.parametrize("cols_to_read", [["a"], ["b"]], ids=["a", "b"])
@pytest.mark.parametrize("index_and_expected_data_keys", INDEXES_AND_EXPECTED_DATA_KEYS, ids=INDEX_IDS)
def test_row_and_column_slicing(
    in_memory_version_store_single_element_segment, index_and_expected_data_keys, cols_to_read, clear_query_stats
):
    lib = in_memory_version_store_single_element_segment
    index, expected_data_keys = index_and_expected_data_keys
    df = create_df(index=index)

    lib.write("sym", df)
    check_n_data_keys(lib, "sym", expected_data_keys)

    qs.enable()
    res_df = lib.read("sym", row_range=(2, 3), columns=cols_to_read).data
    expected_df = df.iloc[2:3][cols_to_read]

    if isinstance(df.index, pd.RangeIndex):
        expected_df.index = pd.RangeIndex(0, 1)
    else:
        expected_df.index = index[2:3]

    assert_frame_equal(expected_df, res_df)

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    if expected_data_keys == 6:
        expected_reads = 1
    elif expected_data_keys == 9:
        expected_reads = 2  # also reading the block where the index is stored
    else:
        pytest.fail(f"Unexpected number of data keys {expected_data_keys}")
    assert res["TABLE_DATA"]["count"] == expected_reads


def test_daterange_slicing(in_memory_version_store_single_element_segment, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    index, expected_data_keys = INDEXES_AND_EXPECTED_DATA_KEYS[0]
    df = create_df(index=index)

    lib.write("sym", df)
    check_n_data_keys(lib, "sym", expected_data_keys)

    qs.enable()

    lib.read("sym", date_range=(pd.Timestamp("2025-01-02"), None))

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == 4


def test_daterange_and_column_slicing(in_memory_version_store_single_element_segment, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    index, expected_data_keys = INDEXES_AND_EXPECTED_DATA_KEYS[0]
    df = create_df(index=index)

    lib.write("sym", df)
    check_n_data_keys(lib, "sym", expected_data_keys)

    qs.enable()

    lib.read("sym", date_range=(pd.Timestamp("2025-01-02"), None), columns=["a"])

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == 2


def test_daterange_slicing_just_index(in_memory_version_store_single_element_segment, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    index, expected_data_keys = INDEXES_AND_EXPECTED_DATA_KEYS[0]
    df = create_df(index=index)

    lib.write("sym", df)
    check_n_data_keys(lib, "sym", expected_data_keys)

    qs.enable()

    res_df = lib.read("sym", date_range=(pd.Timestamp("2025-01-02"), None), columns=[], implement_read_index=True).data
    assert np.all(res_df.index.values == index[1:])
    assert res_df.shape == (2, 0)

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == 2
