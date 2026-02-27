import numpy as np
import pytest
from arcticdb import QueryBuilder
import arcticdb.toolbox.query_stats as qs
import pandas as pd

INDEXES = [pd.date_range("2025-01-01", periods=3), np.arange(0, 3, dtype=np.int64), ["a", "b", "c"], pd.RangeIndex(3)]

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


@pytest.fixture
def in_memory_version_store_single_element_segment(in_memory_store_factory):
    return in_memory_store_factory(column_group_size=1, segment_row_size=1)


@pytest.mark.parametrize("index", INDEXES, ids=INDEX_IDS)
def test_column_slicing(in_memory_version_store_single_element_segment, index, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    df = create_df(index=index)

    lib.write("sym", df)

    qs.enable()

    lib.read("sym", columns=["a"])

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == 3


@pytest.mark.parametrize("index", INDEXES, ids=INDEX_IDS)
def test_row_slicing(in_memory_version_store_single_element_segment, index, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    df = create_df(index=index)

    lib.write("sym", df)

    qs.enable()

    lib.read("sym", row_range=(2, 3))

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == 2


@pytest.mark.parametrize("index", INDEXES, ids=INDEX_IDS)
def test_row_and_column_slicing(in_memory_version_store_single_element_segment, index, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    df = create_df(index=index)

    lib.write("sym", df)

    qs.enable()

    lib.read("sym", row_range=(2, 3), columns=["a"])

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == 1


def test_daterange_slicing(in_memory_version_store_single_element_segment, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    df = create_df(index=INDEXES[0])

    lib.write("sym", df)

    qs.enable()

    lib.read("sym", date_range=(pd.Timestamp("2025-01-02"), None))

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == 4


def test_daterange_and_column_slicing(in_memory_version_store_single_element_segment, clear_query_stats):
    lib = in_memory_version_store_single_element_segment
    df = create_df(index=INDEXES[0])

    lib.write("sym", df)

    qs.enable()

    lib.read("sym", date_range=(pd.Timestamp("2025-01-02"), None), columns=["a"])

    res = qs.get_query_stats()["storage_operations"]["Memory_GetObject"]

    assert res["TABLE_INDEX"]["count"] == 1
    assert res["TABLE_DATA"]["count"] == 2
