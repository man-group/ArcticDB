"""
Copyright 2023 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from arcticdb_ext.storage import KeyType, NoDataFoundException


def get_data_keys(lib, symbol):
    lt = lib.library_tool()
    data_keys = lt.find_keys(KeyType.TABLE_DATA)

    return [v for v in data_keys if v.id == symbol]


def get_multi_data_keys(lib, symbol):
    lt = lib.library_tool()
    data_keys = lt.find_keys(KeyType.TABLE_DATA)

    return [v for v in data_keys if symbol in v.id]


def comp_dict(d1, d2):
    assert len(d1) == len(d2)
    for k in d1:
        if isinstance(d1[k], np.ndarray):
            assert (d1[k] == d2[k]).all()
        else:
            assert d1[k] == d2[k]


def test_basic_de_dup(basic_store_factory):
    lib = basic_store_factory(column_group_size=2, segment_row_size=2, de_duplication=True)
    symbol = "test_basic_de_dup"

    num_elements = 100

    # This will insert 50 data keys
    d1 = {"x": np.arange(0, num_elements, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)

    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    d2 = {"x": np.arange(num_elements, 2 * num_elements, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2)
    # This will insert 100 data keys, but 50 will be de-duped. So total 100
    new_df = pd.concat([df1, df2], ignore_index=True)
    lib.write(symbol, new_df)
    assert_frame_equal(lib.read(symbol).data, new_df)

    assert len(get_data_keys(lib, symbol)) == num_elements


def test_de_dup_same_value_written(basic_store_factory):
    lib = basic_store_factory(column_group_size=2, segment_row_size=2, de_duplication=True)
    symbol = "test_de_dup_same_value_written"

    # This will insert 50 data keys
    d1 = {"x": np.arange(0, 100, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)

    num_keys = len(get_data_keys(lib, symbol))

    lib.write(symbol, df1)

    assert len(lib.list_versions(symbol)) == 2
    assert len(get_data_keys(lib, symbol)) == num_keys

    lib.write(symbol, df1, prune_previous_version=True)
    assert len(lib.list_versions(symbol)) == 1
    assert len(get_data_keys(lib, symbol)) == num_keys


def test_de_dup_with_delete(basic_store_factory):
    lib = basic_store_factory(column_group_size=2, segment_row_size=2, de_duplication=True)
    symbol = "test_de_dup_with_delete"

    num_elements = 100

    idx1 = np.arange(0, num_elements)
    d1 = {"x": np.arange(0, num_elements, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)

    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    idx2 = np.arange(num_elements, 2 * num_elements)
    d2 = {"x": np.arange(num_elements, 2 * num_elements, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    new_df = pd.concat([df1, df2])
    lib.write(symbol, new_df)
    assert_frame_equal(lib.read(symbol).data, new_df)

    assert len(get_data_keys(lib, symbol)) == num_elements

    idx3 = np.arange(2 * num_elements, 3 * num_elements)
    d3 = {"x": np.arange(2 * num_elements, 3 * num_elements, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    final_df = pd.concat([df2, df3])
    lib.write(symbol, final_df)
    assert_frame_equal(lib.read(symbol).data, final_df)
    assert_frame_equal(lib.read(symbol, as_of=1).data, new_df)
    assert_frame_equal(lib.read(symbol, as_of=0).data, df1)

    # This won't be de-duped as the there is data overlap but the start_index and end_index dont match for segments
    assert len(get_data_keys(lib, symbol)) == 2 * num_elements

    lib.delete_version(symbol, 1)
    assert_frame_equal(lib.read(symbol).data, final_df)
    assert_frame_equal(lib.read(symbol, as_of=0).data, df1)
    with pytest.raises(NoDataFoundException):
        lib.read(symbol, as_of=1)

    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.delete_version(symbol, 2)
    assert_frame_equal(lib.read(symbol).data, df1)
    with pytest.raises(NoDataFoundException):
        lib.read(symbol, as_of=1)
    with pytest.raises(NoDataFoundException):
        lib.read(symbol, as_of=2)

    lib.write(symbol, final_df, prune_previous_version=True)
    assert_frame_equal(lib.read(symbol).data, final_df)
    assert len(get_data_keys(lib, symbol)) == num_elements


def test_de_dup_with_snapshot(basic_store_factory):
    lib = basic_store_factory(column_group_size=2, segment_row_size=2, de_duplication=True)
    symbol = "test_de_dup_with_snapshot"

    num_elements = 100

    idx1 = np.arange(0, num_elements)
    d1 = {"x": np.arange(0, num_elements, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)

    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    idx2 = np.arange(num_elements, 2 * num_elements)
    d2 = {"x": np.arange(num_elements, 2 * num_elements, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    new_df = pd.concat([df1, df2])
    lib.write(symbol, new_df)
    assert_frame_equal(lib.read(symbol).data, new_df)

    assert len(get_data_keys(lib, symbol)) == num_elements

    lib.snapshot("my_snap")

    idx3 = np.arange(2 * num_elements, 3 * num_elements)
    d3 = {"x": np.arange(2 * num_elements, 3 * num_elements, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    final_df = pd.concat([new_df, df3])
    lib.write(symbol, final_df)

    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.delete_version(symbol, 0)
    assert_frame_equal(lib.read(symbol, as_of="my_snap").data, new_df)

    lib.delete_version(symbol, 2)
    assert_frame_equal(lib.read(symbol, as_of="my_snap").data, new_df)


def test_de_dup_with_tombstones(basic_store_factory):
    lib = basic_store_factory(column_group_size=2, segment_row_size=2, de_duplication=True, use_tombstones=True)
    symbol = "test_de_dup_with_tombstones"

    num_elements = 100

    idx1 = np.arange(0, num_elements)
    d1 = {"x": np.arange(0, num_elements, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)

    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    idx2 = np.arange(num_elements, 2 * num_elements)
    d2 = {"x": np.arange(num_elements, 2 * num_elements, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    new_df = pd.concat([df1, df2])
    lib.write(symbol, new_df)
    assert_frame_equal(lib.read(symbol).data, new_df)

    assert len(get_data_keys(lib, symbol)) == num_elements

    idx3 = np.arange(2 * num_elements, 3 * num_elements)
    d3 = {"x": np.arange(2 * num_elements, 3 * num_elements, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    final_df = pd.concat([df1, df2, df3])
    lib.write(symbol, final_df)
    assert_frame_equal(lib.read(symbol).data, final_df)
    assert_frame_equal(lib.read(symbol, as_of=1).data, new_df)
    assert_frame_equal(lib.read(symbol, as_of=0).data, df1)

    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.write(symbol, final_df)

    # complete de-dup
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.delete_version(symbol, 3)
    lib.delete_version(symbol, 2)
    assert_frame_equal(lib.read(symbol).data, new_df)
    assert_frame_equal(lib.read(symbol, as_of=0).data, df1)
    with pytest.raises(NoDataFoundException):
        lib.read(symbol, as_of=3)
    with pytest.raises(NoDataFoundException):
        lib.read(symbol, as_of=2)

    # tomstones deletes data (it doesn't delete data when delayed deletes is on)
    assert len(get_data_keys(lib, symbol)) == num_elements

    lib.write(symbol, final_df, prune_previous_version=True)
    assert_frame_equal(lib.read(symbol).data, final_df)

    # won't dedup with the tombstoned version
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2


def test_snapshot_dedup_basic(basic_store_factory):
    lib = basic_store_factory(
        column_group_size=2, segment_row_size=2, de_duplication=True, use_tombstones=True, snapshot_dedup=True
    )
    symbol = "test_snapshot_dedup_basic"

    num_elements = 100

    idx1 = np.arange(0, num_elements)
    d1 = {"x": np.arange(0, num_elements, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)
    lib.snapshot("my_snap1")
    lib.delete(symbol)

    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    idx2 = np.arange(num_elements, 2 * num_elements)
    d2 = {"x": np.arange(num_elements, 2 * num_elements, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    new_df = pd.concat([df1, df2])
    lib.write(symbol, new_df)
    assert_frame_equal(lib.read(symbol).data, new_df)

    # Half of the new dataframe was deduped with the snapshot
    assert len(get_data_keys(lib, symbol)) == num_elements

    lib.delete_snapshot("my_snap1")
    assert_frame_equal(lib.read(symbol).data, new_df)

    lib.delete(symbol)

    assert len(get_data_keys(lib, symbol)) == 0

    lib.write(symbol, new_df)
    assert_frame_equal(lib.read(symbol).data, new_df)

    assert len(get_data_keys(lib, symbol)) == num_elements


def test_snapshot_dedup_multiple1(basic_store_factory):
    lib = basic_store_factory(
        column_group_size=2, segment_row_size=2, de_duplication=True, use_tombstones=True, snapshot_dedup=True
    )
    symbol = "test_snapshot_dedup_multiple1"

    num_elements = 100

    idx1 = np.arange(0, num_elements)
    d1 = {"x": np.arange(0, num_elements, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)
    lib.snapshot("my_snap1")

    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    idx2 = np.arange(num_elements, 2 * num_elements)
    d2 = {"x": np.arange(num_elements, 2 * num_elements, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    new_df = pd.concat([df1, df2])
    lib.write(symbol, new_df)
    assert_frame_equal(lib.read(symbol).data, new_df)
    lib.snapshot("my_snap2")

    # Half of the new dataframe was deduped
    assert len(get_data_keys(lib, symbol)) == num_elements

    idx3 = np.arange(2 * num_elements, 3 * num_elements)
    d3 = {"x": np.arange(2 * num_elements, 3 * num_elements, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    final_df = pd.concat([new_df, df3])
    lib.write(symbol, final_df)
    lib.snapshot("my_snap3")

    # Only non-deduped was written out
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.delete(symbol)

    lib.write(symbol, final_df)
    assert_frame_equal(lib.read(symbol).data, final_df)
    # complete de-dup with the my_snap3
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.delete_snapshot("my_snap3")
    lib.delete(symbol)

    assert len(get_data_keys(lib, symbol)) == num_elements

    lib.write(symbol, final_df)
    assert_frame_equal(lib.read(symbol).data, final_df)
    # partial de-dup with my_snap2
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.delete_snapshot("my_snap2")
    lib.delete(symbol)

    # only my_snap1 remains
    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    lib.write(symbol, final_df)
    assert_frame_equal(lib.read(symbol).data, final_df)
    # partial de-dup with my_snap1
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2


def test_snapshot_dedup_multiple2(basic_store_factory):
    lib = basic_store_factory(
        column_group_size=2, segment_row_size=2, de_duplication=True, use_tombstones=True, snapshot_dedup=True
    )
    symbol = "test_snapshot_dedup_multiple2"

    num_elements = 100

    idx1 = np.arange(0, num_elements)
    d1 = {"x": np.arange(0, num_elements, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)
    lib.snapshot("my_snap1")

    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    idx2 = np.arange(num_elements, 2 * num_elements)
    d2 = {"x": np.arange(num_elements, 2 * num_elements, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    new_df = pd.concat([df1, df2])
    lib.write(symbol, new_df)
    assert_frame_equal(lib.read(symbol).data, new_df)

    # Half of the new dataframe was deduped
    assert len(get_data_keys(lib, symbol)) == num_elements

    idx3 = np.arange(2 * num_elements, 3 * num_elements)
    d3 = {"x": np.arange(2 * num_elements, 3 * num_elements, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    final_df = pd.concat([new_df, df3])
    lib.write(symbol, final_df)
    lib.snapshot("my_snap3")

    # Only non-deduped was written out
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.delete(symbol)

    lib.write(symbol, final_df)
    assert_frame_equal(lib.read(symbol).data, final_df)
    # complete de-dup with the my_snap3
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.delete_snapshot("my_snap3")
    lib.delete(symbol)

    assert len(get_data_keys(lib, symbol)) == num_elements / 2

    lib.write(symbol, final_df)
    assert_frame_equal(lib.read(symbol).data, final_df)
    # partial de-dup with my_snap1
    assert len(get_data_keys(lib, symbol)) == 3 * num_elements / 2


def test_dedup_multi_keys(basic_store_factory):
    lib = basic_store_factory(
        column_group_size=2, segment_row_size=2, de_duplication=True, use_tombstones=True, snapshot_dedup=True
    )
    symbol = "test_dedup_multi_keys"
    num_elements = 100

    data1 = {"e": np.arange(num_elements), "f": np.arange(2 * num_elements), "g": None}

    lib.write(symbol, data=data1, metadata="realyolo2", recursive_normalizers=True)

    assert len(get_multi_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.write(symbol, data=data1, metadata="realyolo2", recursive_normalizers=True)
    comp_dict(data1, lib.read(symbol).data)
    # complete de-dup
    assert len(get_multi_data_keys(lib, symbol)) == 3 * num_elements / 2

    data2 = {"e": np.arange(2 * num_elements)}
    lib.write(symbol, data=data2, metadata="realyolo2", recursive_normalizers=True)
    comp_dict(data2, lib.read(symbol).data)

    # complete de-dup, "e" matches with "f"
    assert len(get_multi_data_keys(lib, symbol)) == 3 * num_elements / 2

    data3 = {"e": np.arange(4 * num_elements), "f": np.arange(3 * num_elements)}
    lib.write(symbol, data=data3, metadata="realyolo2", recursive_normalizers=True)
    comp_dict(data3, lib.read(symbol).data)

    # partial de-dup
    assert len(get_multi_data_keys(lib, symbol)) == 3 * num_elements

    lib.delete_version(symbol, 2)

    comp_dict(data3, lib.read(symbol).data)
    comp_dict(data1, lib.read(symbol, 0).data)
    comp_dict(data1, lib.read(symbol, 1).data)

    lib.delete_version(symbol, 0)

    comp_dict(data3, lib.read(symbol).data)
    comp_dict(data1, lib.read(symbol, 1).data)

    lib.delete_version(symbol, 1)

    comp_dict(data3, lib.read(symbol).data)


def test_dedup_multi_keys_snapshot(basic_store_factory):
    lib = basic_store_factory(
        column_group_size=2, segment_row_size=2, de_duplication=True, use_tombstones=True, snapshot_dedup=True
    )
    symbol = "test_dedup_multi_keys"
    num_elements = 100

    data1 = {"e": np.arange(num_elements), "f": np.arange(2 * num_elements), "g": None}

    lib.write(symbol, data=data1, metadata="realyolo2", recursive_normalizers=True)
    lib.snapshot("mysnap1")
    lib.delete(symbol)
    assert len(get_multi_data_keys(lib, symbol)) == 3 * num_elements / 2

    lib.write(symbol, data=data1, metadata="realyolo2", recursive_normalizers=True)
    lib.snapshot("mysnap2")
    comp_dict(data1, lib.read(symbol).data)
    # complete de-dup with snapshot
    assert len(get_multi_data_keys(lib, symbol)) == 3 * num_elements / 2
    lib.delete(symbol)

    data2 = {"e": np.arange(2 * num_elements)}
    lib.write(symbol, data=data2, metadata="realyolo2", recursive_normalizers=True)
    comp_dict(data2, lib.read(symbol).data)

    # complete de-dup, "e" matches with "f"
    assert len(get_multi_data_keys(lib, symbol)) == 3 * num_elements / 2
    lib.delete(symbol)

    data3 = {"e": np.arange(4 * num_elements), "f": np.arange(3 * num_elements)}
    lib.write(symbol, data=data3, metadata="realyolo2", recursive_normalizers=True)
    comp_dict(data3, lib.read(symbol).data)

    # partial de-dup
    assert len(get_multi_data_keys(lib, symbol)) == 3 * num_elements

    comp_dict(data1, lib.read(symbol, "mysnap1").data)
    comp_dict(data1, lib.read(symbol, "mysnap2").data)
