"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import sys
import numpy as np
import pandas as pd
import pytest
import random

from arcticdb_ext.exceptions import UserInputException
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb.util.test import config_context, random_string, assert_frame_equal, distinct_timestamps


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def comp_dict(d1, d2):
    assert len(d1) == len(d2)
    for k in d1:
        if isinstance(d1[k], np.ndarray):
            assert (d1[k] == d2[k]).all()
        else:
            assert d1[k] == d2[k]


SECOND = 1000000000
MAP_TIMEOUTS = [0, SECOND * 5, SECOND * 10000]


@pytest.mark.parametrize("map_timeout", MAP_TIMEOUTS)
@pytest.mark.storage
def test_delete_all(map_timeout, object_and_mem_and_lmdb_version_store, sym):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        lib = object_and_mem_and_lmdb_version_store
        symbol = sym
        df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
        lib.write(symbol, df1)
        df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
        lib.write(symbol, df2)
        df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
        lib.write(symbol, df3)
        vit = lib.read(symbol)
        assert_frame_equal(vit.data, df3)
        lib.delete(symbol)
        assert lib.has_symbol(symbol) is False
        lib.write(symbol, df2)
        vit = lib.read(symbol)
        assert_frame_equal(vit.data, df2)
        lib.write(symbol, df1)
        vit = lib.read(symbol)
        assert_frame_equal(vit.data, df1)


@pytest.mark.storage
def test_version_missing(object_version_store):
    with pytest.raises(NoDataFoundException):
        object_version_store.read("not_there")


@pytest.mark.parametrize("idx", [0, 1, 2])
def test_delete_version_basic(s3_version_store, idx, sym):
    object_version_store = s3_version_store
    symbol = sym
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    object_version_store.write(symbol, df1)
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    object_version_store.write(symbol, df2)
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
    object_version_store.write(symbol, df3)
    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, df3)

    dfs = [df1, df2, df3]

    assert len(object_version_store.list_versions(symbol)) == 3

    object_version_store.delete_version(symbol, idx)

    with pytest.raises(NoDataFoundException):
        object_version_store.read(symbol, idx)
    assert len(object_version_store.list_versions(symbol)) == 2
    if idx != 2:
        assert_frame_equal(object_version_store.read(symbol).data, df3)
    else:
        assert_frame_equal(object_version_store.read(symbol).data, df2)

    assert_frame_equal(object_version_store.read(symbol, (idx - 1) % 3).data, dfs[(idx - 1) % 3])
    assert_frame_equal(object_version_store.read(symbol, (idx - 2) % 3).data, dfs[(idx - 2) % 3])

    object_version_store.delete_version(symbol, (idx + 1) % 3)
    with pytest.raises(NoDataFoundException):
        object_version_store.read(symbol, (idx + 1) % 3)
    with pytest.raises(NoDataFoundException):
        object_version_store.read(symbol, idx)
    assert len(object_version_store.list_versions(symbol)) == 1
    if idx == 2:
        assert_frame_equal(object_version_store.read(symbol).data, df2)
    elif idx == 1:
        assert_frame_equal(object_version_store.read(symbol).data, df1)
    else:
        assert_frame_equal(object_version_store.read(symbol).data, df3)
    assert_frame_equal(object_version_store.read(symbol, (idx + 2) % 3).data, dfs[(idx + 2) % 3])

    object_version_store.delete_version(symbol, (idx + 2) % 3)
    with pytest.raises(NoDataFoundException):
        object_version_store.read(symbol, (idx + 2) % 3)
    with pytest.raises(NoDataFoundException):
        object_version_store.read(symbol, (idx + 1) % 3)
    with pytest.raises(NoDataFoundException):
        object_version_store.read(symbol, idx)
    assert len(object_version_store.list_versions(symbol)) == 0


@pytest.mark.parametrize("versions", [[], [0, 1], [1, 2], [0, 2], [0, 1, 2], [0, 0, 1, 2], [0, 1, 1, 2], [0, 1, 2, 2]])
def test_delete_versions_basic(s3_version_store, versions, sym):
    object_version_store = s3_version_store
    symbol = sym
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    object_version_store.write(symbol, df1)
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    object_version_store.write(symbol, df2)
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
    object_version_store.write(symbol, df3)
    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, df3)

    dfs = [df1, df2, df3]

    assert len(object_version_store.list_versions(symbol)) == 3

    object_version_store.delete_versions(symbol, versions)

    for idx in versions:
        with pytest.raises(NoDataFoundException):
            object_version_store.read(symbol, idx)
    assert len(object_version_store.list_versions(symbol)) == len(dfs) - len(set(versions))


@pytest.mark.storage
def test_delete_version_with_batch_write(object_version_store, sym):
    sym_1 = sym
    sym_2 = "another-{}".format(sym)
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    object_version_store.batch_write([sym_1, sym_2], [df1, df1])

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    object_version_store.batch_write([sym_1, sym_2], [df2, df2])
    vit = object_version_store.batch_read([sym_1, sym_2])
    expected = df2
    assert vit[sym_1].version == 1
    assert vit[sym_2].version == 1
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)

    object_version_store.delete_version(sym_2, 1)

    assert_frame_equal(object_version_store.read(sym_2).data, df1)
    assert_frame_equal(object_version_store.read(sym_2, 0).data, df1)
    idx3 = np.arange(2000000, 3000000)
    d3 = {"x": np.arange(3000000, 4000000, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    object_version_store.batch_write([sym_1, sym_2], [df3, df3])
    vit = object_version_store.batch_read([sym_1, sym_2])
    expected = df3
    assert vit[sym_1].version == 2
    assert vit[sym_2].version == 2
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)


def check_tombstones_after_multiple_delete(lib_tool, symbol, versions_to_delete, num_versions_written):
    ref_key = lib_tool.find_keys_for_symbol(KeyType.VERSION_REF, symbol)[0]
    assert len(lib_tool.find_keys(KeyType.VERSION)) == num_versions_written + 1
    ver_ref_entries = lib_tool.read_to_keys(ref_key)

    tombstone_key = ver_ref_entries[0]
    tombstone_ver = ver_ref_entries[1]
    assert len(ver_ref_entries) == 2 and tombstone_key.version_id == tombstone_ver.version_id == max(versions_to_delete)
    keys_in_tombstone_ver = lib_tool.read_to_keys(tombstone_ver)
    assert len(keys_in_tombstone_ver) == len(versions_to_delete) + 1
    tombstone_entries = [k for k in keys_in_tombstone_ver if k.type == KeyType.TOMBSTONE]
    assert len(tombstone_entries) == len(versions_to_delete)
    assert [t.version_id for t in tombstone_entries] == sorted(versions_to_delete, reverse=True)
    previous_version_key = keys_in_tombstone_ver[-1]
    assert previous_version_key.type == KeyType.VERSION
    assert previous_version_key.version_id == num_versions_written - 1
    # The indexes should be deleted but the data should still be there
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == num_versions_written - len(versions_to_delete)


@pytest.mark.parametrize("idx", [0, 1])
@pytest.mark.storage
def test_delete_version_with_append(object_version_store, idx, sym):
    symbol = sym
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    object_version_store.write(symbol, df1)
    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    object_version_store.append(symbol, df2)
    vit = object_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)

    object_version_store.delete_version(symbol, idx)
    assert len(object_version_store.list_versions(symbol)) == 1

    if idx == 0:
        assert_frame_equal(object_version_store.read(symbol).data, expected)
        assert_frame_equal(object_version_store.read(symbol, 1).data, expected)
    else:
        assert_frame_equal(object_version_store.read(symbol).data, df1)
        assert_frame_equal(object_version_store.read(symbol, 0).data, df1)
        idx3 = np.arange(2000000, 3000000)
        d3 = {"x": np.arange(3000000, 4000000, dtype=np.int64)}
        df3 = pd.DataFrame(data=d3, index=idx3)
        object_version_store.append(symbol, df3)
        vit = object_version_store.read(symbol)
        expected = pd.concat([df1, df3])
        assert_frame_equal(vit.data, expected)
        assert vit.version == 2


@pytest.mark.parametrize("versions_to_delete", [[0, 1], [1, 2], [0, 2], [0, 1, 2], [2, 3], [0, 3]])
@pytest.mark.storage
def test_delete_versions_with_append(object_version_store, versions_to_delete, sym):
    symbol = sym
    dfs = []
    rows = 100
    vers = 4
    for i in range(vers):
        idx = np.arange(i * rows, (i + 1) * rows)
        d = {"x": np.arange(i * rows, (i + 1) * rows, dtype=np.int64)}
        df = pd.DataFrame(data=d, index=idx)
        object_version_store.append(symbol, df)
        dfs.append(df)
        vit = object_version_store.read(symbol)
        assert_frame_equal(vit.data, pd.concat(dfs))

    lib_tool = object_version_store.library_tool()

    assert len(lib_tool.find_keys(KeyType.VERSION)) == vers
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == vers
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == vers

    object_version_store.delete_versions(symbol, versions_to_delete)
    assert len(object_version_store.list_versions(symbol)) == len(dfs) - len(versions_to_delete)
    check_tombstones_after_multiple_delete(lib_tool, symbol, versions_to_delete, vers)

    if versions_to_delete == [2, 3]:
        # The data from the recent versions should be deleted
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 2
        assert_frame_equal(object_version_store.read(symbol).data, pd.concat(dfs[:-2]))
    elif versions_to_delete == [0, 3]:
        # only the the data from the latest version should be deleted
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 3
        assert_frame_equal(object_version_store.read(symbol).data, pd.concat(dfs[:-1]))
    else:
        # data from the past versions is should still be present
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == vers
        assert_frame_equal(object_version_store.read(symbol).data, pd.concat(dfs))


@pytest.mark.parametrize("versions_to_delete", [[0, 1], [1, 2], [0, 2], [0, 1, 2], [2, 3], [0, 3]])
@pytest.mark.storage
def test_delete_versions_with_append_large_data(object_version_store, versions_to_delete, sym):
    symbol = sym
    dfs = []
    rows = 1000000
    vers = 4
    for i in range(vers):
        idx = np.arange(i * rows, (i + 1) * rows)
        d = {"x": np.arange(i * rows, (i + 1) * rows, dtype=np.int64)}
        df = pd.DataFrame(data=d, index=idx)
        object_version_store.append(symbol, df)
        dfs.append(df)
        vit = object_version_store.read(symbol)
        assert_frame_equal(vit.data, pd.concat(dfs))

    lib_tool = object_version_store.library_tool()

    assert len(lib_tool.find_keys(KeyType.VERSION)) == vers
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == vers
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == vers * 10

    object_version_store.delete_versions(symbol, versions_to_delete)
    assert len(object_version_store.list_versions(symbol)) == len(dfs) - len(versions_to_delete)
    check_tombstones_after_multiple_delete(lib_tool, symbol, versions_to_delete, vers)

    if versions_to_delete == [2, 3]:
        # The data from the recent versions should be deleted
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 20
        assert_frame_equal(object_version_store.read(symbol).data, pd.concat(dfs[:-2]))
    elif versions_to_delete == [0, 3]:
        # only the the data from the latest version should be deleted
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 30
        assert_frame_equal(object_version_store.read(symbol).data, pd.concat(dfs[:-1]))
    else:
        # data from the past versions is should still be present
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == vers * 10
        assert_frame_equal(object_version_store.read(symbol).data, pd.concat(dfs))


@pytest.mark.parametrize("versions_to_delete", [[0, 1], [1, 2], [0, 2], [0, 1, 2], [2, 3], [0, 3]])
@pytest.mark.storage
def test_delete_versions_with_update(object_version_store, versions_to_delete, sym):
    symbol = sym
    dfs = []
    idx_start = pd.Timestamp("2000-1-1")
    rows = 100

    def build_expected_df(dataframes):
        expected = pd.DataFrame()
        for d in dataframes:
            expected = expected.combine_first(d)
            if not expected.empty:
                expected.loc[d.index] = d
        # pandas 1.0 patch, otherwise the col will be float64
        expected['x'] = expected['x'].astype('int64')
        return expected

    # Write initial data
    vers = 4
    for i in range(vers):
        overlap_start = idx_start + pd.Timedelta(seconds=(i * (rows / 2)))
        idx = pd.date_range(overlap_start, periods=rows, freq="s")
        d = {"x": np.arange(i * rows, (i + 1) * rows, dtype=np.int64)}
        df = pd.DataFrame(data=d, index=idx)
        object_version_store.update(symbol, df, upsert=True)
        dfs.append(df)
        assert_frame_equal(object_version_store.read(symbol).data, build_expected_df(dfs))

    lib_tool = object_version_store.library_tool()

    assert len(lib_tool.find_keys(KeyType.VERSION)) == vers
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == vers
    # Versions + (versions - 1) because of the overlapping parts
    expected_data_keys = vers + (vers - 1)
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == expected_data_keys

    # Delete versions and verify
    object_version_store.delete_versions(symbol, versions_to_delete)
    assert len(object_version_store.list_versions(symbol)) == len(dfs) - len(versions_to_delete)
    check_tombstones_after_multiple_delete(lib_tool, symbol, versions_to_delete, vers)

    # Determine which versions to keep based on deletion pattern
    if versions_to_delete == [2, 3]:
        remaining_dfs = dfs[:-2]  # Keep first two versions
        # Data keys:
        # 0 - original
        # 1 - part that overlaps with v0 (original from v0 that is shared with v1 + new from v1)
        # 1 - new data from the v1
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 3
    elif versions_to_delete == [0, 3]:
        remaining_dfs = dfs[:-1]  # Keep all but last version
        # Data keys:
        # 1 - part that overlaps with v0 (original from v0 that is shared with v1 + new from v1)
        # 1 - new data from the v1
        # 2 - part that overlaps with v1 (original from v1 that is shared with v2 + new from v2)
        # 2 - new data from the v2
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 4
    else:
        remaining_dfs = dfs
        # For the rest of the cases, we should delete as many data keys as versions to delete
        assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == expected_data_keys - len(versions_to_delete)

    assert_frame_equal(object_version_store.read(symbol).data, build_expected_df(remaining_dfs))


@pytest.mark.parametrize("versions_to_delete", [[0, 1], [1, 2], [0, 2], [0, 1, 2], [2, 3], [0, 3]])
@pytest.mark.storage
def test_delete_versions_with_update_large_data(object_version_store, versions_to_delete, sym):
    symbol = sym
    dfs = []
    idx_start = pd.Timestamp("2000-1-1")
    rows = 1000000

    def build_expected_df(dataframes):
        expected = pd.DataFrame()
        for d in dataframes:
            expected = expected.combine_first(d)
            if not expected.empty:
                expected.loc[d.index] = d
        # pandas 1.0 patch, otherwise the col will be float64
        expected['x'] = expected['x'].astype('int64')
        return expected

    # Write initial data
    vers = 4
    for i in range(vers):
        overlap_start = idx_start + pd.Timedelta(seconds=(i * (rows / 2)))
        idx = pd.date_range(overlap_start, periods=rows, freq="s")
        d = {"x": np.arange(i * rows, (i + 1) * rows, dtype=np.int64)}
        df = pd.DataFrame(data=d, index=idx)
        object_version_store.update(symbol, df, upsert=True)
        dfs.append(df)
        assert_frame_equal(object_version_store.read(symbol).data, build_expected_df(dfs))

    lib_tool = object_version_store.library_tool()

    assert len(lib_tool.find_keys(KeyType.VERSION)) == vers
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == vers
    # Because the data is large, each version is split in 10 data keys
    # after the first version, the data in half of those overlaps with the previous version
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == vers * 10
    # Delete versions and verify
    object_version_store.delete_versions(symbol, versions_to_delete)
    assert len(object_version_store.list_versions(symbol)) == len(dfs) - len(versions_to_delete)

    check_tombstones_after_multiple_delete(lib_tool, symbol, versions_to_delete, vers)

    # Determine which versions to keep based on deletion pattern
    if versions_to_delete == [2, 3]:
        remaining_dfs = dfs[:-2]  # Keep first two versions
        expected_data_keys = 20
    elif versions_to_delete == [0, 3]:
        remaining_dfs = dfs[:-1]
        expected_data_keys = 25
    else:
        remaining_dfs = dfs
        expected_data_keys = vers * 10 - (len(versions_to_delete) * 5)

    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == expected_data_keys

    assert_frame_equal(object_version_store.read(symbol).data, build_expected_df(remaining_dfs))


@pytest.mark.storage
def test_delete_version_with_batch_append(object_version_store, sym):
    sym_1 = sym
    sym_2 = "another-{}".format(sym)
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    object_version_store.batch_write([sym_1, sym_2], [df1, df1])

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    object_version_store.batch_append([sym_1, sym_2], [df2, df2])
    vit = object_version_store.batch_read([sym_1, sym_2])
    expected = pd.concat([df1, df2])
    assert vit[sym_1].version == 1
    assert vit[sym_2].version == 1
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)

    object_version_store.delete_version(sym_2, 1)

    assert_frame_equal(object_version_store.read(sym_2).data, df1)
    assert_frame_equal(object_version_store.read(sym_2, 0).data, df1)
    idx3 = np.arange(2000000, 3000000)
    d3 = {"x": np.arange(3000000, 4000000, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    object_version_store.batch_append([sym_1, sym_2], [df3, df3])
    vit = object_version_store.batch_read([sym_1, sym_2])
    expected_1 = pd.concat([df1, df2, df3])
    expected_2 = pd.concat([df1, df3])
    assert vit[sym_1].version == 2
    assert vit[sym_2].version == 2
    assert_frame_equal(vit[sym_1].data, expected_1)
    assert_frame_equal(vit[sym_2].data, expected_2)


@pytest.mark.storage
def test_delete_version_with_update(object_version_store, sym):
    symbol = sym
    idx1 = pd.date_range("2000-1-1", periods=5)
    d1 = {"x": np.arange(0, 5, dtype=np.float64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    object_version_store.write(symbol, df1)

    idx2 = pd.date_range("2000-1-6", periods=5)
    d2 = {"x": np.arange(5, 10, dtype=np.float64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    object_version_store.append(symbol, df2)

    object_version_store.delete_version(symbol, 1)

    idx3 = pd.date_range("2000-1-2", periods=2)
    d3 = {"x": np.arange(101, 103, dtype=np.float64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    object_version_store.update(symbol, df3)
    vit = object_version_store.read(symbol)
    expected = df1
    expected.update(df3)
    assert_frame_equal(vit.data, expected)
    assert vit.version == 2


@pytest.mark.parametrize("idx", [2, 3])
@pytest.mark.storage
def test_delete_version_with_write_metadata(object_version_store, idx, sym):
    symbol = sym
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    object_version_store.write(symbol, df1)
    metadata_1 = {"a": 1}
    object_version_store.write_metadata(symbol, metadata_1)
    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)
    assert vit.metadata == metadata_1
    assert vit.version == 1

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    object_version_store.append(symbol, df2)
    metadata_2 = {"b": 2}
    object_version_store.write_metadata(symbol, metadata_2)
    vit = object_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)
    assert vit.metadata == metadata_2
    assert vit.version == 3

    object_version_store.delete_version(symbol, idx)
    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, expected)
    if idx == 2:
        assert vit.metadata == metadata_2
        assert vit.version == 3
    else:
        assert vit.metadata is None
        assert vit.version == 2

    metadata_3 = {"c": 3}
    object_version_store.write_metadata(symbol, metadata_3)
    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, expected)
    assert vit.metadata == metadata_3
    assert vit.version == 4


@pytest.mark.parametrize("idx", [2, 3])
@pytest.mark.storage
def test_delete_version_with_batch_write_metadata(object_version_store, idx, sym):
    sym_1 = sym
    sym_2 = "another-{}".format(sym)
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    object_version_store.batch_write([sym_1, sym_2], [df1, df1])
    metadata_1 = {"a": 1}
    object_version_store.batch_write_metadata([sym_1, sym_2], [metadata_1, metadata_1])
    vit = object_version_store.batch_read([sym_1, sym_2])
    assert_frame_equal(vit[sym_1].data, df1)
    assert_frame_equal(vit[sym_2].data, df1)
    assert vit[sym_1].metadata == metadata_1
    assert vit[sym_2].metadata == metadata_1
    assert vit[sym_1].version == 1
    assert vit[sym_2].version == 1

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    object_version_store.batch_append([sym_1, sym_2], [df2, df2])
    metadata_2 = {"b": 2}
    object_version_store.batch_write_metadata([sym_1, sym_2], [metadata_2, metadata_2])
    expected = pd.concat([df1, df2])
    vit = object_version_store.batch_read([sym_1, sym_2])
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)
    assert vit[sym_1].metadata == metadata_2
    assert vit[sym_2].metadata == metadata_2
    assert vit[sym_1].version == 3
    assert vit[sym_2].version == 3

    object_version_store.delete_version(sym_2, idx)

    vit = object_version_store.read(sym_2)
    assert_frame_equal(vit.data, expected)
    if idx == 2:
        assert vit.metadata == metadata_2
        assert vit.version == 3
    else:
        assert vit.metadata is None
        assert vit.version == 2

    metadata_3 = {"c": 3}
    object_version_store.batch_write_metadata([sym_1, sym_2], [metadata_3, metadata_3])
    vit = object_version_store.batch_read([sym_1, sym_2])
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)
    assert vit[sym_1].metadata == metadata_3
    assert vit[sym_2].metadata == metadata_3
    assert vit[sym_1].version == 4
    assert vit[sym_2].version == 4


@pytest.mark.parametrize("map_timeout", MAP_TIMEOUTS)
@pytest.mark.storage
def test_delete_version_with_snapshot(map_timeout, object_and_mem_and_lmdb_version_store, sym):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        lib = object_and_mem_and_lmdb_version_store
        symbol = sym
        df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
        lib.write(symbol, df1)
        lib.snapshot("delete_version_snap_1")

        df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
        lib.write(symbol, df2)
        lib.snapshot("delete_version_snap_2")

        df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
        lib.write(symbol, df3)
        vit = lib.read(symbol)
        assert_frame_equal(vit.data, df3)

        assert len(lib.list_versions(symbol)) == 3

        lib.delete_version(symbol, 1)
        assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 2

        assert_frame_equal(lib.read(symbol, "delete_version_snap_2").data, df2)
        lib.read(symbol, 1)

        lib.delete_version(symbol, 0)
        assert len([ver for ver in lib.list_versions(symbol) if not ver["deleted"]]) == 1

        assert_frame_equal(lib.read(symbol, "delete_version_snap_1").data, df1)


@pytest.mark.parametrize("map_timeout", MAP_TIMEOUTS)
@pytest.mark.storage
def test_delete_versions_with_snapshot(map_timeout, object_and_mem_and_lmdb_version_store, sym):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        lib = object_and_mem_and_lmdb_version_store
        symbol = sym
        df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
        lib.write(symbol, df1)
        lib.snapshot("delete_version_snap_1")

        df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
        lib.write(symbol, df2)
        lib.snapshot("delete_version_snap_2")

        df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
        lib.write(symbol, df3)
        vit = lib.read(symbol)
        assert_frame_equal(vit.data, df3)

        assert len(lib.list_versions(symbol)) == 3

        lib.delete_versions(symbol, [0, 1])
        assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 1

        assert_frame_equal(lib.read(symbol, "delete_version_snap_2").data, df2)
        assert_frame_equal(lib.read(symbol, "delete_version_snap_1").data, df1)


@pytest.mark.storage
def test_delete_mixed(object_version_store, sym):
    symbol = sym
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    object_version_store.write(symbol, df1)
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    object_version_store.write(symbol, df2)
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
    object_version_store.write(symbol, df3)
    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, df3)
    object_version_store.delete(symbol)
    assert object_version_store.has_symbol(symbol) is False


@pytest.mark.storage
def tests_with_pruning_and_tombstones(basic_store_tombstone_and_pruning, sym):
    symbol = sym
    lib = basic_store_tombstone_and_pruning

    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    lib.write(symbol, df1)
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    lib.write(symbol, df2)
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
    lib.write(symbol, df3)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df3)
    lib.delete(symbol)
    assert lib.has_symbol(symbol) is False


@pytest.mark.parametrize("map_timeout", MAP_TIMEOUTS)
@pytest.mark.storage
def test_with_snapshot_pruning_tombstones(basic_store_tombstone_and_pruning, map_timeout, sym):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        symbol = sym
        lib = basic_store_tombstone_and_pruning

        df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
        lib.write(symbol, df1)
        lib.snapshot("delete_version_snap_1")

        df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
        lib.write(symbol, df2)
        lib.snapshot("delete_version_snap_2")

        df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
        lib.write(symbol, df3)
        vit = lib.read(symbol)
        assert_frame_equal(vit.data, df3)

        # pruning enabled
        assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 1

        assert_frame_equal(lib.read(symbol, "delete_version_snap_2").data, df2)
        # with pytest.raises(NoDataFoundException):
        # This won't raise anymore as it's in delete_version_snap_2
        lib.read(symbol, 1)

        assert_frame_equal(lib.read(symbol, "delete_version_snap_1").data, df1)
        # with pytest.raises(NoDataFoundException):
        # Won't raise as in snapshot
        lib.read(symbol, 0)


@pytest.mark.parametrize("map_timeout", MAP_TIMEOUTS)
@pytest.mark.storage
def test_normal_flow_with_snapshot_and_pruning(basic_store_tombstone_and_pruning, map_timeout, sym):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        symbol = sym
        lib = basic_store_tombstone_and_pruning

        lib_tool = basic_store_tombstone_and_pruning.library_tool()
        lib.write("sym1", 1)
        lib.write("sym2", 1)

        lib.snapshot("snap1")

        lib.write("sym1", 2)
        lib.write("sym1", 3)
        lib.delete("sym1")
        with pytest.raises(NoDataFoundException):
            lib.read(symbol, 0)
        assert lib.read("sym1", as_of="snap1").data == 1

        assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 1

        version_keys = lib_tool.find_keys(KeyType.VERSION)
        keys_for_a = [k for k in version_keys if k.id == "sym1"]
        keys_per_write = 2
        num_writes = 2
        assert len(keys_for_a) == keys_per_write * num_writes

        lib.write("sym1", 4)
        assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 2


@pytest.mark.storage
def test_deleting_tombstoned_versions(basic_store_tombstone_and_pruning, sym):
    lib = basic_store_tombstone_and_pruning
    lib.write(sym, 1)
    lib.write(sym, 1)
    lib.write(sym, 1)

    # Two versions are tombstoned at this point.


@pytest.mark.storage
def test_delete_multi_keys(object_version_store, sym):
    object_version_store.write(
        sym,
        data={"e": np.arange(1000), "f": np.arange(8000), "g": None},
        metadata="realyolo2",
        recursive_normalizers=True,
    )
    lt = object_version_store.library_tool()
    assert len(lt.find_keys(KeyType.MULTI_KEY)) != 0

    object_version_store.delete(sym)

    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 0
    assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 0
    assert len(lt.find_keys(KeyType.TABLE_DATA)) == 0


@pytest.mark.parametrize("map_timeout", MAP_TIMEOUTS)
@pytest.mark.storage
def test_delete_multi_keys_snapshot(basic_store, map_timeout, sym):
    data = {"e": np.arange(1000), "f": np.arange(8000), "g": None}
    basic_store.write(sym, data=data, metadata="realyolo2", recursive_normalizers=True)
    basic_store.snapshot("mysnap1")
    basic_store.delete(sym)

    with pytest.raises(Exception):
        basic_store.read(sym)

    comp_dict(data, basic_store.read(sym, as_of="mysnap1").data)

    basic_store.delete_snapshot("mysnap1")

    lt = basic_store.library_tool()
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 0
    assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 0
    assert len(lt.find_keys(KeyType.TABLE_DATA)) == 0


@pytest.mark.parametrize("index_start", range(10))
def test_delete_date_range_with_strings(version_store_factory, index_start):
    lib = version_store_factory(column_group_size=3, segment_row_size=3)

    symbol = "delete_daterange"
    periods = 100
    idx = pd.date_range("1970-01-01", periods=periods, freq="D")
    df = pd.DataFrame({"a": [random_string(10) for _ in range(len(idx))]}, index=idx)
    lib.write(symbol, df)

    start = random.randrange(index_start, periods - 2)
    end = random.randrange(start, periods - 1)

    start_time = idx[start]
    end_time = idx[end]

    range_to_delete = pd.date_range(start=start_time, end=end_time)
    lib.delete(symbol, date_range=range_to_delete)
    df = df.drop(df.index[start : end + 1])  # Arctic is end date inclusive

    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df)


@pytest.mark.parametrize("prune_previous_versions", [True, False])
def test_delete_date_range_with_prune_previous(lmdb_version_store, prune_previous_versions):
    lib = lmdb_version_store
    symbol = "delete_daterange"
    periods = 100
    idx = pd.date_range("1970-01-01", periods=periods, freq="D")
    df = pd.DataFrame({"a": [random_string(10) for _ in range(len(idx))]}, index=idx)
    lib.write(symbol, df)

    start = random.randrange(9, periods - 2)
    end = random.randrange(start, periods - 1)

    start_time = idx[start]
    end_time = idx[end]

    range_to_delete = pd.date_range(start=start_time, end=end_time)
    lib.delete(symbol, date_range=range_to_delete, prune_previous_versions=prune_previous_versions)
    old_df = df
    df = df.drop(df.index[start : end + 1])  # Arctic is end date inclusive

    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df)

    versions = [version["version"] for version in lib.list_versions(symbol)]
    if prune_previous_versions:
        assert len(versions) == 1 and versions[0] == 1
    else:
        assert len(versions) == 2
        assert_frame_equal(lib.read(symbol, as_of=0).data, old_df)


@pytest.mark.parametrize("map_timeout", MAP_TIMEOUTS)
def test_delete_date_range_remove_everything(version_store_factory, map_timeout):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        lib = version_store_factory(column_group_size=3, segment_row_size=3)

        symbol = "delete_daterange"
        periods = 100
        idx = pd.date_range("1970-01-01", periods=periods, freq="D")
        df = pd.DataFrame({"a": [random_string(10) for _ in range(len(idx))]}, index=idx)
        lib.write(symbol, df)

        start = 0
        end = 99

        start_time = idx[start]
        end_time = idx[end]

        range_to_delete = pd.date_range(start=start_time, end=end_time)
        lib.delete(symbol, date_range=range_to_delete)

        vit = lib.read(symbol)
        df = df.drop(df.index[start : end + 1])  # Arctic is end date inclusive
        assert len(df) == 0
        # Empty DataFrames in Pandas < 2 have different default dtype.
        df = df.astype(vit.data.dtypes)
        assert_frame_equal(vit.data, df)


def test_delete_date_range_get_info(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    sym = "test_delete_date_range_get_info"
    data = {
        "col_0": [0, 1, 2, 3, 4],
        "col_1": [5, 6, 7, 8, 9],
        "col_2": [10, 11, 12, 13, 14],
    }
    df = pd.DataFrame(data, index=pd.date_range(pd.Timestamp(1000), freq="us", periods=5))
    lib.write(sym, df)
    date_range = lib.get_info(sym)["date_range"]
    assert df.index[0] == date_range[0]
    assert df.index[-1] == date_range[1]

    lib.delete(sym, (pd.Timestamp(4000), pd.Timestamp(5000)))
    received = lib.read(sym).data
    assert_frame_equal(df.iloc[:3], received)
    assert received.index[-1] == lib.get_info(sym)["date_range"][1]

    lib.delete(sym, (pd.Timestamp(1000), pd.Timestamp(2000)))
    received = lib.read(sym).data
    assert_frame_equal(df.iloc[2:3], received)
    assert received.index[0] == lib.get_info(sym)["date_range"][0]


@pytest.mark.storage
def test_delete_read_from_timestamp(basic_store):
    sym = "test_from_timestamp_with_delete"
    lib = basic_store
    lib.write(sym, 1)
    lib.write(sym, 2)

    with distinct_timestamps(basic_store) as deletion_time:
        lib.delete_version(sym, 0)
    with distinct_timestamps(basic_store):
        lib.write(sym, 3)
    lib.write(sym, 4)
    assert lib.read(sym, as_of=deletion_time.after).data == 2
    assert lib.batch_read([sym], as_ofs=[deletion_time.after])[sym].data == 2
