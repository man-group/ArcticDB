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
from itertools import chain, product
from datetime import datetime
from tests.conftest import PERSISTENT_STORAGE_TESTS_ENABLED

from arcticdb.config import MACOS_CONDA_BUILD
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb.util.test import config_context, random_string, assert_frame_equal, distinct_timestamps

from tests.conftest import PERSISTENT_STORAGE_TESTS_ENABLED


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


def get_map_timeouts():
    return [0, SECOND * 5, SECOND * 10000]


def gen_params_store_and_timeout():
    p = [
        [
            "s3_version_store_v1",
            "s3_version_store_v2",
            "s3_version_store_v1",
            "s3_version_store_v2",
        ],
        get_map_timeouts(),
    ]
    if not MACOS_CONDA_BUILD:
        p[0].append("azure_version_store")

    if PERSISTENT_STORAGE_TESTS_ENABLED:
        p[0].append("real_s3_version_store")
    return list(product(*p))


@pytest.mark.parametrize("lib_type, map_timeout", gen_params_store_and_timeout())
def test_delete_all(lib_type, map_timeout, sym, request):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        lib = request.getfixturevalue(lib_type)
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


@pytest.mark.parametrize("idx", [0, 1])
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


@pytest.mark.parametrize("lib_type, map_timeout", gen_params_store_and_timeout())
def test_delete_version_with_snapshot(lib_type, map_timeout, sym, request):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        lib = request.getfixturevalue(lib_type)
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


@pytest.mark.parametrize("map_timeout", get_map_timeouts())
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


@pytest.mark.parametrize("map_timeout", get_map_timeouts())
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
        assert len(keys_for_a) == 3 * 2  # 2 keys for the number of writes

        lib.write("sym1", 4)
        assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 2


def test_deleting_tombstoned_versions(basic_store_tombstone_and_pruning, sym):
    lib = basic_store_tombstone_and_pruning
    lib.write(sym, 1)
    lib.write(sym, 1)
    lib.write(sym, 1)

    # Two versions are tombstoned at this point.


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


@pytest.mark.parametrize("map_timeout", get_map_timeouts())
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


@pytest.mark.parametrize("map_timeout", get_map_timeouts())
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
        df = df.drop(df.index[start : end + 1])  # Arctic is end date inclusive

        vit = lib.read(symbol)
        assert_frame_equal(vit.data, df)


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
