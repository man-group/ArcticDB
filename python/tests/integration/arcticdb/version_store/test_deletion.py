"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""
from __future__ import print_function

import sys

import numpy as np
import pandas as pd
import pytest
from arcticdb.config import Defaults
from arcticdb.version_store.helper import ArcticMemoryConfig
from arcticdb_ext.storage import NoDataFoundException
from pandas.testing import assert_frame_equal
import pandas.util.testing as tm
import random
from itertools import chain, product
from arcticdb.util.test import config_context
from typing import TYPE_CHECKING


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
    p = [["s3_version_store", "s3_version_store"], get_map_timeouts()]
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


def test_version_missing(s3_version_store):
    with pytest.raises(NoDataFoundException):
        s3_version_store.read("not_there")


@pytest.mark.parametrize("idx", [0, 1, 2])
def test_delete_version_basic(s3_version_store, idx, sym):
    symbol = sym
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    s3_version_store.write(symbol, df1)
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    s3_version_store.write(symbol, df2)
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
    s3_version_store.write(symbol, df3)
    vit = s3_version_store.read(symbol)
    assert_frame_equal(vit.data, df3)

    dfs = [df1, df2, df3]

    assert len(s3_version_store.list_versions(symbol)) == 3

    s3_version_store.delete_version(symbol, idx)

    with pytest.raises(NoDataFoundException):
        s3_version_store.read(symbol, idx)
    assert len(s3_version_store.list_versions(symbol)) == 2
    if idx != 2:
        assert_frame_equal(s3_version_store.read(symbol).data, df3)
    else:
        assert_frame_equal(s3_version_store.read(symbol).data, df2)

    assert_frame_equal(s3_version_store.read(symbol, (idx - 1) % 3).data, dfs[(idx - 1) % 3])
    assert_frame_equal(s3_version_store.read(symbol, (idx - 2) % 3).data, dfs[(idx - 2) % 3])

    s3_version_store.delete_version(symbol, (idx + 1) % 3)
    with pytest.raises(NoDataFoundException):
        s3_version_store.read(symbol, (idx + 1) % 3)
    with pytest.raises(NoDataFoundException):
        s3_version_store.read(symbol, idx)
    assert len(s3_version_store.list_versions(symbol)) == 1
    if idx == 2:
        assert_frame_equal(s3_version_store.read(symbol).data, df2)
    elif idx == 1:
        assert_frame_equal(s3_version_store.read(symbol).data, df1)
    else:
        assert_frame_equal(s3_version_store.read(symbol).data, df3)
    assert_frame_equal(s3_version_store.read(symbol, (idx + 2) % 3).data, dfs[(idx + 2) % 3])

    s3_version_store.delete_version(symbol, (idx + 2) % 3)
    with pytest.raises(NoDataFoundException):
        s3_version_store.read(symbol, (idx + 2) % 3)
    with pytest.raises(NoDataFoundException):
        s3_version_store.read(symbol, (idx + 1) % 3)
    with pytest.raises(NoDataFoundException):
        s3_version_store.read(symbol, idx)
    assert len(s3_version_store.list_versions(symbol)) == 0


def test_delete_version_with_batch_write(s3_version_store, sym):
    sym_1 = sym
    sym_2 = "another-{}".format(sym)
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    s3_version_store.batch_write([sym_1, sym_2], [df1, df1])

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    s3_version_store.batch_write([sym_1, sym_2], [df2, df2])
    vit = s3_version_store.batch_read([sym_1, sym_2])
    expected = df2
    assert vit[sym_1].version == 1
    assert vit[sym_2].version == 1
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)

    s3_version_store.delete_version(sym_2, 1)

    assert_frame_equal(s3_version_store.read(sym_2).data, df1)
    assert_frame_equal(s3_version_store.read(sym_2, 0).data, df1)
    idx3 = np.arange(2000000, 3000000)
    d3 = {"x": np.arange(3000000, 4000000, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    s3_version_store.batch_write([sym_1, sym_2], [df3, df3])
    vit = s3_version_store.batch_read([sym_1, sym_2])
    expected = df3
    assert vit[sym_1].version == 2
    assert vit[sym_2].version == 2
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)


@pytest.mark.parametrize("idx", [0, 1])
def test_delete_version_with_append(s3_version_store, idx, sym):
    symbol = sym
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    s3_version_store.write(symbol, df1)
    vit = s3_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    s3_version_store.append(symbol, df2)
    vit = s3_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)

    s3_version_store.delete_version(symbol, idx)
    assert len(s3_version_store.list_versions(symbol)) == 1

    if idx == 0:
        assert_frame_equal(s3_version_store.read(symbol).data, expected)
        assert_frame_equal(s3_version_store.read(symbol, 1).data, expected)
    else:
        assert_frame_equal(s3_version_store.read(symbol).data, df1)
        assert_frame_equal(s3_version_store.read(symbol, 0).data, df1)
        idx3 = np.arange(2000000, 3000000)
        d3 = {"x": np.arange(3000000, 4000000, dtype=np.int64)}
        df3 = pd.DataFrame(data=d3, index=idx3)
        s3_version_store.append(symbol, df3)
        vit = s3_version_store.read(symbol)
        expected = pd.concat([df1, df3])
        assert_frame_equal(vit.data, expected)
        assert vit.version == 2


def test_delete_version_with_batch_append(s3_version_store, sym):
    sym_1 = sym
    sym_2 = "another-{}".format(sym)
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    s3_version_store.batch_write([sym_1, sym_2], [df1, df1])

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    s3_version_store.batch_append([sym_1, sym_2], [df2, df2])
    vit = s3_version_store.batch_read([sym_1, sym_2])
    expected = pd.concat([df1, df2])
    assert vit[sym_1].version == 1
    assert vit[sym_2].version == 1
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)

    s3_version_store.delete_version(sym_2, 1)

    assert_frame_equal(s3_version_store.read(sym_2).data, df1)
    assert_frame_equal(s3_version_store.read(sym_2, 0).data, df1)
    idx3 = np.arange(2000000, 3000000)
    d3 = {"x": np.arange(3000000, 4000000, dtype=np.int64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    s3_version_store.batch_append([sym_1, sym_2], [df3, df3])
    vit = s3_version_store.batch_read([sym_1, sym_2])
    expected_1 = pd.concat([df1, df2, df3])
    expected_2 = pd.concat([df1, df3])
    assert vit[sym_1].version == 2
    assert vit[sym_2].version == 2
    assert_frame_equal(vit[sym_1].data, expected_1)
    assert_frame_equal(vit[sym_2].data, expected_2)


def test_delete_version_with_update(s3_version_store, sym):
    symbol = sym
    idx1 = pd.date_range("2000-1-1", periods=5)
    d1 = {"x": np.arange(0, 5, dtype=np.float64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    s3_version_store.write(symbol, df1)

    idx2 = pd.date_range("2000-1-6", periods=5)
    d2 = {"x": np.arange(5, 10, dtype=np.float64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    s3_version_store.append(symbol, df2)

    s3_version_store.delete_version(symbol, 1)

    idx3 = pd.date_range("2000-1-2", periods=2)
    d3 = {"x": np.arange(101, 103, dtype=np.float64)}
    df3 = pd.DataFrame(data=d3, index=idx3)
    s3_version_store.update(symbol, df3)
    vit = s3_version_store.read(symbol)
    expected = df1
    expected.update(df3)
    assert_frame_equal(vit.data, expected)
    assert vit.version == 2


@pytest.mark.parametrize("idx", [2, 3])
def test_delete_version_with_write_metadata(s3_version_store, idx, sym):
    symbol = sym
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    s3_version_store.write(symbol, df1)
    metadata_1 = {"a": 1}
    s3_version_store.write_metadata(symbol, metadata_1)
    vit = s3_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)
    assert vit.metadata == metadata_1
    assert vit.version == 1

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    s3_version_store.append(symbol, df2)
    metadata_2 = {"b": 2}
    s3_version_store.write_metadata(symbol, metadata_2)
    vit = s3_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)
    assert vit.metadata == metadata_2
    assert vit.version == 3

    s3_version_store.delete_version(symbol, idx)
    vit = s3_version_store.read(symbol)
    assert_frame_equal(vit.data, expected)
    if idx == 2:
        assert vit.metadata == metadata_2
        assert vit.version == 3
    else:
        assert vit.metadata is None
        assert vit.version == 2

    metadata_3 = {"c": 3}
    s3_version_store.write_metadata(symbol, metadata_3)
    vit = s3_version_store.read(symbol)
    assert_frame_equal(vit.data, expected)
    assert vit.metadata == metadata_3
    assert vit.version == 4


@pytest.mark.parametrize("idx", [2, 3])
def test_delete_version_with_batch_write_metadata(s3_version_store, idx, sym):
    sym_1 = sym
    sym_2 = "another-{}".format(sym)
    idx1 = np.arange(0, 1000000)
    d1 = {"x": np.arange(1000000, 2000000, dtype=np.int64)}
    df1 = pd.DataFrame(data=d1, index=idx1)
    s3_version_store.batch_write([sym_1, sym_2], [df1, df1])
    metadata_1 = {"a": 1}
    s3_version_store.batch_write_metadata([sym_1, sym_2], [metadata_1, metadata_1])
    vit = s3_version_store.batch_read([sym_1, sym_2])
    assert_frame_equal(vit[sym_1].data, df1)
    assert_frame_equal(vit[sym_2].data, df1)
    assert vit[sym_1].metadata == metadata_1
    assert vit[sym_2].metadata == metadata_1
    assert vit[sym_1].version == 1
    assert vit[sym_2].version == 1

    idx2 = np.arange(1000000, 2000000)
    d2 = {"x": np.arange(2000000, 3000000, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)
    s3_version_store.batch_append([sym_1, sym_2], [df2, df2])
    metadata_2 = {"b": 2}
    s3_version_store.batch_write_metadata([sym_1, sym_2], [metadata_2, metadata_2])
    expected = pd.concat([df1, df2])
    vit = s3_version_store.batch_read([sym_1, sym_2])
    assert_frame_equal(vit[sym_1].data, expected)
    assert_frame_equal(vit[sym_2].data, expected)
    assert vit[sym_1].metadata == metadata_2
    assert vit[sym_2].metadata == metadata_2
    assert vit[sym_1].version == 3
    assert vit[sym_2].version == 3

    s3_version_store.delete_version(sym_2, idx)

    vit = s3_version_store.read(sym_2)
    assert_frame_equal(vit.data, expected)
    if idx == 2:
        assert vit.metadata == metadata_2
        assert vit.version == 3
    else:
        assert vit.metadata is None
        assert vit.version == 2

    metadata_3 = {"c": 3}
    s3_version_store.batch_write_metadata([sym_1, sym_2], [metadata_3, metadata_3])
    vit = s3_version_store.batch_read([sym_1, sym_2])
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


def test_delete_mixed(s3_version_store, sym):
    symbol = sym
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    s3_version_store.write(symbol, df1)
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    s3_version_store.write(symbol, df2)
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
    s3_version_store.write(symbol, df3)
    vit = s3_version_store.read(symbol)
    assert_frame_equal(vit.data, df3)
    s3_version_store.delete(symbol)
    assert s3_version_store.has_symbol(symbol) is False


def tests_with_pruning_and_tombstones(lmdb_version_store_tombstone_and_pruning, sym):
    symbol = sym
    lib = lmdb_version_store_tombstone_and_pruning

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
def test_with_snapshot_pruning_tombstones(lmdb_version_store_tombstone_and_pruning, map_timeout, sym):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        symbol = sym
        lib = lmdb_version_store_tombstone_and_pruning

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


def test_deleting_tombstoned_versions(lmdb_version_store_tombstone_and_pruning, sym):
    lib = lmdb_version_store_tombstone_and_pruning
    lib.write(sym, 1)
    lib.write(sym, 1)
    lib.write(sym, 1)

    # Two versions are tombstoned at this point.


@pytest.mark.parametrize("index_start", range(10))
def test_delete_date_range_with_strings(arcticdb_test_lmdb_config, index_start, lib_name):
    local_lib_cfg = arcticdb_test_lmdb_config(lib_name)
    lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
    lib.version.write_options.column_group_size = 3
    lib.version.write_options.segment_row_size = 3
    lmdb_version_store = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

    symbol = "delete_daterange"
    periods = 100
    idx = pd.date_range("1970-01-01", periods=periods, freq="D")
    df = pd.DataFrame({"a": [tm.rands(10) for _ in range(len(idx))]}, index=idx)
    lmdb_version_store.write(symbol, df)

    start = random.randrange(index_start, periods - 2)
    end = random.randrange(start, periods - 1)

    start_time = idx[start]
    end_time = idx[end]

    range_to_delete = pd.date_range(start=start_time, end=end_time)
    lmdb_version_store.delete(symbol, date_range=range_to_delete)
    df = df.drop(df.index[start : end + 1])  # Arctic is end date inclusive

    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df)


@pytest.mark.parametrize("map_timeout", get_map_timeouts())
def test_delete_date_range_remove_everything(arcticdb_test_lmdb_config, map_timeout, lib_name):
    with config_context("VersionMap.ReloadInterval", map_timeout):
        local_lib_cfg = arcticdb_test_lmdb_config(lib_name)
        lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
        lib.version.write_options.column_group_size = 3
        lib.version.write_options.segment_row_size = 3
        lmdb_version_store = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

        symbol = "delete_daterange"
        periods = 100
        idx = pd.date_range("1970-01-01", periods=periods, freq="D")
        df = pd.DataFrame({"a": [tm.rands(10) for _ in range(len(idx))]}, index=idx)
        lmdb_version_store.write(symbol, df)

        start = 0
        end = 99

        start_time = idx[start]
        end_time = idx[end]

        range_to_delete = pd.date_range(start=start_time, end=end_time)
        lmdb_version_store.delete(symbol, date_range=range_to_delete)
        df = df.drop(df.index[start : end + 1])  # Arctic is end date inclusive

        vit = lmdb_version_store.read(symbol)
        assert_frame_equal(vit.data, df)
