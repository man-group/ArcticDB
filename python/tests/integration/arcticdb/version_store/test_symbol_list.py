"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pytest
import requests

from arcticdb.config import Defaults
from arcticdb.util.test import sample_dataframe
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.toolbox.library_tool import VariantKey, AtomKey, key_to_props_dict, props_dict_to_atom_key
from arcticdb.version_store.helper import get_s3_uri_from_endpoint
from arcticdb.arctic import Arctic
from arcticdb_ext import set_config_int, unset_config_int
from arcticdb_ext.storage import KeyType, OpenMode
from arcticdb_ext.tools import CompactionId, CompactionLockName
from arcticdb_ext.exceptions import InternalException


@pytest.fixture(autouse=True)
def make_lock_wait_less():
    set_config_int("StorageLock.WaitMs", 1)
    try:
        yield
    finally:
        unset_config_int("StorageLock.WaitMs")


@pytest.fixture
def small_max_delta():
    set_config_int("SymbolList.MaxDelta", 2)
    try:
        yield
    finally:
        unset_config_int("SymbolList.MaxDelta")


def make_read_only(lib):
    return NativeVersionStore.create_store_from_lib_config(lib.lib_cfg(), Defaults.ENV, OpenMode.READ)


def test_with_symbol_list(lmdb_version_store):
    syms = []
    for i in range(100):
        df = sample_dataframe(100, i)
        sym = "sym_{}".format(i)
        lmdb_version_store.write(sym, df)
        syms.append(sym)

    list_syms = lmdb_version_store.list_symbols()
    assert len(list_syms) == len(syms)

    for sym in syms:
        assert sym in list_syms

    for sym in list_syms:
        assert sym in syms

    for j in range(0, 100, 2):
        sym = "sym_{}".format(j)
        lmdb_version_store.delete(sym)

    expected_syms = []
    for k in range(1, 100, 2):
        sym = "sym_{}".format(k)
        expected_syms.append(sym)

    list_syms = lmdb_version_store.list_symbols()
    assert len(list_syms) == len(expected_syms)

    for sym in expected_syms:
        assert sym in list_syms

    for sym in list_syms:
        assert sym in expected_syms


def test_symbol_list_with_rec_norm(lmdb_version_store):
    lmdb_version_store.write(
        "rec_norm", data={"a": np.arange(5), "b": np.arange(8), "c": None}, recursive_normalizers=True
    )
    assert not lmdb_version_store.is_symbol_pickled("rec_norm")
    assert lmdb_version_store.list_symbols() == ["rec_norm"]


def test_interleaved_store_read(version_store_factory):
    vs1 = version_store_factory()
    vs2 = version_store_factory(reuse_name=True)

    vs1.write("a", 1)
    vs2.delete("a")

    assert vs1.list_symbols() == []


@pytest.mark.parametrize("compact_first", [True, False])
# Using S3 because LMDB does not allow OpenMode to be changed
def test_symbol_list_read_only_compaction_needed(small_max_delta, object_version_store, compact_first):
    lib_write = object_version_store
    lib_read = make_read_only(lib_write)
    lt = lib_write.library_tool()
    old_compaction = []
    if compact_first:
        # Do initial symbol list compaction from version keys
        lib_write.list_symbols()
        old_compaction = lt.find_keys_for_id(KeyType.SYMBOL_LIST, CompactionId)
    # Write symbols so that symbol list needs compaction
    num_symbols = 10
    for idx in range(num_symbols):
        lib_write.write(f"sym-{idx}", idx)
    assert len(lib_read.list_symbols()) == num_symbols
    # Verify our setup/assumption is valid:
    assert lt.find_keys_for_id(KeyType.SYMBOL_LIST, CompactionId) == old_compaction
    lib_write.list_symbols()
    new_compaction = lt.find_keys_for_id(KeyType.SYMBOL_LIST, CompactionId)
    assert new_compaction != old_compaction


def test_symbol_list_delete(lmdb_version_store):
    lib = lmdb_version_store
    lib.write("a", 1)
    assert lib.list_symbols() == ["a"]
    lib.write("b", 1)
    lib.delete("a")
    assert lib.list_symbols() == ["b"]


def test_symbol_list_delete_incremental(lmdb_version_store):
    lib = lmdb_version_store
    lib.write("a", 1)
    lib.write("a", 2, prune_previous=False)
    lib.write("b", 1)
    lib.delete_version("a", 0)
    assert sorted(lib.list_symbols()) == ["a", "b"]
    lib.delete_version("a", 1)
    assert lib.list_symbols() == ["b"]


def test_deleted_symbol_with_tombstones(lmdb_version_store_tombstones_no_symbol_list):
    lib = lmdb_version_store_tombstones_no_symbol_list
    lib.write("a", 1)
    assert lib.list_symbols() == ["a"]
    lib.write("b", 1)
    lib.delete("a")
    assert lib.list_symbols() == ["b"]


def test_empty_lib(lmdb_version_store):
    lib = lmdb_version_store
    assert lib.list_symbols() == []
    lt = lib.library_tool()
    assert len(lt.find_keys(KeyType.SYMBOL_LIST)) == 1


def test_no_active_symbols(lmdb_version_store_prune_previous):
    lib = lmdb_version_store_prune_previous
    for idx in range(20):
        lib.write(str(idx), idx)
    for idx in range(20):
        lib.delete(str(idx))
    lib.version_store.reload_symbol_list()
    assert lib.list_symbols() == []
    lt = lib.library_tool()
    assert len(lt.find_keys(KeyType.SYMBOL_LIST)) == 1
    assert lib.list_symbols() == []


def test_only_latest_compaction_key_is_used(lmdb_version_store):
    lib: NativeVersionStore = lmdb_version_store
    lt = lib.library_tool()

    # Preserve an old compacted segment
    lib.write("a", 1)
    lib.list_symbols()
    key: VariantKey = lt.find_keys(KeyType.SYMBOL_LIST)[0]
    old_segment = lt.read_to_segment(key)

    # Replacement storage content
    lib.version_store.clear()
    lib.write("b", 2)
    lib.list_symbols()
    lib.write("c", 3)

    # Write old compacted segment back with much newer timestamp
    new_key_dict = key_to_props_dict(key)
    new_key_dict["creation_ts"] = key.creation_ts + int(100e9)
    lt.write(props_dict_to_atom_key(new_key_dict), old_segment)
    keys = lt.find_keys(KeyType.SYMBOL_LIST)
    assert len(keys) == 3

    # Should return content based on the latest timestamp
    assert lib.list_symbols() == ["a"]


@pytest.mark.parametrize("write_another", [False, True])
def test_turning_on_symbol_list_after_a_symbol_written(s3_store_factory, write_another):
    # The if(!maybe_last_compaction) case
    lib: NativeVersionStore = s3_store_factory(symbol_list=False)

    lib.write("a", 1)
    assert not lib.library_tool().find_keys(KeyType.SYMBOL_LIST)

    lib = s3_store_factory(reuse_name=True, symbol_list=True)
    lt = lib.library_tool()
    if write_another:
        lib.write("b", 2)

        sl_keys = lt.find_keys(KeyType.SYMBOL_LIST)
        assert sl_keys
        assert not any(k.id == CompactionId for k in sl_keys), "Should not have any compaction yet"

    ro = make_read_only(lib)
    # For some reason, symbol_list=True is not always picked up on the first call, so forcing it:
    symbols = ro.list_symbols(use_symbol_list=True)
    assert set(symbols) == ({"a", "b"} if write_another else {"a"})
    assert not any(k.id == CompactionId for k in lt.find_keys(KeyType.SYMBOL_LIST))

    symbols = lib.list_symbols(use_symbol_list=True)
    assert set(symbols) == ({"a", "b"} if write_another else {"a"})
    sl_keys = lt.find_keys(KeyType.SYMBOL_LIST)
    assert len(sl_keys) == 1
    assert sl_keys[0].id == CompactionId


@pytest.mark.parametrize("mode", ["conflict", "normal"])
def test_lock_contention(small_max_delta, lmdb_version_store, mode):
    lib = lmdb_version_store
    lt = lib.library_tool()
    lock = lib.version_store.get_storage_lock(CompactionLockName)

    lib.list_symbols()
    lib.write("a", 1)
    lib.write("b", 2)
    orig_sl = lt.find_keys(KeyType.SYMBOL_LIST)
    assert len(orig_sl) > 2  # > small_max_delta

    if mode == "conflict":
        lock.lock()

    assert set(lib.list_symbols()) == {"a", "b"}

    if mode == "conflict":
        # Should not have attempted to compact without lock:
        assert lt.find_keys(KeyType.SYMBOL_LIST) == orig_sl
    else:
        assert lt.find_keys(KeyType.SYMBOL_LIST) != orig_sl


def test_symbol_list_exception_and_printout(moto_s3_endpoint_and_credentials):
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials
    uri = get_s3_uri_from_endpoint(endpoint, bucket, aws_access_key, aws_secret_key, port)
    ac = Arctic(uri)
    assert ac.list_libraries() == []
    lib_name = "pytest_test_lib"
    lib = ac.create_library(lib_name)
    requests.post(endpoint + "/rate_limit", b"0").raise_for_status()
    try:
        with pytest.raises(InternalException, match="E_S3_RETRYABLE Retry-able error"):
            lib.list_symbols()
    finally:
        requests.post(endpoint + "/rate_limit", b"-1").raise_for_status()
        ac.delete_library(lib_name)