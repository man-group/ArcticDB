"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.config import Defaults
from arcticdb.util.test import sample_dataframe, random_ascii_strings
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.toolbox.library_tool import (
    VariantKey,
    AtomKey,
    key_to_props_dict,
    props_dict_to_atom_key,
)
from arcticdb_ext import set_config_int, unset_config_int
from arcticdb_ext.storage import KeyType, OpenMode
from arcticdb_ext.tools import CompactionId, CompactionLockName
from arcticdb_ext.exceptions import InternalException, PermissionException

from multiprocessing import Pool
from arcticdb_ext import set_config_int
from tests.util.mark import MACOS, REAL_AZURE_TESTS_MARK


@pytest.fixture
def small_max_delta():
    set_config_int("SymbolList.MaxDelta", 2)
    try:
        yield
    finally:
        unset_config_int("SymbolList.MaxDelta")


def make_read_only(lib):
    return NativeVersionStore.create_store_from_lib_config(
        lib_cfg=lib.lib_cfg(), env=Defaults.ENV, open_mode=OpenMode.READ, native_cfg=lib.lib_native_cfg()
    )


@pytest.mark.storage
def test_with_symbol_list(basic_store):
    syms = []
    df = sample_dataframe(100)
    for i in range(100):
        sym = "sym_{}".format(i)
        basic_store.write(sym, df)
        syms.append(sym)

    list_syms = basic_store.list_symbols()
    assert len(list_syms) == len(syms)

    for sym in syms:
        assert sym in list_syms

    for sym in list_syms:
        assert sym in syms

    for j in range(0, 100, 2):
        sym = "sym_{}".format(j)
        basic_store.delete(sym)

    expected_syms = []
    for k in range(1, 100, 2):
        sym = "sym_{}".format(k)
        expected_syms.append(sym)

    list_syms = basic_store.list_symbols()
    assert len(list_syms) == len(expected_syms)

    for sym in expected_syms:
        assert sym in list_syms

    for sym in list_syms:
        assert sym in expected_syms


@pytest.mark.storage
def test_symbol_list_with_rec_norm(basic_store):
    basic_store.write(
        "rec_norm",
        data={"a": np.arange(5), "b": np.arange(8), "c": None},
        recursive_normalizers=True,
    )

    assert not basic_store.is_symbol_pickled("rec_norm")
    assert basic_store.list_symbols() == ["rec_norm"]


@pytest.mark.storage
def test_interleaved_store_read(version_store_and_real_s3_basic_store_factory):
    basic_store_factory = version_store_and_real_s3_basic_store_factory
    vs1 = basic_store_factory()
    vs2 = basic_store_factory(reuse_name=True)

    vs1.write("a", 1)
    vs2.delete("a")

    assert vs1.list_symbols() == []


@pytest.mark.storage
def test_symbol_list_regex(basic_store):
    for i in range(15):
        basic_store.write(f"sym_{i}", pd.DataFrame())

    assert set(basic_store.list_symbols(regex="1$")) == {"sym_1", "sym_11"}
    assert set(basic_store.list_symbols(regex=".*1.*")) == {
        "sym_1",
        "sym_10",
        "sym_11",
        "sym_12",
        "sym_13",
        "sym_14",
    }


@pytest.mark.xfail(MACOS and REAL_AZURE_TESTS_MARK.args[0], reason="MacOS problem with Azure (9713365654)")
@pytest.mark.parametrize("compact_first", [True, False])
# Using S3 because LMDB does not allow OpenMode to be changed
@pytest.mark.storage
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


@pytest.mark.storage
def test_symbol_list_delete(basic_store):
    lib = basic_store
    lib.write("a", 1)
    assert lib.list_symbols() == ["a"]
    lib.write("b", 1)
    lib.delete("a")
    assert lib.list_symbols() == ["b"]


@pytest.mark.storage
def test_symbol_list_delete_incremental(basic_store):
    lib = basic_store
    lib.write("a", 1)
    lib.write("a", 2, prune_previous=False)
    lib.write("b", 1)
    lib.delete_version("a", 0)
    assert sorted(lib.list_symbols()) == ["a", "b"]
    lib.delete_version("a", 1)
    assert lib.list_symbols() == ["b"]


@pytest.mark.storage
def test_symbol_list_delete_multiple_versions(basic_store):
    lib = basic_store
    lib.write("a", 1)
    lib.write("a", 2, prune_previous=False)
    lib.write("b", 1)
    lib.delete_versions("a", [0, 1])
    assert lib.list_symbols() == ["b"]


@pytest.mark.storage
@pytest.mark.parametrize("versions_to_delete", [[0, 1], [1, 2]])
def test_symbol_list_delete_multiple_versions_symbol_alive(basic_store, versions_to_delete):
    lib = basic_store

    lib.write("a", 1)
    lib.write("a", 2, prune_previous=False)
    lib.write("a", 3, prune_previous=False)
    lib.write("b", 1)
    lib.delete_versions("a", versions_to_delete)
    assert sorted(lib.list_symbols()) == ["a", "b"]


@pytest.mark.storage
def test_deleted_symbol_with_tombstones(basic_store_tombstones_no_symbol_list):
    lib = basic_store_tombstones_no_symbol_list
    lib.write("a", 1)
    assert lib.list_symbols() == ["a"]
    lib.write("b", 1)
    lib.delete("a")
    assert lib.list_symbols() == ["b"]


@pytest.mark.storage
@pytest.mark.xfail(MACOS and REAL_AZURE_TESTS_MARK.args[0], reason="MacOS problem with Azure (9713365654)")
def test_empty_lib(basic_store):
    lib = basic_store
    assert lib.list_symbols() == []
    lt = lib.library_tool()
    assert len(lt.find_keys(KeyType.SYMBOL_LIST)) == 1


@pytest.mark.storage
@pytest.mark.xfail(MACOS and REAL_AZURE_TESTS_MARK.args[0], reason="MacOS problem with Azure (9713365654)")
def test_no_active_symbols(basic_store_prune_previous):
    lib = basic_store_prune_previous
    for idx in range(20):
        lib.write(str(idx), idx)
    for idx in range(20):
        lib.delete(str(idx))
    lib.version_store.reload_symbol_list()
    assert lib.list_symbols() == []
    lt = lib.library_tool()
    assert len(lt.find_keys(KeyType.SYMBOL_LIST)) == 1
    assert lib.list_symbols() == []


@pytest.mark.storage
@pytest.mark.xfail(MACOS and REAL_AZURE_TESTS_MARK.args[0], reason="MacOS problem with Azure (9713365654)")
def test_only_latest_compaction_key_is_used(basic_store):
    lib: NativeVersionStore = basic_store
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
    assert sorted(lib.list_symbols()) == ["a", "c"]


@pytest.mark.parametrize("write_another", [False, True])
@pytest.mark.storage
@pytest.mark.xfail(MACOS and REAL_AZURE_TESTS_MARK.args[0], reason="MacOS problem with Azure (9713365654)")
def test_turning_on_symbol_list_after_a_symbol_written(object_store_factory, write_another):
    # The if(!maybe_last_compaction) case
    lib: NativeVersionStore = object_store_factory(symbol_list=False)

    lib.write("a", 1)
    assert not lib.library_tool().find_keys(KeyType.SYMBOL_LIST)

    lib = object_store_factory(reuse_name=True, symbol_list=True)
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
@pytest.mark.storage
def test_lock_contention(small_max_delta, basic_store, mode):
    lib = basic_store
    lt = lib.library_tool()
    lock = lib.version_store.get_storage_lock(CompactionLockName)

    lib.list_symbols()
    lib.write("a", 1)
    lib.write("b", 2)
    orig_sl = lt.find_keys(KeyType.SYMBOL_LIST)
    assert len(orig_sl) >= 2  # > small_max_delta

    if mode == "conflict":
        lock.lock()

    assert set(lib.list_symbols()) == {"a", "b"}

    if mode == "conflict":
        # Should not have attempted to compact without lock:
        assert lt.find_keys(KeyType.SYMBOL_LIST) == orig_sl
    else:
        assert lt.find_keys(KeyType.SYMBOL_LIST) != orig_sl


def _tiny_df(idx):
    return pd.DataFrame(
        {"x": np.arange(idx % 10, idx % 10 + 10)},
        index=pd.date_range(idx % 10, periods=10, freq="h"),
    )


def _trigger(count, frequency):
    return count > frequency and count % frequency == 0


def _perform_actions(args):
    lib, symbols, idx, num_cycles, list_freq, delete_freq, update_freq = args
    for _ in range(num_cycles):
        for c, sym in enumerate(symbols):
            count = c + idx
            if _trigger(count, delete_freq):
                lib.delete(sym)
            elif _trigger(count, list_freq):
                lib.list_symbols()
            elif _trigger(count, update_freq):
                lib.update(sym, _tiny_df(idx + c), upsert=True)
            else:
                lib.write(sym, _tiny_df(idx + c))


class ScopedMaxDelta:
    def __init__(self, max_delta):
        set_config_int("SymbolList.MaxDelta", max_delta)

    def __del__(self):
        unset_config_int("SymbolList.MaxDelta")


@pytest.mark.parametrize("list_freq", [2, 100])
@pytest.mark.parametrize("delete_freq", [2, 10])
@pytest.mark.parametrize("update_freq", [3, 8])
@pytest.mark.parametrize("compaction_size", [2, 10, 200])
@pytest.mark.parametrize("same_symbols", [True, False])
@pytest.mark.xfail(reason="Needs to be fixed with issue #496")
def test_symbol_list_parallel_stress_with_delete(
    lmdb_version_store_v1,
    list_freq,
    delete_freq,
    update_freq,
    compaction_size,
    same_symbols,
):
    pd.set_option("display.max_rows", None)
    ScopedMaxDelta(compaction_size)

    lib = lmdb_version_store_v1
    num_pre_existing_symbols = 100
    num_symbols = 10
    num_workers = 5
    num_cycles = 1
    symbol_length = 6

    pre_existing_symbols = random_ascii_strings(num_pre_existing_symbols, symbol_length)
    for idx, existing in enumerate(pre_existing_symbols):
        lib.write(existing, _tiny_df(idx))

    if same_symbols:
        frozen_symbols = random_ascii_strings(num_symbols, symbol_length)
        symbols = [frozen_symbols for _ in range(num_workers)]
    else:
        symbols = [random_ascii_strings(num_symbols, symbol_length) for _ in range(num_workers)]

    with Pool(num_workers) as p:
        p.map(
            _perform_actions,
            [(lib, syms, idx, num_cycles, list_freq, delete_freq, update_freq) for idx, syms in enumerate(symbols)],
        )

    p.close()
    p.join()

    expected_symbols = set(lib.list_symbols(use_symbol_list=False))
    got_symbols = set(lib.list_symbols())
    extra_symbols = got_symbols - expected_symbols
    assert len(extra_symbols) == 0
    missing_symbols = expected_symbols - got_symbols
    for sym in missing_symbols:
        assert not lib.version_store.indexes_sorted(sym)


def test_force_compact_symbol_list(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    lib_tool = lib.library_tool()
    # No symbol list keys
    assert lib.compact_symbol_list() == 0
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == 1
    assert not len(lib.list_symbols())
    lib_tool.remove(symbol_list_keys[0])

    num_syms = 1000
    syms = [f"sym_{idx:03}" for idx in range(num_syms)]
    data = [1 for _ in range(num_syms)]
    lib.batch_write(syms, data)
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == num_syms
    assert lib.compact_symbol_list() == num_syms
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == 1
    assert set(lib.list_symbols()) == set(syms)
    # Idempotent
    assert lib.compact_symbol_list() == 1
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == 1
    assert set(lib.list_symbols()) == set(syms)
    # Everything deleted
    for sym in syms:
        lib.delete(sym)
    # +1 for previous compacted key
    assert lib.compact_symbol_list() == num_syms + 1
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    assert len(symbol_list_keys) == 1
    assert not len(lib.list_symbols())


# Using S3 because LMDB does not allow OpenMode to be changed
def test_force_compact_symbol_list_read_only(s3_version_store_v1):
    lib_write = s3_version_store_v1
    lib_read_only = make_read_only(lib_write)
    # No symbol list keys
    with pytest.raises(PermissionException):
        lib_read_only.compact_symbol_list()
    # Some symbol list keys
    lib_write.write("sym1", 1)
    lib_write.write("sym2", 1)
    with pytest.raises(PermissionException):
        lib_read_only.compact_symbol_list()
    # One compacted symbol list key
    assert lib_write.compact_symbol_list() == 2
    with pytest.raises(PermissionException):
        lib_read_only.compact_symbol_list()


def test_force_compact_symbol_list_lock_held(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    lock = lib.version_store.get_storage_lock(CompactionLockName)
    lock.lock()
    with pytest.raises(InternalException):
        lib.compact_symbol_list()
    lock.unlock()
    assert lib.compact_symbol_list() == 0


@pytest.fixture()
def make_lock_ttl_less():
    set_config_int("StorageLock.TTL", 5_000_000_000)
    try:
        yield
    finally:
        unset_config_int("StorageLock.TTL")


@pytest.mark.skipif(MACOS, reason="Failing on macOS for unclear reasons")
def test_force_compact_symbol_list_lock_held_past_ttl(lmdb_version_store_v1, make_lock_ttl_less):
    # Set TTL to 5 seconds. Compact symbol list will retry for 10 seconds, so should always work
    lib = lmdb_version_store_v1
    lock = lib.version_store.get_storage_lock(CompactionLockName)
    lock.lock()
    assert lib.compact_symbol_list() == 0
