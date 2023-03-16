"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import random
import numpy as np
import pytest
from arcticdb.config import Defaults
from arcticdb.util.test import sample_dataframe
from arcticdb.version_store._store import NativeVersionStore
from arcticdb_ext.storage import KeyType, OpenMode as _OpenMode


@pytest.fixture()
def symbol():
    return "sym" + str(random.randint(0, 10000))


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


def test_symbol_list_normal_flow(lmdb_version_store):
    lib = lmdb_version_store
    lib.write("a", 1)
    assert lib.list_symbols() == ["a"]
    lib.write("b", 1)
    lib.write("a", 1)
    lib_tool = lib.library_tool()
    assert len(lib_tool.find_keys(KeyType.VERSION)) == 3  # 2 versions for a, 1 for b
    symbol_list_keys = lib_tool.find_keys(KeyType.SYMBOL_LIST)
    add_keys = [key for key in symbol_list_keys if key.id == "__add__"]
    assert len(add_keys) == 2
    lib.version_store._clear_symbol_list_keys()
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 0

    print("refreshing symbol list")
    lib.version_store.reload_symbol_list()
    import time

    time.sleep(1)
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) > 0
    assert len(symbol_list_keys) > 0
    assert set(lib.list_symbols()) == {"a", "b"}

    lib.version_store._clear_symbol_list_keys()
    assert set(lib.list_symbols()) == {"a", "b"}


def test_interleaved_store_read(version_store_factory):
    vs1 = version_store_factory()
    vs2 = version_store_factory(reuse_name=True)

    vs1.write("a", 1)
    vs2.delete("a")

    assert vs1.list_symbols() == []


@pytest.mark.skip
@pytest.mark.parametrize("compact_first", [True, False])
def test_symbol_list_read_only_compaction_needed(s3_version_store, compact_first):
    lib_write = s3_version_store
    lib_read = NativeVersionStore.create_store_from_lib_config(lib_write.lib_cfg(), Defaults.ENV, _OpenMode.READ)
    if compact_first:
        # Do initial symbol list compaction from version keys
        lib_write.list_symbols()
    # Write symbols so that symbol list needs compaction
    num_symbols = 1000
    for idx in range(num_symbols):
        lib_write.write(f"sym-{idx}", idx)
    assert len(lib_read.list_symbols()) == num_symbols


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
    lib.version_store._clear_symbol_list_keys()
    lib.version_store.reload_symbol_list()
    assert lib.list_symbols() == []
    lt = lib.library_tool()
    assert len(lt.find_keys(KeyType.SYMBOL_LIST)) == 1
    assert lib.list_symbols() == []
