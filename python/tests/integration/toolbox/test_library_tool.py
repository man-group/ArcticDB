"""
Copyright 2023 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
import numpy as np
import pytest

from arcticdb.util.test import sample_dataframe, populate_db, assert_frame_equal
from arcticdb_ext.storage import KeyType


def get_ref_key_types():
    return [
        KeyType.VERSION_REF,
        KeyType.SNAPSHOT_REF,
        KeyType.APPEND_REF,
        KeyType.STORAGE_INFO,
        KeyType.LOCK,
        KeyType.OFFSET,
        KeyType.BACKUP_SNAPSHOT_REF,
    ]


def get_log_types():
    return [KeyType.LOG, KeyType.LOG_COMPACTED]


def test_get_types(object_and_mem_and_lmdb_version_store):
    df = sample_dataframe()
    lib = object_and_mem_and_lmdb_version_store
    lib.write("symbol1", df)
    lib_tool = lib.library_tool()
    version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "symbol1")
    assert len(version_keys) == 1
    key = version_keys[0]
    assert key.id == "symbol1"
    version_segment = lib_tool.read_to_segment(key)
    assert version_segment.fields_size() == 8
    index_keys = lib_tool.read_to_keys(key)
    assert len(index_keys) == 1
    index_df = lib_tool.read_to_dataframe(index_keys[0])
    assert index_df.at[pd.Timestamp(0), "version_id"] == 0


def test_read_keys(object_and_mem_and_lmdb_version_store):
    populate_db(object_and_mem_and_lmdb_version_store)
    lib_tool = object_and_mem_and_lmdb_version_store.library_tool()
    all_key_types = lib_tool.key_types()
    all_keys = []
    for key_type in all_key_types:
        all_keys = all_keys + lib_tool.find_keys(key_type)
    for key in all_keys:
        lib_tool.read_to_segment(key)
        lib_tool.remove(key)
    for key_type in all_key_types:
        assert len(lib_tool.find_keys(key_type)) == 0


def test_empty_excluding_key_types(lmdb_version_store_v2):
    version_store = lmdb_version_store_v2
    populate_db(version_store)
    lib_tool = version_store.library_tool()
    to_remove = (KeyType.VERSION, KeyType.VERSION_REF, KeyType.TABLE_INDEX)
    for key_type in to_remove:
        keys = lib_tool.find_keys(key_type)
        for k in keys:
            lib_tool.remove(k)

    assert not version_store.version_store.is_empty_excluding_key_types([KeyType.TABLE_DATA])
    assert not version_store.version_store.is_empty_excluding_key_types([KeyType.VERSION])
    assert version_store.version_store.is_empty_excluding_key_types([KeyType.TABLE_DATA, KeyType.SYMBOL_LIST,
                                                                     KeyType.MULTI_KEY, KeyType.SNAPSHOT_REF,
                                                                     KeyType.SNAPSHOT, KeyType.VERSION])


def test_empty_excluding_key_types_empty_lib(lmdb_version_store_v2):
    version_store = lmdb_version_store_v2
    version_store.write("test", [1, 2, 3])

    assert not version_store.version_store.is_empty_excluding_key_types([])
    assert not version_store.version_store.empty()

    lib_tool = version_store.library_tool()
    to_remove = (KeyType.VERSION, KeyType.VERSION_REF, KeyType.TABLE_INDEX, KeyType.TABLE_DATA, KeyType.SYMBOL_LIST)
    for key_type in to_remove:
        keys = lib_tool.find_keys(key_type)
        for k in keys:
            lib_tool.remove(k)

    assert version_store.version_store.is_empty_excluding_key_types([])
    assert version_store.version_store.empty()


def test_empty_excluding_key_types_just_symbol_list(lmdb_version_store_v2):
    version_store = lmdb_version_store_v2

    version_store.list_symbols()

    assert not version_store.version_store.is_empty_excluding_key_types([])
    assert version_store.version_store.is_empty_excluding_key_types([KeyType.SYMBOL_LIST])
    assert version_store.version_store.empty()


def test_write_keys(object_and_mem_and_lmdb_version_store):
    populate_db(object_and_mem_and_lmdb_version_store)
    lib_tool = object_and_mem_and_lmdb_version_store.library_tool()
    all_key_types = lib_tool.key_types()
    all_keys = []
    for key_type in all_key_types:
        if key_type not in get_ref_key_types() + get_log_types():
            all_keys = all_keys + lib_tool.find_keys(key_type)
    for key in all_keys:
        segment = lib_tool.read_to_segment(key)
        key.change_id("new_id")
        lib_tool.write(key, segment)
        lib_tool.remove(key)
    new_keys = []
    for key_type in all_key_types:
        if key_type not in get_ref_key_types() + get_log_types():
            new_keys = new_keys + lib_tool.find_keys(key_type)
    assert len(new_keys) == len(all_keys)


def test_count_keys(object_and_mem_and_lmdb_version_store):
    df = sample_dataframe()
    lib = object_and_mem_and_lmdb_version_store
    lib.write("symbol", df)
    lib.write("pickled", data={"a": 1}, pickle_on_failure=True)
    lib.snapshot("mysnap")
    lib.write("rec_norm", data={"a": np.arange(5), "b": np.arange(8), "c": None}, recursive_normalizers=True)
    lib_tool = lib.library_tool()
    assert lib.is_symbol_pickled("pickled")
    assert not lib.is_symbol_pickled("rec_norm")
    assert len(lib.list_symbols()) == 3
    assert lib_tool.count_keys(KeyType.VERSION) == 3
    assert lib_tool.count_keys(KeyType.SNAPSHOT_REF) == 1
    assert lib_tool.count_keys(KeyType.MULTI_KEY) == 1
    assert lib_tool.count_keys(KeyType.SNAPSHOT) == 0


@pytest.mark.parametrize("use_time_index", [True, False])
def test_read_data_key_from_version_ref(in_memory_version_store, use_time_index):
    lib = in_memory_version_store
    lib_tool = lib.library_tool()
    sym = "sym"

    df = pd.DataFrame({"a": [1, 2, 3]})
    if use_time_index:
        df = pd.DataFrame(index=pd.date_range(start=pd.Timestamp(0), periods=3), data={"a": [1, 2, 3]})
    lib.write(sym, df)

    ver_ref_key = lib_tool.find_keys_for_symbol(KeyType.VERSION_REF, sym)[0]
    ver_ref_entries = lib_tool.read_to_keys(ver_ref_key)
    assert len(ver_ref_entries) == 2 # We expect a 2 entry version ref (i.e. without a cached undeleted) because we have just one version
    assert ver_ref_entries[0].type == KeyType.TABLE_INDEX
    assert ver_ref_entries[1].type == KeyType.VERSION

    ver_key = ver_ref_entries[-1]
    ver_entries = lib_tool.read_to_keys(ver_key)
    assert len(ver_entries) == 1
    assert ver_entries[0].type == KeyType.TABLE_INDEX

    index_key = ver_entries[0]
    index_entries = lib_tool.read_to_keys(index_key)
    assert len(index_entries) == 1
    assert index_entries[0].type == KeyType.TABLE_DATA

    data_key = index_entries[0]
    stored_df = lib_tool.read_to_dataframe(data_key)

    # Fix index because reading directly from data key loses that information
    if use_time_index:
        stored_df.index.name = None
    else:
        stored_df = stored_df.reset_index()
    assert_frame_equal(stored_df, df)


def test_iterate_version_chain_with_lib_tool(in_memory_version_store):
    lib = in_memory_version_store
    lib_tool = lib.library_tool()
    sym = "sym"
    num_versions = 20

    # Populate some versions
    df = pd.DataFrame(index=pd.date_range(start=pd.Timestamp(0), periods=3), data={"a": [1, 2, 3]})
    for i in range(num_versions):
        prune_previous = i % 3 == 0
        lib.write(sym, df, prune_previous_version=prune_previous)

    keys_by_key_type = {}
    # No need for memoization because we will visit each entry exactly once because we only do writes.
    # (If we e.g. did appends we would have added table data entries multiple times)
    def iterate_through_version_chain(key):
        nonlocal keys_by_key_type
        nonlocal lib_tool
        # Add current key
        if key.type not in keys_by_key_type:
            keys_by_key_type[key.type] = []
        keys_by_key_type[key.type].append(key)

        # Iterate next keys
        next_keys = []
        if key.type == KeyType.VERSION_REF:
            # For version refs we only want to visit the last entry which is the last VERSION key
            next_keys = lib_tool.read_to_keys(key)[-1:]
        if key.type in [KeyType.VERSION, KeyType.TABLE_INDEX]:
            try:
                next_keys = lib_tool.read_to_keys(key)
            except:
                # Deleted index key
                next_keys = []
        for next_key in next_keys:
            iterate_through_version_chain(next_key)

    version_ref = lib_tool.find_keys_for_symbol(KeyType.VERSION_REF, sym)[0]
    iterate_through_version_chain(version_ref)

    # We exclude index keys because we'll see table indices while iterating which are deleted.
    for key_type in [KeyType.VERSION_REF, KeyType.VERSION, KeyType.TABLE_DATA]:
        expected_keys = [str(key) for key in lib_tool.find_keys_for_symbol(key_type, sym)]
        iterated_keys = [str(key) for key in keys_by_key_type[key_type]]
        assert sorted(iterated_keys) == sorted(expected_keys)

    assert len(keys_by_key_type[KeyType.VERSION_REF]) == 1
    assert len(keys_by_key_type[KeyType.VERSION]) == num_versions + num_versions // 3
    assert len(keys_by_key_type[KeyType.TABLE_INDEX]) == num_versions
    assert len(keys_by_key_type[KeyType.TABLE_DATA]) == (num_versions-1) % 3 + 1
    assert len(keys_by_key_type[KeyType.TOMBSTONE_ALL]) == num_versions // 3

