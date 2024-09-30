"""
Copyright 2023 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
import numpy as np

from arcticdb.util.test import sample_dataframe, populate_db
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
    lib_tool = version_store.library_tool()
    to_remove = (KeyType.VERSION, KeyType.VERSION_REF, KeyType.TABLE_INDEX, KeyType.TABLE_DATA, KeyType.SYMBOL_LIST)
    for key_type in to_remove:
        keys = lib_tool.find_keys(key_type)
        for k in keys:
            lib_tool.remove(k)

    assert version_store.version_store.is_empty_excluding_key_types([])


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
