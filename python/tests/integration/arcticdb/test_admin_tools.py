"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
import numpy as np
import pandas as pd
from arcticdb.util.logger import get_logger
import arcticdb_ext
import arcticdb_ext.storage
from arcticdb.util.test import sample_dataframe
from arcticdb import KeyType, Size, Arctic

from arcticdb.options import EnterpriseLibraryOptions
from arcticdb.version_store.admin_tools import AdminTools, sum_sizes

logger = get_logger()


def retry_get_sizes(admin_tools: AdminTools, retries=3, base_delay=1, key_types=None):
    for attempt in range(retries + 1):
        try:
            result = admin_tools.get_sizes(key_types=key_types)
            return result
        except arcticdb_ext.exceptions.StorageException as e:
            if ("E_UNEXPECTED_AZURE_ERROR" in str(e)) and (attempt < retries):
                wait_time = base_delay * (2**attempt)
                logger.info(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise


def test_get_sizes(arctic_client, lib_name, all_recursive_metastructure_versions):
    lib_opts = EnterpriseLibraryOptions(replication=True)
    arctic_library = arctic_client.create_library(lib_name, enterprise_library_options=lib_opts)
    # Given
    arctic_library.write_pickle("sym_1", 1)
    arctic_library.write_pickle("sym_1", 2)
    df = sample_dataframe(size=250_000)
    arctic_library.write("sym_1", df)
    arctic_library.write("sym_2", df)
    arctic_library.delete("sym_1", versions=[0])

    # When
    sizes = retry_get_sizes(arctic_library.admin_tools())

    # Then
    assert len(sizes) == 10
    assert sizes[KeyType.VERSION_REF].count == 2
    assert 500 < sizes[KeyType.VERSION_REF].bytes_compressed < 2000
    assert sizes[KeyType.VERSION].count == 5
    assert 3000 < sizes[KeyType.VERSION].bytes_compressed < 5000
    assert sizes[KeyType.TABLE_INDEX].count == 3
    assert 3000 < sizes[KeyType.TABLE_INDEX].bytes_compressed < 6000
    assert sizes[KeyType.TABLE_DATA].count == 7
    assert 20e6 < sizes[KeyType.TABLE_DATA].bytes_compressed < 30e6
    assert sizes[KeyType.SYMBOL_LIST].count == 2
    assert 250 < sizes[KeyType.SYMBOL_LIST].bytes_compressed < 3000
    assert sizes[KeyType.LOG].count == 5

    for t in (KeyType.APPEND_DATA, KeyType.SNAPSHOT_REF, KeyType.LOG_COMPACTED, KeyType.MULTI_KEY):
        assert sizes[t].count == 0
        assert sizes[t].bytes_compressed == 0

    arctic_library.delete("sym_1")
    sizes = retry_get_sizes(arctic_library.admin_tools())
    assert sizes[KeyType.VERSION_REF].count == 2
    assert sizes[KeyType.VERSION].count == 6
    assert sizes[KeyType.TABLE_INDEX].count == 1
    assert sizes[KeyType.TABLE_DATA].count == 3
    assert sizes[KeyType.SYMBOL_LIST].count == 3
    assert 10e6 < sizes[KeyType.TABLE_DATA].bytes_compressed < 15e6

    total_size = sum_sizes(sizes.values())
    assert total_size.count == 21
    assert total_size.bytes_compressed == sum(s.bytes_compressed for s in sizes.values())

    # Check the other key types
    arctic_library.snapshot("snap")
    arctic_library.write("new_sym", df, staged=True)
    sizes = retry_get_sizes(arctic_library.admin_tools())
    assert sizes[KeyType.SNAPSHOT_REF].count == 1
    assert sizes[KeyType.APPEND_DATA].count == 3
    assert 10e6 < sizes[KeyType.APPEND_DATA].bytes_compressed < 15e6

    arctic_library._nvs.write("rec", [df, df], recursive_normalizers=True)
    sizes = retry_get_sizes(arctic_library.admin_tools())
    assert sizes[KeyType.MULTI_KEY].count == 1
    assert sizes[KeyType.MULTI_KEY].bytes_compressed > 0


DEFAULT_SCANNED_KEY_TYPES = {
    KeyType.TABLE_DATA,
    KeyType.TABLE_INDEX,
    KeyType.VERSION,
    KeyType.VERSION_REF,
    KeyType.APPEND_DATA,
    KeyType.MULTI_KEY,
    KeyType.SNAPSHOT_REF,
    KeyType.LOG,
    KeyType.LOG_COMPACTED,
    KeyType.SYMBOL_LIST,
}


def test_key_type_native_mapping_is_one_to_one():
    """The mapping is derived by name, so the only way to break it is a wrong _NATIVE_NAMES alias.

    That fails silently rather than loudly: two members mapping to the same native type means one of them
    reports the other's numbers, and _from_native has no way back to the one it displaced. Whether every
    member can actually be scanned is covered by test_get_sizes_key_types_everything."""
    natives = [k.to_native() for k in KeyType]

    assert len(set(natives)) == len(natives)


def test_get_sizes_key_types_default_unchanged(arctic_client, lib_name):
    arctic_library = arctic_client.create_library(lib_name)
    arctic_library.write("sym", sample_dataframe(size=100))

    sizes = retry_get_sizes(arctic_library.admin_tools())

    assert set(sizes) == DEFAULT_SCANNED_KEY_TYPES


def test_get_sizes_key_types_subset(arctic_client, lib_name):
    arctic_library = arctic_client.create_library(lib_name)
    arctic_library.write("sym", sample_dataframe(size=100))

    requested = [KeyType.TABLE_DATA, KeyType.VERSION_REF]
    sizes = retry_get_sizes(arctic_library.admin_tools(), key_types=requested)

    assert set(sizes) == set(requested)
    assert sizes[KeyType.TABLE_DATA].count > 0
    assert sizes[KeyType.VERSION_REF].count == 1


def test_get_sizes_key_types_everything(arctic_client, lib_name):
    lib_opts = EnterpriseLibraryOptions(replication=True)
    arctic_library = arctic_client.create_library(lib_name, enterprise_library_options=lib_opts)
    arctic_library.write("sym", sample_dataframe(size=100))
    arctic_library._nvs.create_column_stats_experimental("sym")

    sizes = retry_get_sizes(arctic_library.admin_tools(), key_types=list(KeyType))

    # Every key type that can exist as an object in storage is scannable, and all of them are reported
    assert set(sizes) == set(KeyType)
    assert DEFAULT_SCANNED_KEY_TYPES < set(sizes)

    # A key type outside the default set that this library does have
    assert sizes[KeyType.COLUMN_STATS].count == 1
    assert sizes[KeyType.COLUMN_STATS].bytes_compressed > 0

    # ...and ones it does not. These are reported as empty rather than omitted.
    for key_type in (KeyType.SNAPSHOT, KeyType.GENERATION, KeyType.PARTITION, KeyType.METRICS):
        assert sizes[key_type].count == 0
        assert sizes[key_type].bytes_compressed == 0


def test_scan_object_sizes_records_duration(arctic_client, lib_name):
    arctic_library = arctic_client.create_library(lib_name)
    arctic_library.write("sym", sample_dataframe(size=100))

    sizes = arctic_library._nvs.version_store.scan_object_sizes()

    by_key_type = {s.key_type: s for s in sizes}
    data = by_key_type[arcticdb_ext.storage.KeyType.TABLE_DATA]
    assert data.count > 0
    assert data.scan_duration_ns > 0


def test_scan_object_sizes_for_stream_records_duration(arctic_client, lib_name):
    """The per-symbol scan reports a duration too - a scan happened, so zero would be a lie."""
    arctic_library = arctic_client.create_library(lib_name)
    arctic_library.write("sym", sample_dataframe(size=100))

    sizes = arctic_library._nvs.version_store.scan_object_sizes_for_stream("sym")

    by_key_type = {s.key_type: s for s in sizes}
    data = by_key_type[arcticdb_ext.storage.KeyType.TABLE_DATA]
    assert data.count > 0
    assert data.scan_duration_ns > 0


def test_get_sizes_by_symbol(arctic_client, lib_name, all_recursive_metastructure_versions):
    lib_opts = EnterpriseLibraryOptions(replication=True)
    arctic_library = arctic_client.create_library(lib_name, enterprise_library_options=lib_opts)
    # Given
    arctic_library.write_pickle("sym_1", 1)
    arctic_library.write_pickle("sym_1", 2)
    df = sample_dataframe(size=250_000)
    arctic_library.write("sym_1", df)
    arctic_library.write("sym_2", df)
    arctic_library.delete("sym_1", versions=[0])

    # When
    sizes = arctic_library.admin_tools().get_sizes_by_symbol()

    # Then
    assert len(sizes) == 2
    assert len(sizes["sym_1"]) == 6
    assert len(sizes["sym_2"]) == 6
    assert sizes["sym_1"].keys() == {
        KeyType.VERSION_REF,
        KeyType.VERSION,
        KeyType.TABLE_INDEX,
        KeyType.TABLE_DATA,
        KeyType.APPEND_DATA,
        KeyType.MULTI_KEY,
    }

    assert sizes["sym_1"][KeyType.VERSION_REF].count == 1
    assert sizes["sym_2"][KeyType.VERSION_REF].count == 1
    assert 500 < sizes["sym_1"][KeyType.VERSION_REF].bytes_compressed < 2000

    assert sizes["sym_1"][KeyType.VERSION].count == 4
    assert sizes["sym_2"][KeyType.VERSION].count == 1
    assert 2000 < sizes["sym_1"][KeyType.VERSION].bytes_compressed < 4000
    assert 500 < sizes["sym_2"][KeyType.VERSION].bytes_compressed < 1000

    assert sizes["sym_1"][KeyType.TABLE_INDEX].count == 2
    assert 2000 < sizes["sym_1"][KeyType.TABLE_INDEX].bytes_compressed < 4000
    assert sizes["sym_1"][KeyType.TABLE_DATA].count == 4
    assert 10e6 < sizes["sym_1"][KeyType.TABLE_DATA].bytes_compressed < 20e6
    assert sizes["sym_1"][KeyType.APPEND_DATA].count == 0
    assert sizes["sym_1"][KeyType.APPEND_DATA].bytes_compressed == 0
    assert sizes["sym_1"][KeyType.MULTI_KEY].count == 0
    assert sizes["sym_1"][KeyType.MULTI_KEY].bytes_compressed == 0

    arctic_library.delete("sym_1")
    sizes = arctic_library.admin_tools().get_sizes_by_symbol()
    assert sizes["sym_1"][KeyType.VERSION_REF].count == 1
    assert sizes["sym_1"][KeyType.VERSION].count == 5
    assert sizes["sym_1"][KeyType.TABLE_INDEX].count == 0
    assert sizes["sym_1"][KeyType.TABLE_DATA].count == 0

    arctic_library.write("new_sym", df, staged=True)
    sizes = arctic_library.admin_tools().get_sizes_by_symbol()
    assert sizes["new_sym"][KeyType.APPEND_DATA].count == 3
    assert 10e6 < sizes["new_sym"][KeyType.APPEND_DATA].bytes_compressed < 15e6

    arctic_library._nvs.write("rec", [df, df], recursive_normalizers=True)
    sizes = arctic_library.admin_tools().get_sizes_by_symbol()["rec"]
    assert sizes[KeyType.MULTI_KEY].count == 1
    assert sizes[KeyType.MULTI_KEY].bytes_compressed > 0


def test_get_sizes_for_symbol(arctic_client, lib_name, all_recursive_metastructure_versions):
    lib_opts = EnterpriseLibraryOptions(replication=True)
    arctic_library = arctic_client.create_library(lib_name, enterprise_library_options=lib_opts)
    arctic_library.write_pickle("sym_1", 1)
    arctic_library.write_pickle("sym_1", 2)
    df = sample_dataframe(size=250_000)
    arctic_library.write("sym_1", df)
    arctic_library.delete("sym_1", versions=[0])

    arctic_library.write_pickle("delete_me", 1)
    arctic_library.delete("delete_me")

    non_existent_sizes = arctic_library.admin_tools().get_sizes_for_symbol("non-existent")

    expected_key_types = {
        KeyType.VERSION_REF,
        KeyType.VERSION,
        KeyType.TABLE_INDEX,
        KeyType.TABLE_DATA,
        KeyType.APPEND_DATA,
        KeyType.MULTI_KEY,
    }
    assert non_existent_sizes.keys() == expected_key_types
    for size in non_existent_sizes.values():
        assert size == Size(0, 0)

    deleted_sizes = arctic_library.admin_tools().get_sizes_for_symbol("delete_me")
    assert deleted_sizes.keys() == expected_key_types
    assert deleted_sizes[KeyType.VERSION_REF].count == 1
    assert deleted_sizes[KeyType.VERSION].count == 2
    for t in (KeyType.TABLE_INDEX, KeyType.TABLE_DATA, KeyType.APPEND_DATA):
        assert deleted_sizes[t] == Size(0, 0)

    sizes = arctic_library.admin_tools().get_sizes_for_symbol("sym_1")
    assert sizes.keys() == expected_key_types

    assert sizes[KeyType.VERSION_REF].count == 1
    assert sizes[KeyType.VERSION_REF].count == 1
    assert 500 < sizes[KeyType.VERSION_REF].bytes_compressed < 2000

    assert sizes[KeyType.VERSION].count == 4
    assert 1000 < sizes[KeyType.VERSION].bytes_compressed < 4000

    assert sizes[KeyType.TABLE_INDEX].count == 2
    assert 2000 < sizes[KeyType.TABLE_INDEX].bytes_compressed < 4500
    assert sizes[KeyType.TABLE_DATA].count == 4
    assert 10e6 < sizes[KeyType.TABLE_DATA].bytes_compressed < 15e6
    assert sizes[KeyType.APPEND_DATA].count == 0
    assert sizes[KeyType.APPEND_DATA].bytes_compressed == 0

    arctic_library.delete("sym_1")
    sizes = arctic_library.admin_tools().get_sizes_for_symbol("sym_1")
    assert sizes[KeyType.VERSION_REF].count == 1
    assert sizes[KeyType.VERSION].count == 5
    assert sizes[KeyType.TABLE_INDEX].count == 0
    assert sizes[KeyType.TABLE_INDEX].bytes_compressed == 0
    assert sizes[KeyType.TABLE_DATA].count == 0
    assert sizes[KeyType.TABLE_DATA].bytes_compressed == 0

    arctic_library.write("new_sym", df, staged=True)
    sizes = arctic_library.admin_tools().get_sizes_for_symbol("new_sym")
    assert sizes[KeyType.APPEND_DATA].count == 3
    assert 10e6 < sizes[KeyType.APPEND_DATA].bytes_compressed < 15e6

    arctic_library._nvs.write("rec", [df, df], recursive_normalizers=True)
    sizes = arctic_library.admin_tools().get_sizes_for_symbol("rec")
    assert sizes[KeyType.MULTI_KEY].count == 1
    assert sizes[KeyType.MULTI_KEY].bytes_compressed > 0


def test_size_apis_self_consistent(arctic_library, lib_name):
    # Given
    arctic_library.write_pickle("sym_1", 1)
    arctic_library.write_pickle("sym_1", 2)
    df = sample_dataframe(size=250_000)
    arctic_library.write("sym_1", df)
    arctic_library.write("sym_1", df, staged=True)

    # When
    sizes = retry_get_sizes(arctic_library.admin_tools())
    by_symbol = arctic_library.admin_tools().get_sizes_by_symbol()
    assert len(by_symbol) == 1
    by_symbol = by_symbol["sym_1"]
    for_symbol = arctic_library.admin_tools().get_sizes_for_symbol("sym_1")

    # Then
    for t in (KeyType.VERSION_REF, KeyType.VERSION, KeyType.TABLE_INDEX, KeyType.TABLE_DATA, KeyType.APPEND_DATA):
        size = sizes[t]
        assert size == by_symbol[t]
        assert size == for_symbol[t]
        assert size.count > 0
        assert size.bytes_compressed > 0


def test_symbol_sizes_docs_example():
    """Test the documentation in `library_sizes.md`"""
    lib = Arctic("mem://").create_library("tst")
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 5)))
    lib.write("sym", df)

    admin_tools = lib.admin_tools()

    sizes = retry_get_sizes(admin_tools)
    assert sum_sizes(sizes.values()).count > 0
    assert sum_sizes(sizes.values()).bytes_compressed > 0
    assert sizes[KeyType.TABLE_DATA].count > 0
    assert sizes[KeyType.TABLE_DATA].bytes_compressed > 0

    by_symbol = admin_tools.get_sizes_by_symbol()
    size_for_sym = by_symbol["sym"]
    assert sum_sizes(size_for_sym.values()).count > 0
    assert sum_sizes(size_for_sym.values()).bytes_compressed > 0
    assert size_for_sym[KeyType.TABLE_INDEX].count > 0
    assert size_for_sym[KeyType.TABLE_INDEX].bytes_compressed > 0

    for_symbol = admin_tools.get_sizes_for_symbol("sym")
    assert sum_sizes(for_symbol.values()).count > 0
    assert sum_sizes(for_symbol.values()).bytes_compressed > 0
    assert for_symbol[KeyType.VERSION].count > 0
    assert for_symbol[KeyType.VERSION].bytes_compressed > 0
