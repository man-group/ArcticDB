"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb.util.test import sample_dataframe
from arcticdb import KeyType, Size

from arcticdb.options import EnterpriseLibraryOptions


def test_get_sizes(arctic_client, lib_name):
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
    sizes = arctic_library.admin_tools().get_sizes()

    # Then
    assert len(sizes) == 9
    assert sizes[KeyType.VERSION_REF].count == 2
    assert 500 < sizes[KeyType.VERSION_REF].bytes_compressed < 2000
    assert sizes[KeyType.VERSION].count == 5
    assert 3000 < sizes[KeyType.VERSION].bytes_compressed < 5000
    assert sizes[KeyType.TABLE_INDEX].count == 3
    assert 3000 < sizes[KeyType.TABLE_INDEX].bytes_compressed < 6000
    assert sizes[KeyType.TABLE_DATA].count == 7
    assert 20e6 < sizes[KeyType.TABLE_DATA].bytes_compressed < 30e6
    assert sizes[KeyType.SYMBOL_LIST].count == 4
    assert 500 < sizes[KeyType.SYMBOL_LIST].bytes_compressed < 3000
    assert sizes[KeyType.LOG].count == 5

    for t in (KeyType.APPEND_DATA, KeyType.SNAPSHOT_REF, KeyType.LOG_COMPACTED):
        assert sizes[t].count == 0
        assert sizes[t].bytes_compressed == 0

    arctic_library.delete("sym_1")
    sizes = arctic_library.admin_tools().get_sizes()
    assert sizes[KeyType.VERSION_REF].count == 2
    assert sizes[KeyType.VERSION].count == 6
    assert sizes[KeyType.TABLE_INDEX].count == 1
    assert sizes[KeyType.TABLE_DATA].count == 3
    assert sizes[KeyType.SYMBOL_LIST].count == 5
    assert 10e6 < sizes[KeyType.TABLE_DATA].bytes_compressed < 15e6

    # Check the other key types
    arctic_library.snapshot("snap")
    arctic_library.write("new_sym", df, staged=True)
    sizes = arctic_library.admin_tools().get_sizes()
    assert sizes[KeyType.SNAPSHOT_REF].count == 1
    assert sizes[KeyType.APPEND_DATA].count == 3
    assert 10e6 < sizes[KeyType.APPEND_DATA].bytes_compressed < 15e6


def test_get_sizes_by_symbol(arctic_client, lib_name):
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
    assert len(sizes["sym_1"]) == 5
    assert len(sizes["sym_2"]) == 5
    assert sizes["sym_1"].keys() == {KeyType.VERSION_REF, KeyType.VERSION, KeyType.TABLE_INDEX, KeyType.TABLE_DATA,
                                     KeyType.APPEND_DATA}

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


def test_get_sizes_for_symbol(arctic_client, lib_name):
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

    expected_key_types = {KeyType.VERSION_REF, KeyType.VERSION, KeyType.TABLE_INDEX, KeyType.TABLE_DATA, KeyType.APPEND_DATA}
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


def test_size_apis_self_consistent(arctic_library, lib_name):
    # Given
    arctic_library.write_pickle("sym_1", 1)
    arctic_library.write_pickle("sym_1", 2)
    df = sample_dataframe(size=250_000)
    arctic_library.write("sym_1", df)
    arctic_library.write("sym_1", df, staged=True)

    # When
    sizes = arctic_library.admin_tools().get_sizes()
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
