"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest
from arcticdb.storage_fixtures.api import StorageFixture
from arcticdb.util.test import sample_dataframe
from arcticdb import KeyType, Size, Arctic

from arcticdb.options import EnterpriseLibraryOptions
from arcticdb.version_store.admin_tools import sum_sizes
from arcticdb.version_store.library import Library


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("azurite"),
    ],
)
def azurite_client(request, encoding_version) -> Arctic:
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=encoding_version)
    return ac


@pytest.fixture
def arctic_library(azurite_client, lib_name) -> Library:
    yield azurite_client.create_library(lib_name)
    azurite_client.delete_library(lib_name)
    

def test_get_sizes(azurite_client, lib_name):
    lib_opts = EnterpriseLibraryOptions(replication=True)
    arctic_library = azurite_client.create_library(lib_name, enterprise_library_options=lib_opts)
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
    assert len(sizes) == 10
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

    for t in (KeyType.APPEND_DATA, KeyType.SNAPSHOT_REF, KeyType.LOG_COMPACTED, KeyType.MULTI_KEY):
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

    total_size = sum_sizes(sizes.values())
    assert total_size.count == 23
    assert total_size.bytes_compressed == sum(s.bytes_compressed for s in sizes.values())

    # Check the other key types
    arctic_library.snapshot("snap")
    arctic_library.write("new_sym", df, staged=True)
    sizes = arctic_library.admin_tools().get_sizes()
    assert sizes[KeyType.SNAPSHOT_REF].count == 1
    assert sizes[KeyType.APPEND_DATA].count == 3
    assert 10e6 < sizes[KeyType.APPEND_DATA].bytes_compressed < 15e6

    arctic_library._nvs.write("rec", [df, df], recursive_normalizers=True)
    sizes = arctic_library.admin_tools().get_sizes()
    assert sizes[KeyType.MULTI_KEY].count == 1
    assert sizes[KeyType.MULTI_KEY].bytes_compressed > 0

    azurite_client.delete_library(lib_name)


def test_get_sizes_by_symbol(azurite_client, lib_name):
    lib_opts = EnterpriseLibraryOptions(replication=True)
    arctic_library = azurite_client.create_library(lib_name, enterprise_library_options=lib_opts)
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
    assert sizes["sym_1"].keys() == {KeyType.VERSION_REF, KeyType.VERSION, KeyType.TABLE_INDEX, KeyType.TABLE_DATA,
                                     KeyType.APPEND_DATA, KeyType.MULTI_KEY}

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

    azurite_client.delete_library(lib_name)


def test_get_sizes_for_symbol(azurite_client, lib_name):
    lib_opts = EnterpriseLibraryOptions(replication=True)
    arctic_library = azurite_client.create_library(lib_name, enterprise_library_options=lib_opts)
    arctic_library.write_pickle("sym_1", 1)
    arctic_library.write_pickle("sym_1", 2)
    df = sample_dataframe(size=250_000)
    arctic_library.write("sym_1", df)
    arctic_library.delete("sym_1", versions=[0])

    arctic_library.write_pickle("delete_me", 1)
    arctic_library.delete("delete_me")

    non_existent_sizes = arctic_library.admin_tools().get_sizes_for_symbol("non-existent")

    expected_key_types = {KeyType.VERSION_REF, KeyType.VERSION, KeyType.TABLE_INDEX, KeyType.TABLE_DATA, KeyType.APPEND_DATA, KeyType.MULTI_KEY}
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

    azurite_client.delete_library(lib_name)


def test_size_apis_self_consistent(arctic_library):
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


def test_one(arctic_library):
    arctic_library.write_pickle("sym_1", 1)


def test_two(arctic_library):
    arctic_library.write("sym_1", sample_dataframe(size=25))


def test_two(arctic_library):
    arctic_library.write("sym_1", sample_dataframe(size=1000))
    arctic_library.write("sym_2", sample_dataframe(size=1000))
    arctic_library.write("sym_3", sample_dataframe(size=1000))
