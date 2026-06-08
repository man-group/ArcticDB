"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, version 2.0.
"""

import pytest

from arcticdb_ext.exceptions import UserInputException
from arcticdb.storage_fixtures.azure import AzureContainer
from arcticdb.util.test import assert_frame_equal, config_context, sample_dataframe
from tests.util.mark import AZURE_TESTS_MARK

# Azure Blob Storage silently converts '\' to '/' (the service is built on .NET's Uri class), so the recorded name no
# longer matches the on-disk blob path. New symbol/snapshot/library names containing '\' must therefore be rejected on
# Azure, while names that already exist must keep working.
BACKSLASH_NAME = "with\\backslash"


@AZURE_TESTS_MARK
def test_write_new_symbol_with_backslash_rejected_on_azure(azure_version_store):
    with pytest.raises(UserInputException):
        azure_version_store.write(BACKSLASH_NAME, sample_dataframe())
    assert BACKSLASH_NAME not in azure_version_store.list_symbols()


@pytest.mark.parametrize("symbol", ["with\\backslash", "with/forwardslash"])
def test_write_new_symbol_with_slashes_allowed_on_non_azure(lmdb_version_store, symbol):
    # Backslash is only problematic for Azure, and the '/' library-name restriction (Mongo) must not leak onto symbol
    # names. Other backends must keep accepting both.
    lmdb_version_store.write(symbol, sample_dataframe())
    assert symbol in lmdb_version_store.list_symbols()


@AZURE_TESTS_MARK
def test_existing_backslash_symbol_remains_usable_on_azure(azure_version_store):
    lib = azure_version_store
    df_v0 = sample_dataframe(10)
    # Simulate a symbol created before this validation existed by disabling the strict check for the initial write.
    with config_context("VersionStore.NoStrictSymbolCheck", 1):
        lib.write(BACKSLASH_NAME, df_v0)

    assert_frame_equal(lib.read(BACKSLASH_NAME).data, df_v0)

    df_v1 = sample_dataframe(20)
    lib.append(BACKSLASH_NAME, df_v1)  # version_id != 0 -> validation is skipped
    df_v2 = sample_dataframe(30)
    lib.write(BACKSLASH_NAME, df_v2)  # overwriting an existing symbol -> validation is skipped
    assert_frame_equal(lib.read(BACKSLASH_NAME).data, df_v2)


@AZURE_TESTS_MARK
def test_create_snapshot_with_backslash_rejected_on_azure(azurite_storage: AzureContainer):
    ac = azurite_storage.create_arctic()
    lib = ac.create_library("test_snapshot_backslash")
    lib.write("sym", sample_dataframe())
    with pytest.raises(UserInputException):
        lib.snapshot(BACKSLASH_NAME)
    assert BACKSLASH_NAME not in lib.list_snapshots()


@AZURE_TESTS_MARK
def test_create_library_with_backslash_rejected_on_azure(azurite_storage: AzureContainer):
    ac = azurite_storage.create_arctic()
    with pytest.raises(UserInputException):
        ac.create_library(f"lib_{BACKSLASH_NAME}")


def test_create_snapshot_with_backslash_allowed_on_non_azure(lmdb_storage):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library("test_snapshot_backslash_allowed")
    lib.write("sym", sample_dataframe())
    lib.snapshot(BACKSLASH_NAME)
    assert BACKSLASH_NAME in lib.list_snapshots()


def test_create_library_with_backslash_allowed_on_non_azure(lmdb_storage):
    ac = lmdb_storage.create_arctic()
    lib_name = f"lib_{BACKSLASH_NAME}"
    ac.create_library(lib_name)
    assert lib_name in ac.list_libraries()
