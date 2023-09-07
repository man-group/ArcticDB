import pytest
import os
import pandas as pd
from arcticdb.arctic import Arctic
from arcticdb.util.storage_test import (
    get_seed_libraries,
    get_real_s3_uri,
    generate_pseudo_random_dataframe,
    generate_ascending_dataframe,
    verify_ascending_dataframe,
    verify_pseudo_random_dataframe,
)
from tests.conftest import PERSISTENT_STORAGE_TESTS_ENABLED

if PERSISTENT_STORAGE_TESTS_ENABLED:
    LIBRARIES = get_seed_libraries()
else:
    LIBRARIES = []


# TODO: Add a check if the real storage tests are enabled
@pytest.mark.parametrize("library", LIBRARIES)
@pytest.mark.skipif(
    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="This test should run only if the persistent storage tests are enabled"
)
def test_real_s3_storage_read(library):
    ac = Arctic(get_real_s3_uri())
    lib = ac[library]
    symbols = lib.list_symbols()
    assert len(symbols) == 3
    for sym in ["one", "two", "three"]:
        assert sym in symbols
    for sym in symbols:
        df = lib.read(sym).data
        column_names = df.columns.values.tolist()
        assert column_names == ["x", "y", "z"]


@pytest.mark.skipif(
    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="This test should run only if the persistent storage tests are enabled"
)
def test_real_s3_storage_write(three_col_df):
    strategy_branch = os.getenv("ARCTICDB_PERSISTENT_STORAGE_STRATEGY_BRANCH")
    library_to_write_to = f"test_{strategy_branch}"
    ac = Arctic(get_real_s3_uri())
    # There shouldn't be a library with this name present, so delete just in case
    ac.delete_library(library_to_write_to)
    ac.create_library(library_to_write_to)
    lib = ac[library_to_write_to]
    one_df = three_col_df()
    lib.write("one", one_df)
    val = lib.read("one")
    # TODO: assert one_df.equals(val.data)
    assert len(val.data) == 10

    two_df_1 = three_col_df(1)
    lib.write("two", two_df_1)
    two_df_2 = three_col_df(2)
    lib.append("two", two_df_2)
    val = lib.read("two")
    # TODO: Add a better check
    assert len(val.data) == 20

    three_df = three_col_df(3)
    lib.append("three", three_df)
    val = lib.read("three")


@pytest.mark.parametrize(
    "num_rows",
    [
        1_000_000,
        # 10_000_000
    ],
)
@pytest.mark.skipif(
    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="This test should run only if the persistent storage tests are enabled"
)
# We are testing with arctic_client so we can test the performance of the various client options over big data
def test_persistent_storage_read_write_large_data_ascending(arctic_client, num_rows):
    arctic_client.create_library("test_persistent_storage_read_write_large_data_ascending")
    lib = arctic_client["test_persistent_storage_read_write_large_data_ascending"]

    sym = str(num_rows)
    lib.write(sym, generate_ascending_dataframe(num_rows))
    result_df = lib.read(sym).data
    verify_ascending_dataframe(result_df, num_rows)


@pytest.mark.parametrize(
    "num_rows",
    [
        1_000_000,
        # 10_000_000,
        # 100_000_000
    ],
)
@pytest.mark.skipif(
    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="This test should run only if the persistent storage tests are enabled"
)
# We are testing with arctic_client so we can test the performance of the various client options over big data
def test_persistent_storage_read_write_large_data_random(arctic_client, num_rows):
    arctic_client.create_library("test_persistent_storage_read_write_large_data_ascending")
    lib = arctic_client["test_persistent_storage_read_write_large_data_ascending"]

    sym = str(num_rows)
    lib.write(sym, generate_pseudo_random_dataframe(num_rows))
    result_df = lib.read(sym).data
    verify_pseudo_random_dataframe(result_df, num_rows)
