import pytest
import os
import pandas as pd
from arcticdb.arctic import Arctic
from arcticdb.util.test import assert_frame_equal
from tests.util.storage_test import (
    get_seed_libraries,
    get_real_s3_uri,
    generate_pseudo_random_dataframe,
    generate_ascending_dataframe,
)
from arcticdb.version_store.library import WritePayload, ReadRequest
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
    val = lib.read("one").data
    assert_frame_equal(val, one_df)

    two_df_1 = three_col_df(1)
    lib.write("two", two_df_1)
    two_df_2 = three_col_df(2)
    lib.append("two", two_df_2)
    val = lib.read("two")
    # TODO: Add a better check
    assert len(val.data) == 20

    three_df = three_col_df(3)
    lib.append("three", three_df)
    val = lib.read("three").data
    assert_frame_equal(val, three_df)


@pytest.mark.parametrize(
    "num_rows",
    [1_000_000, 10_000_000],
)
@pytest.mark.skipif(
    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="This test should run only if the persistent storage tests are enabled"
)
# We are testing with arctic_client so we can test the performance of the various client options over big data
def test_persistent_storage_read_write_large_data_ascending(arctic_client, num_rows):
    arctic_client.create_library("test_persistent_storage_read_write_large_data_ascending")
    lib = arctic_client["test_persistent_storage_read_write_large_data_ascending"]

    sym = str(num_rows)
    orig_df = generate_ascending_dataframe(num_rows)
    lib.write(sym, orig_df)
    result_df = lib.read(sym).data
    assert_frame_equal(result_df, orig_df)


@pytest.mark.parametrize(
    "num_rows",
    [1_000_000, 10_000_000, 100_000_000],
)
@pytest.mark.skipif(
    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="This test should run only if the persistent storage tests are enabled"
)
# We are testing with arctic_client so we can test the performance of the various client options over big data
def test_persistent_storage_read_write_large_data_random(arctic_client, num_rows):
    arctic_client.create_library("test_persistent_storage_read_write_large_data_ascending")
    lib = arctic_client["test_persistent_storage_read_write_large_data_ascending"]

    sym = str(num_rows)
    orig_df = generate_pseudo_random_dataframe(num_rows)
    lib.write(sym, orig_df)
    result_df = lib.read(sym).data
    assert_frame_equal(result_df, orig_df)


@pytest.mark.parametrize(
    "num_syms",
    [
        1_000,
        5_000,
        10_000,
    ],
)
@pytest.mark.skipif(
    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="This test should run only if the persistent storage tests are enabled"
)
# We are testing with arctic_client so we can test the performance of the various client options over big data
def test_persistent_storage_read_write_many_syms(arctic_client, num_syms, three_col_df):
    # For now, this tests only the breadth (e.g. number of symbols)
    # We have another test, that tests with "deeper" data frames
    arctic_client.create_library("test_persistent_storage_read_write_many_syms")
    lib = arctic_client["test_persistent_storage_read_write_many_syms"]
    df = three_col_df()
    payloads = [WritePayload(f"{sym}_sym", df) for sym in range(num_syms)]
    lib.write_batch(payloads)

    read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_syms)]
    results = lib.read_batch(read_reqs)
    for res in result_df:
        result_df = res.data
        assert_frame_equal(result_df, df)
