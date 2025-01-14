import pytest
import os
from arcticdb.util.test import assert_frame_equal
from tests.util.mark import PERSISTENT_STORAGE_TESTS_ENABLED, REAL_S3_TESTS_MARK
from tests.util.storage_test import (
    get_seed_libraries,
    generate_pseudo_random_dataframe,
    generate_ascending_dataframe,
    read_persistent_library,
    write_persistent_library,
)
from arcticdb.version_store.library import WritePayload, ReadRequest

if PERSISTENT_STORAGE_TESTS_ENABLED:
    LIBRARIES = get_seed_libraries()
else:
    LIBRARIES = []


@pytest.fixture(params=[pytest.param("REAL_S3", marks=REAL_S3_TESTS_MARK)])
# Only test with encoding version 0 (a.k.a.) for now
# because there is a problem when older versions try to read configs with a written encoding version
# def shared_persistent_arctic_client(real_s3_storage_without_clean_up, encoding_version):
def shared_persistent_arctic_client(real_s3_storage_without_clean_up):
    return real_s3_storage_without_clean_up.create_arctic()


# TODO: Add a check if the real storage tests are enabled
@pytest.mark.parametrize("library", LIBRARIES)
@REAL_S3_TESTS_MARK
def test_real_s3_storage_read(shared_persistent_arctic_client, library):
    ac = shared_persistent_arctic_client
    lib = ac[library]
    read_persistent_library(lib)


@REAL_S3_TESTS_MARK
def test_real_s3_storage_write(shared_persistent_arctic_client):
    strategy_branch = os.getenv("ARCTICDB_PERSISTENT_STORAGE_STRATEGY_BRANCH")
    library_to_write_to = f"test_{strategy_branch}"
    ac = shared_persistent_arctic_client
    # There shouldn't be a library with this name present, so delete just in case
    ac.delete_library(library_to_write_to)
    ac.create_library(library_to_write_to)
    lib = ac[library_to_write_to]
    write_persistent_library(lib, latest=True)


@pytest.fixture(params=[pytest.param("REAL_S3", marks=REAL_S3_TESTS_MARK)])
def persistent_arctic_client(real_s3_storage, encoding_version):
    return real_s3_storage.create_arctic(encoding_version=encoding_version)


@pytest.mark.parametrize("num_rows", [1_000_000])
def test_persistent_storage_read_write_large_data_ascending(persistent_arctic_client, num_rows):
    ac = persistent_arctic_client
    ac.create_library("test_persistent_storage_read_write_large_data_ascending")
    lib = ac["test_persistent_storage_read_write_large_data_ascending"]

    sym = str(num_rows)
    orig_df = generate_ascending_dataframe(num_rows)
    lib.write(sym, orig_df)
    result_df = lib.read(sym).data
    assert_frame_equal(result_df, orig_df)


@pytest.mark.parametrize("num_rows", [100_000_000])
def test_persistent_storage_read_write_large_data_random(persistent_arctic_client, num_rows):
    ac = persistent_arctic_client
    ac.create_library("test_persistent_storage_read_write_large_data_random")
    lib = ac["test_persistent_storage_read_write_large_data_random"]

    sym = str(num_rows)
    orig_df = generate_pseudo_random_dataframe(num_rows)
    lib.write(sym, orig_df)
    result_df = lib.read(sym).data
    assert_frame_equal(result_df, orig_df)


@pytest.mark.parametrize("num_syms", [1_000])
def test_persistent_storage_read_write_many_syms(persistent_arctic_client, num_syms, three_col_df):
    # For now, this tests only the breadth (e.g. number of symbols)
    # We have another test, that tests with "deeper" data frames
    ac = persistent_arctic_client
    ac.create_library("test_persistent_storage_read_write_many_syms")
    lib = ac["test_persistent_storage_read_write_many_syms"]
    df = three_col_df()
    print(f"Writing {num_syms} symbols")
    payloads = [WritePayload(f"{sym}_sym", df) for sym in range(num_syms)]
    lib.write_batch(payloads)

    print(f"Reading {num_syms} symbols")
    read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_syms)]
    results = lib.read_batch(read_reqs)

    print(f"Comparing {num_syms} symbols")
    for res in results:
        result_df = res.data
        assert_frame_equal(result_df, df)
