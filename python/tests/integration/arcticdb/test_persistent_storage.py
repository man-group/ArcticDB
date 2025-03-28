import time
import pytest
import os
from arcticdb.arctic import Arctic
from arcticdb.storage_fixtures.api import StorageFixture
from arcticdb.util.test import assert_frame_equal
from tests.conftest import real_gcp_storage, real_gcp_storage_without_clean_up, real_s3_storage, real_s3_storage_without_clean_up
from tests.util.mark import PERSISTENT_STORAGE_TESTS_ENABLED
from tests.util.storage_test import (
    PersistentTestType,
    get_seed_libraries,
    generate_pseudo_random_dataframe,
    generate_ascending_dataframe,
    persistent_test_type,
    read_persistent_library,
    write_persistent_library,
)
from arcticdb.version_store.library import WritePayload, ReadRequest

if PERSISTENT_STORAGE_TESTS_ENABLED:
    LIBRARIES = get_seed_libraries()
else:
    LIBRARIES = []


# Only test with encoding version 0 (a.k.a.) for now
# because there is a problem when older versions try to read configs with a written encoding version
# def shared_persistent_arctic_client(real_s3_storage_without_clean_up, encoding_version):
@pytest.fixture
def shared_persistent_arctic_client(request):
    try:
        if persistent_test_type() == PersistentTestType.GCP:
            return request.getfixturevalue(real_gcp_storage_without_clean_up.__name__).create_arctic()
        elif persistent_test_type() == PersistentTestType.AWS_S3:
            return request.getfixturevalue(real_s3_storage_without_clean_up.__name__).create_arctic()
    except Exception as e:
        print(e)
        pytest.skip("No persistence tests selected or error during configuration.")

# TODO: Add a check if the real storage tests are enabled
@pytest.mark.parametrize("library", LIBRARIES)
@pytest.mark.storage
def test_real_storage_read(shared_persistent_arctic_client, library):
    ac = shared_persistent_arctic_client
    lib = ac[library]
    print(f"Storage type: {persistent_test_type()}")
    read_persistent_library(lib)


@pytest.mark.storage
def test_real_storage_write(shared_persistent_arctic_client):
    strategy_branch = os.getenv("ARCTICDB_PERSISTENT_STORAGE_STRATEGY_BRANCH")
    library_to_write_to = f"test_{strategy_branch}"
    print(f"Storage type: {persistent_test_type()}")
    ac = shared_persistent_arctic_client
    # There shouldn't be a library with this name present, so delete just in case
    ac.delete_library(library_to_write_to)
    ac.create_library(library_to_write_to)
    lib = ac[library_to_write_to]
    write_persistent_library(lib, latest=True)


@pytest.fixture
def persistent_arctic_client(request, encoding_version):
    try:
        if persistent_test_type() == PersistentTestType.GCP:
            return request.getfixturevalue(real_gcp_storage.__name__).create_arctic(encoding_version=encoding_version)
        elif persistent_test_type() == PersistentTestType.AWS_S3:
            return request.getfixturevalue(real_s3_storage.__name__).create_arctic(encoding_version=encoding_version)
    except Exception as e:
        pytest.skip("No persistence tests selected or error during configuration.")   


@pytest.mark.parametrize("num_rows", [1_000_000])
@pytest.mark.storage
def test_persistent_storage_read_write_large_data_ascending(persistent_arctic_client, num_rows):
    ac: Arctic = persistent_arctic_client
    lib_name = "test_persistent_storage_read_write_large_data_ascending1"
    lib = ac.create_library(lib_name)

    print(f"Storage type: {persistent_test_type()}")
    sym = str(num_rows)
    orig_df = generate_ascending_dataframe(num_rows)
    print(f"Generation complete for {num_rows} rows")
    lib.write(sym, orig_df)
    print(f"Write complete")
    result_df = lib.read(sym).data
    print(f"Read complete")
    assert_frame_equal(result_df, orig_df)
    print(f"Comparison complete")
    ac.delete_library(lib_name)
    print(f"Delete complete")


@pytest.mark.parametrize("num_rows", [100_000_000])
@pytest.mark.storage
def test_persistent_storage_read_write_large_data_random(persistent_arctic_client, num_rows):
    ac = persistent_arctic_client
    lib_name = "test_persistent_storage_read_write_large_data_random"
    print(f"Storage type: {persistent_test_type()}")
    ac.create_library(lib_name)
    lib = ac[lib_name]

    sym = str(num_rows)
    orig_df = generate_pseudo_random_dataframe(num_rows)
    print(f"Generation complete for {num_rows} rows")
    lib.write(sym, orig_df)
    print(f"Write complete")
    result_df = lib.read(sym).data
    print(f"Read complete")
    assert_frame_equal(result_df, orig_df)
    print(f"Comparison complete")
    ac.delete_library(lib_name)
    print(f"Delete complete")


@pytest.mark.parametrize("num_syms", [1_000])
@pytest.mark.storage
def test_persistent_storage_read_write_many_syms(persistent_arctic_client, num_syms, three_col_df):
    # For now, this tests only the breadth (e.g. number of symbols)
    # We have another test, that tests with "deeper" data frames
    ac: Arctic = persistent_arctic_client

    lib_name = "test_persistent_storage_read_write_many_syms"
    print(f"Storage type: {persistent_test_type()}")
    ac.create_library(lib_name)
    lib = ac[lib_name]
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

    ac.delete_library(lib_name)
