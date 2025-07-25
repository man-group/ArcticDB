from typing import Generator
import pytest
import os
from arcticdb.arctic import Arctic, Library
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.utils import get_logger
from tests.conftest import (
    real_gcp_storage, 
    real_gcp_storage_without_clean_up, 
    real_s3_storage, 
    real_s3_storage_without_clean_up,
    real_azure_storage,
    real_azure_storage_without_clean_up,
)
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


logger = get_logger("persistant_tests")

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
        elif persistent_test_type() == PersistentTestType.AZURE:
            return request.getfixturevalue(real_azure_storage_without_clean_up.__name__).create_arctic()
    except Exception as e:
        print(e)
        pytest.skip("No persistence tests selected or error during configuration.")

# TODO: Add a check if the real storage tests are enabled
@pytest.mark.parametrize("library", LIBRARIES)
@pytest.mark.storage
def test_real_storage_read(shared_persistent_arctic_client, library):
    ac = shared_persistent_arctic_client
    lib = ac[library]
    logger.info(f"Storage type: {persistent_test_type()}")
    read_persistent_library(lib)


@pytest.mark.storage
def test_real_storage_write(shared_persistent_arctic_client):
    strategy_branch = os.getenv("ARCTICDB_PERSISTENT_STORAGE_STRATEGY_BRANCH")
    library_to_write_to = f"test_{strategy_branch}"
    logger.info(f"Storage type: {persistent_test_type()}")
    ac = shared_persistent_arctic_client
    # There shouldn't be a library with this name present, so delete just in case
    ac.delete_library(library_to_write_to)
    ac.create_library(library_to_write_to)
    lib = ac[library_to_write_to]
    write_persistent_library(lib, latest=True)


@pytest.fixture
def persistent_arctic_library(request, encoding_version, lib_name) -> Generator[Library, None, None]:
    try:
        if persistent_test_type() == PersistentTestType.GCP:
            ac: Arctic = request.getfixturevalue(real_gcp_storage.__name__).create_arctic(encoding_version=encoding_version)
        elif persistent_test_type() == PersistentTestType.AWS_S3:
            ac: Arctic =  request.getfixturevalue(real_s3_storage.__name__).create_arctic(encoding_version=encoding_version)
        elif persistent_test_type() == PersistentTestType.AZURE:
            ac: Arctic =  request.getfixturevalue(real_azure_storage.__name__).create_arctic(encoding_version=encoding_version)
    except Exception as e:
        logger.info("An error occurred", exc_info=True)
        pytest.skip("No persistence tests selected or error during configuration.")   
    
    lib: Library = ac.create_library(lib_name)
    yield lib
    ac.delete_library(lib_name)


@pytest.mark.parametrize("num_rows", [1_000_000])
@pytest.mark.storage
def test_persistent_storage_read_write_large_data_ascending(persistent_arctic_library, num_rows):
    lib = persistent_arctic_library

    logger.info(f"Storage type: {persistent_test_type()}")
    sym = str(num_rows)
    orig_df = generate_ascending_dataframe(num_rows)
    logger.info(f"Generation complete for {num_rows} rows")
    lib.write(sym, orig_df)
    logger.info(f"Write complete")
    result_df = lib.read(sym).data
    logger.info(f"Read complete")
    assert_frame_equal(result_df, orig_df)
    logger.info(f"Comparison complete")


@pytest.mark.parametrize("num_rows", [100_000_000])
@pytest.mark.storage
def test_persistent_storage_read_write_large_data_random(persistent_arctic_library, num_rows):
    lib = persistent_arctic_library

    sym = str(num_rows)
    orig_df = generate_pseudo_random_dataframe(num_rows)
    logger.info(f"Generation complete for {num_rows} rows")
    lib.write(sym, orig_df)
    logger.info(f"Write complete")
    result_df = lib.read(sym).data
    logger.info(f"Read complete")
    assert_frame_equal(result_df, orig_df)
    logger.info(f"Comparison complete")


@pytest.mark.parametrize("num_syms", [1_000])
@pytest.mark.storage
def test_persistent_storage_read_write_many_syms(persistent_arctic_library, num_syms, three_col_df):
    # For now, this tests only the breadth (e.g. number of symbols)
    # We have another test, that tests with "deeper" data frames
    lib = persistent_arctic_library
    df = three_col_df()
    logger.info(f"Writing {num_syms} symbols")
    payloads = [WritePayload(f"{sym}_sym", df) for sym in range(num_syms)]
    lib.write_batch(payloads)

    logger.info(f"Reading {num_syms} symbols")
    read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_syms)]
    results = lib.read_batch(read_reqs)

    logger.info(f"Comparing {num_syms} symbols")
    for res in results:
        result_df = res.data
        assert_frame_equal(result_df, df)
