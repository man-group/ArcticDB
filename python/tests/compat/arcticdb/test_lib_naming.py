import pytest
import sys
from arcticdb.util.utils import get_logger
from arcticdb_ext.exceptions import UserInputException
from arcticdb.util.test import sample_dataframe
from tests.util.mark import SLOW_TESTS_MARK


@SLOW_TESTS_MARK
@pytest.mark.parametrize("prefix", ["", "prefix"])
@pytest.mark.parametrize("suffix", ["", "suffix"])
@pytest.mark.storage
@pytest.mark.skip_fixture_params(["real_gcp"], "Skipped because of issues with lib names containing \\n and \\r")
def test_create_library_with_all_chars(arctic_client_v1, prefix, suffix):
    logger = get_logger("test_create_library_with_all_chars")
    ac = arctic_client_v1
    if sys.platform == "win32" and "lmdb" in ac.get_uri():
        pytest.skip(reason="Github actions runners run out of disk space on Windows in this test with lmdb")
    # Create library names with each character (except '\' because Azure replaces it with '/' in some cases)
    names = [f"{prefix}{chr(i)}{suffix}" for i in range(256) if chr(i) != "\\"]

    failed = False
    created_libraries = set()
    try:
        for cnt, name in enumerate(names):
            logger.info(f"Iteration: {cnt}/{len(names)}")
            try:
                ac.create_library(name)
                created_libraries.add(name)
                logger.info(f"added lib with name: {repr(name)}")
            # We should only fail with UserInputException (indicating that name validation failed)
            except UserInputException:
                logger.info(f"exception handled UserInput exception added lib with name: {repr(name)}")
            except Exception:
                logger.info(f"!!!!!! FAILED with name: {repr(name)}")

        result = set(ac.list_libraries())
        assert all(name in result for name in created_libraries)
    finally:
        logger.info("Delete started")
        for cnt, lib in enumerate(created_libraries):
            logger.info(f"Deletion: {cnt}/{len(created_libraries)} lib_name [{repr(lib)}] ")
            ac.delete_library(lib)
        logger.info("Delete ended")

    assert not failed, "There is at least one failure look at the result"

@SLOW_TESTS_MARK
@pytest.mark.parametrize("prefix", ["", "prefix"])
@pytest.mark.parametrize("suffix", ["", "suffix"])
@pytest.mark.storage
def test_symbol_names_with_all_chars(object_version_store, prefix, suffix):
    # Create symbol names with each character (except '\' because Azure replaces it with '/' in some cases)
    names = [f"{prefix}{chr(i)}{suffix}" for i in range(256) if chr(i) != "\\"]
    df = sample_dataframe()

    written_symbols = set()
    for name in names:
        try:
            object_version_store.write(name, df)
            written_symbols.add(name)
        # We should only fail with UserInputException (indicating that name validation failed)
        except UserInputException:
            pass

    assert set(object_version_store.list_symbols()) == written_symbols
