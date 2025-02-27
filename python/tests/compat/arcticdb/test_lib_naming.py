import pytest
import sys
from arcticdb_ext.exceptions import UserInputException
from arcticdb.util.test import sample_dataframe


@pytest.mark.parametrize("prefix", ["", "prefix"])
@pytest.mark.parametrize("suffix", ["", "suffix"])
def test_create_library_with_all_chars(arctic_client_v1, prefix, suffix):
    ac = arctic_client_v1
    if sys.platform == "win32" and "lmdb" in ac.get_uri():
        pytest.skip(reason="Github actions runners run out of disk space on Windows in this test with lmdb")
    # Create library names with each character (except '\' because Azure replaces it with '/' in some cases)
    names = [f"{prefix}{chr(i)}{suffix}" for i in range(256) if chr(i) != "\\"]

    created_libraries = set()
    for name in names:
        try:
            ac.create_library(name)
            created_libraries.add(name)
        # We should only fail with UserInputException (indicating that name validation failed)
        except UserInputException:
            pass

    result = set(ac.list_libraries())
    assert all(name in result for name in created_libraries)
    for lib in created_libraries:
        ac.delete_library(lib)


@pytest.mark.parametrize("prefix", ["", "prefix"])
@pytest.mark.parametrize("suffix", ["", "suffix"])
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
