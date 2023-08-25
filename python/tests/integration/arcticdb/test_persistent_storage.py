import pytest
import os
import pandas as pd
from arcticdb.arctic import Arctic
from tests.conftest import PERSISTENT_STORAGE_TESTS_ENABLED

PERSISTENT_STORAGE_LIB_NAME = os.getenv("ARCTICDB_PERSISTENT_STORAGE_LIB_NAME")
UNIQUE_ID = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_ID")

if PERSISTENT_STORAGE_TESTS_ENABLED:
    # TODO: Maybe add a way to parametrize this
    LIBRARIES = [
        # LINUX
        "linux_cp36",
        "linux_cp37",
        "linux_cp38",
        "linux_cp39",
        "linux_cp310",
        "linux_cp311",
        # WINDOWS
        "windows_cp37",
        "windows_cp38",
        "windows_cp39",
        "windows_cp310",
        "windows_cp311",
    ]
else:
    LIBRARIES = []


def normalize_lib_name(lib_name):
    lib_name = lib_name.replace(".", "_")
    lib_name = lib_name.replace("-", "_")

    return lib_name


# TODO: Add a check if the real storage tests are enabled
@pytest.mark.parametrize("library", LIBRARIES)
@pytest.mark.skipif(
    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="This test should run only if the persistent storage tests are enabled"
)
def test_real_s3_storage_read(real_s3_uri, library):
    ac = Arctic(real_s3_uri)
    lib_name = f"seed_{UNIQUE_ID}_{library}"
    lib_name = normalize_lib_name(lib_name)
    lib = ac[lib_name]
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
def test_real_s3_storage_write(real_s3_uri, three_col_df):
    library_to_write_to = PERSISTENT_STORAGE_LIB_NAME
    library_to_write_to = normalize_lib_name(library_to_write_to)
    ac = Arctic(real_s3_uri)
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
    # TODO: assert three_df.equals(val.data)
