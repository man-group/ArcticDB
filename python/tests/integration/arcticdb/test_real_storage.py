import pytest
import os
import pandas as pd
from arcticdb.arctic import Arctic

REAL_STORAGE_TESTS_ENABLED = True if os.getenv("ARCTICDB_REAL_STORAGE_TESTS") == "1" else False
REAL_STORAGE_LIB_NAME = os.getenv("ARCTICDB_REAL_STORAGE_LIB_NAME")


if REAL_STORAGE_TESTS_ENABLED:
    # TODO: Maybe add a way to parametrize this
    LIBRARIES = [
        # LINUX
        "linux_3_6",
        "linux_3_7",
        "linux_3_8",
        "linux_3_9",
        "linux_3_10",
        "linux_3_11",
        # WINDOWS
        "windows_3_7",
        "windows_3_8",
        "windows_3_9",
        "windows_3_10",
        "windows_3_11",
    ]
else:
    LIBRARIES = []


# TODO: Add a check if the real storage tests are enabled
@pytest.mark.parametrize("library", LIBRARIES)
def test_real_s3_storage_read(real_s3_credentials, library):
    endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials
    uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix=ci_tests/"
    ac = Arctic(uri)
    lib = ac[library]
    symbols = lib.list_symbols()
    assert len(symbols) == 3
    for sym in ["one", "two", "three"]:
        assert sym in symbols
    for sym in symbols:
        df = lib.read(sym).data
        column_names = df.columns.values.tolist()
        assert column_names == ["x", "y", "z"]


def test_real_s3_storage_write(real_s3_credentials, three_col_df):
    library_to_write_to = REAL_STORAGE_LIB_NAME
    endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials
    uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix=ci_tests/"
    ac = Arctic(uri)
    # There shouldn't be a library with this name present, so delete just in case
    ac.delete_library(library_to_write_to)
    ac.create_library(library_to_write_to)
    lib = ac[library_to_write_to]
    one_df = three_col_df()
    lib.write("one", one_df)
    val = lib.read("one")
    print(one_df)
    print(val.data)
    assert one_df.equals(val.data)

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
    assert three_df.equals(val.data)
