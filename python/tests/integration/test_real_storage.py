import pytest
import os
from arcticdb.arctic import Arctic


REAL_STORAGE_TESTS_ENABLED = True if os.getenv("ARCTICDB_REAL_STORAGE_TESTS") == "1" else False

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
    uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}"
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
