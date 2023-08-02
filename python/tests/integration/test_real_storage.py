import pytest
from arcticdb.arctic import Arctic


def list_libraries():
    OSes = ["linux"]
    py_vers = ["3_6", "3_7", "3_8", "3_9", "3_10", "3_11"]
    libs = list()
    for os in OSes:
        for ver in py_vers:
            libs.append(f"{os}_{ver}")
    return libs


LIBRARIES = list_libraries()


# TODO: Add a check if the real storage tests are enabled
@pytest.mark.parametrize("library", LIBRARIES)
def test_real_s3_storage_read(real_s3_credentials, library):
    endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials
    uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}"
    ac = Arctic(uri)
    lib = ac[library]
    symbols = lib.list_symbols()
    assert len(symbols) > 0
    for sym in symbols:
        print(sym)
        print(lib.read(sym).data)
