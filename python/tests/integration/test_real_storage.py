import pytest
from arcticdb.arctic import Arctic


# TODO: Add a check if the real storage tests are enabled
def test_real_s3_storage_read(real_s3_credentials):
    endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials()
    uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}"

    ac = Arctic(uri)
    libs = ac.list_libraries()
    assert len(libs) > 0

    for lib in libs:
        symbols = lib.list_symbols()
        assert len(symbols) > 0
        for sym in symbols:
            print(sym)
            print(lib.read(sym).data)
