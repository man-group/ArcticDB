from arcticdb import Arctic
import pandas as pd
import numpy as np
import os 
import sys

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

def real_s3_credentials():
    endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
    region = os.getenv("ARCTICDB_REAL_S3_REGION")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    clear = True if str(os.getenv("ARCTICDB_REAL_S3_CLEAR")).lower() in ["true", "1"] else False
    
    return endpoint, bucket, region, access_key, secret_key, clear

def test_df_3_cols(start=0):
    return pd.DataFrame(
        {
            "x": np.arange(start, start + 10, dtype=np.int64),
            "y": np.arange(start + 10, start + 20, dtype=np.int64),
            "z": np.arange(start + 20, start + 30, dtype=np.int64),
        },
        index=np.arange(start, start + 10, dtype=np.int64),
    )
# TODO: Add support for other storages
endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials()
uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix=ci_tests/"

print(f"Connecting to {uri}")

ac = Arctic(uri)
branch_name = sys.argv[1]
for lib in LIBRARIES:
    lib_name = f"{branch_name}_{lib}"
    lib = ac[lib_name]
    print(lib_name)

    symbols = lib.list_symbols()
    print(symbols)
    assert len(symbols) == 3
    for sym in ["one", "two", "three"]:
        assert sym in symbols
    for sym in symbols:
        df = lib.read(sym).data
        column_names = df.columns.values.tolist()
        assert column_names == ["x", "y", "z"]

    ac.delete_library(lib_name)
