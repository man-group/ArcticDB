from arcticdb import Arctic
import pandas as pd
import os
import numpy as np

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

def real_s3_credentials():
    endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
    region = os.getenv("ARCTICDB_REAL_S3_REGION")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    clear = True if str(os.getenv("ARCTICDB_REAL_S3_CLEAR")).lower() in ["true", "1"] else False
    
    return endpoint, bucket, region, access_key, secret_key, clear

def get_real_s3_uri():
    endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials()
    uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix=ci_tests/"
    return uri

def test_df_3_cols(start=0):
    return pd.DataFrame(
        {
            "x": np.arange(start, start + 10, dtype=np.int64),
            "y": np.arange(start + 10, start + 20, dtype=np.int64),
            "z": np.arange(start + 20, start + 30, dtype=np.int64),
        },
        index=np.arange(start, start + 10, dtype=np.int64),
    )
