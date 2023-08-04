from arcticdb import Arctic
import pandas as pd
import numpy as np
import os 
import sys

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

# TODO: Add support for other storages
endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials()
uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix=ci_tests/"

print(f"Connecting to {uri}")

ac = Arctic(uri)
branch_name = sys.argv[1]
for lib in LIBRARIES:
    lib_name = f"{branch_name}_{lib}"

    ac.delete_library(lib_name)
