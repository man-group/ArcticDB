from arcticdb import Arctic
import pandas as pd
import numpy as np
import os 
import sys

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

def test_write(lib, symbol, df):
    lib.write(symbol, df)

def test_append(lib, symbol, df, write_if_missing=True):
    lib.append(symbol, df, write_if_missing=write_if_missing)

endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials()
uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}"

print(f"Connecting to {uri}")

ac = Arctic(uri)
lib_name = sys.argv[1]

# TODO: Add some validation of the library, if it is there
if lib_name not in ac.list_libraries():
    ac.create_library(lib_name)
    
library = ac[lib_name]

one_df = test_df_3_cols()
test_write(library, "one", one_df)

two_df = test_df_3_cols(1)
test_write(library, "two", two_df)
two_df = test_df_3_cols(2)
test_append(library, "two", two_df)

three_df = test_df_3_cols(3)
test_append(library, "three", three_df)

if clear:
    ac.delete_library(lib_name)
