from arcticdb import Arctic
import pandas as pd
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


endpoint, bucket, region, access_key, secret_key, clear = real_s3_credentials()
uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}"

print(f"Connecting to {uri}")

ac = Arctic(uri)
lib_name = sys.argv[1]
test_df = pd.DataFrame()
if lib_name not in ac.list_libraries():
    ac.create_library(lib_name)

library = ac[lib_name]
library.write("test", test_df)

if clear:
    ac.delete_library(lib_name)
