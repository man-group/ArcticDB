"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
import numpy as np
import os

def real_s3_credentials(shared_path: bool = True):
    endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
    region = os.getenv("ARCTICDB_REAL_S3_REGION")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    if shared_path:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX")

    clear = str(os.getenv("ARCTICDB_REAL_S3_CLEAR")).lower() in ("true", "1")

    return endpoint, bucket, region, access_key, secret_key, path_prefix, clear


def get_real_s3_uri(shared_path: bool = True):
    endpoint, bucket, region, access_key, secret_key, path_prefix, _ = real_s3_credentials(shared_path)
    aws_uri = (
        f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix={path_prefix}"
    )
    return aws_uri

def generate_pseudo_random_dataframe(n, freq="S", end_timestamp="1/1/2023"):
    """
    Generates a Data Frame with 2 columns (timestamp and value) and N rows
    - timestamp contains timestamps with a given frequency that end at end_timestamp
    - value contains random floats that sum up to approximately N, for easier testing/verifying
    """
    # Generate random values such that their sum is equal to N
    values = np.random.uniform(0, 2, size=n)
    # Generate timestamps
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq=freq)
    # Create dataframe
    df = pd.DataFrame({"timestamp": timestamps, "value": values})
    return df

def generate_benchmark_df(N, K=100, freq="S", end_timestamp="1/1/2023"):
    timestamps = pd.date_range(end=end_timestamp, periods=N, freq=freq)
    
    # Based on https://github.com/duckdblabs/db-benchmark/blob/master/_data/groupby-datagen.R#L19
    DT = pd.DataFrame()
    DT["id1"] = np.random.choice([f'id{str(i).zfill(3)}' for i in range(1, K+1)], N)
    DT["id2"] = np.random.choice([f'id{str(i).zfill(3)}' for i in range(1, K+1)], N)
    DT["id3"] = np.random.choice([f'id{str(i).zfill(10)}' for i in range(1, N//K + 1)], N)
    DT["id4"] = np.random.choice(range(1, K+1), N)
    DT["id5"] = np.random.choice(range(1, K+1), N)
    DT["id6"] = np.random.choice(range(1, N//K + 1), N)
    DT["v1"] = np.random.choice(range(1, 6), N)
    DT["v2"] = np.random.choice(range(1, 16), N)
    DT["v3"] = np.round(np.random.uniform(0, 100, N), 6)

    assert len(timestamps) == len(DT)

    DT.index = timestamps

    return DT

def get_prewritten_lib_name(rows):
    return f"prewritten_{rows}"
