"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import os
from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder

from .common import *


# We keep these functions here to make sure that every version of the benchmark
# has access to the same functions for creating the AWS url and the real S3 credentials
def real_s3_credentials(shared_path: bool = False):
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


def get_real_s3_uri(shared_path: bool = False):
    (
        endpoint,
        bucket,
        region,
        access_key,
        secret_key,
        path_prefix,
        _,
    ) = real_s3_credentials(shared_path)
    aws_uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix={path_prefix}"
    return aws_uri


class PersistentQueryBuilderFunctions:
    number = 2
    timeout = 6000
    LIB_NAME = "query_builder_benchmark_lib"

    params = [1_000_000, 10_000_000]

    def __init__(self):
        self.ac = Arctic(get_real_s3_uri())

        self.lib_name = "query_builder_benchmark_lib"

    def setup(self, num_rows):
        self.lib = self.ac[PersistentQueryBuilderFunctions.LIB_NAME]

    def setup_cache(self):
        self.ac = Arctic(get_real_s3_uri())

        num_rows = PersistentQueryBuilderFunctions.params
        self.lib_name = PersistentQueryBuilderFunctions.LIB_NAME
        self.ac.delete_library(self.lib_name)
        lib = self.ac.create_library(self.lib_name)
        for rows in num_rows:
            lib.write(f"{rows}_rows", generate_benchmark_df(rows))

    # The names are based on the queries used here: https://duckdblabs.github.io/db-benchmark/
    def time_query_1(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_3(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_4(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_adv_query_2(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)
