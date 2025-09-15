"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from arcticdb import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.util.environment_setup import AWS_S3_DEFAULT_BUCKET
from arcticdb.util.logger import get_logger
from arcticdb.version_store.processing import QueryBuilder


LIB_NAME = "TEST_STATS"
SYMBOL = "FLAKY"
ARCTIC_LMDB_URI = "lmdb:///tmp/repo_fails"  


def get_aws_uri():
    endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    region = os.getenv("ARCTICDB_REAL_S3_REGION")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    bucket = AWS_S3_DEFAULT_BUCKET # Will reuse ASV bucket with special PREFIX
    path_prefix = LIB_NAME
    aws_uri = (
        f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix={path_prefix}"
    )
    return aws_uri


ARCTIC_URI = get_aws_uri()

def store_test_stats(failures, flaky_recoveries):
    """
    Store test results into ArcticDB.
    failures: dict[test_name -> output_str]
    flaky_recoveries: set[test_name]
    """

    # GitHub Actions metadata
    gha_run_id = os.getenv("GITHUB_RUN_ID")
    gha_repo = os.getenv("GITHUB_REPOSITORY")
    gha_workflow = os.getenv("GITHUB_WORKFLOW")
    gha_url = (
        f"https://github.com/{gha_repo}/actions/runs/{gha_run_id}"
        if gha_run_id and gha_repo else None
    )

    # Build records
    ts = datetime.utcnow()
    records = []

    for test, output in failures.items():
        if test in flaky_recoveries:
            status = "RECOVERED"
        else:
            status = "FAILED"
        records.append({
            "timestamp": ts,
            "test": test,
            "status": status,
            "workflow": gha_workflow,
            "run_id": gha_run_id,
            "gha_url": gha_url,
            "output": output
        })

    if not records:
        print("No test data to store.")
        return

    df = pd.DataFrame(records).set_index("timestamp")

    ac = Arctic(ARCTIC_URI)
    if LIB_NAME not in ac.list_libraries():
        ac.create_library(LIB_NAME, library_options=LibraryOptions(rows_per_segment=20)) 

    lib = ac[LIB_NAME]
    lib.append(SYMBOL, df)

    get_logger().info(f"✅ Stored {len(df)} records in ArcticDB library '{LIB_NAME}', symbol '{SYMBOL}'")

def read_stats():
    """
    Print the flaky results output
    """
    ac = Arctic(ARCTIC_URI)
    lib = ac[LIB_NAME]
    df = lib.read(SYMBOL).data
    print(df)
    #print(df.iloc[-1, df.columns.get_loc("output")])

def read_stats():
    """
    Print the flaky results output
    """
    ac = Arctic(ARCTIC_URI)
    lib = ac[LIB_NAME]
    df = lib.read(SYMBOL).data
    print(df)

def read_last7_days_data():
    ac = Arctic(ARCTIC_URI)
    lib = ac[LIB_NAME]
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=7)
    qb = QueryBuilder()
    qb = qb.date_range((pd.Timestamp(start_time), pd.Timestamp(end_time)))
    df_last_7 = lib.read(SYMBOL, query_builder=qb).data
    print(df_last_7)

