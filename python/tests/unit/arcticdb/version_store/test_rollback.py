"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.util.test import config_context, config_context_string
from arcticdb import col, concat, LazyDataFrame, LazyDataFrameCollection, QueryBuilder, ReadRequest
from arcticdb.exceptions import NoSuchVersionException, SchemaException
from arcticdb.options import LibraryOptions
from arcticdb.util.test import assert_frame_equal, assert_series_equal
from tests.util.mark import MACOS_WHEEL_BUILD, WINDOWS


def test_write_with_quota_exception(lmdb_library_factory, wrapped_s3_storage_bucket, lib_name):
    lib = wrapped_s3_storage_bucket.create_version_store_factory(lib_name)()
    index = pd.date_range("2025-01-01", periods=3)
    df = pd.DataFrame(
        {
            "col1": np.arange(3, dtype=np.int64),
            "col2": np.arange(100, 103, dtype=np.int64),
            "col3": np.arange(1000, 1003, dtype=np.int64),
        },
        index=index,
    )


    with config_context("S3ClientTestWrapper.EnableFailures", 1):
        with config_context("S3ClientTestWrapper.ErrorCode", 100):
            with config_context_string("S3ClientTestWrapper.ExceptionName", "QuotaExceeded"):

    lib = lmdb_library_factory()
    lib.write("sym0", df)
