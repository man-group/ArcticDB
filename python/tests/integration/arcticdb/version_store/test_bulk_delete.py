"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.storage import KeyType
from arcticdb.util.test import config_context, config_context_multi, query_stats_operation_count
import arcticdb.toolbox.query_stats as qs


def _write_versions(lib, symbol, num_versions):
    for i in range(num_versions):
        df = pd.DataFrame({"x": np.arange(i, i + 5, dtype=np.int64)})
        lib.write(symbol, df)


@pytest.mark.storage
def test_delete_removes_all_keys_when_chunked(object_and_mem_and_lmdb_version_store, sym):
    lib = object_and_mem_and_lmdb_version_store
    num_versions = 12
    batch_size = 5

    with config_context_multi({"S3Storage.DeleteBatchSize": batch_size, "AzureStorage.DeleteBatchSize": batch_size}):
        _write_versions(lib, sym, num_versions)

        lib_tool = lib.library_tool()
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, sym)) == num_versions
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, sym)) == num_versions

        lib.delete(sym)

        assert lib.has_symbol(sym) is False
        assert lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, sym) == []
        assert lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, sym) == []


def test_bulk_delete_issues_one_storage_op_per_batch(s3_version_store_v1, sym, clear_query_stats):
    lib = s3_version_store_v1
    num_versions = 12
    batch_size = 5
    expected_batches_per_key_type = 3

    with config_context("S3Storage.DeleteBatchSize", batch_size):
        _write_versions(lib, sym, num_versions)

        qs.enable()
        qs.reset_stats()
        lib.delete(sym)
        stats = qs.get_query_stats()

    assert query_stats_operation_count(stats, "S3_DeleteObjects", "TABLE_INDEX") == expected_batches_per_key_type
    assert query_stats_operation_count(stats, "S3_DeleteObjects", "TABLE_DATA") == expected_batches_per_key_type
    assert query_stats_operation_count(stats, "S3_DeleteObjects", "COLUMN_STATS") == expected_batches_per_key_type
