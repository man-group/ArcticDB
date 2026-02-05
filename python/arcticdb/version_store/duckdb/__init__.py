"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

from arcticdb.version_store.duckdb.arrow_reader import ArcticRecordBatchReader
from arcticdb.version_store.duckdb.duckdb import DuckDBContext

__all__ = ["DuckDBContext"]
