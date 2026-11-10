"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from arcticdb.exceptions import ArcticNativeException

#
# from arcticdb import QueryBuilder
# from arcticdb.exceptions import InternalException, NoSuchVersionException, SchemaException, UserInputException
# from arcticdb.options import OutputFormat
# import arcticdb.toolbox.query_stats as qs
# from arcticdb.util.test import assert_frame_equal, assert_pandas_equal, query_stats_operation_count
# from arcticdb.version_store import TimeFrame
# from arcticdb_ext.storage import KeyType


def test_arrow_streaming_reads_basic(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_basic"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    metadata = {"hello": "world"}
    lib.write(sym, df, metadata=metadata)
    gen = lib.read_streaming(sym, prefetch_rows=20)
    for idx, df_slice in enumerate(gen):
        assert df_slice.equals(df.slice(idx * 10, 10))
    vit = lib.read(sym)
    vit.data = None
    assert vit == gen.versioned_item()


def test_arrow_streaming_reads_col_slicing(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10, column_group_size=2)
    sym = "test_arrow_streaming_reads_col_slicing"
    df = pl.from_pandas(pd.DataFrame({f"col_{i}": np.arange(i * 20, (i + 1) * 20) for i in range(5)}))
    lib.write(sym, df)
    gen = lib.read_streaming(sym)
    for idx, df_slice in enumerate(gen):
        assert df_slice.equals(df.slice(idx * 10, 10))
    vit = lib.read(sym)
    vit.data = None
    assert vit == gen.versioned_item()


@pytest.mark.parametrize("prefetch_rows", [-5, "not a number"])
def test_arrow_streaming_reads_invalid_prefetch_rows(in_memory_version_store, prefetch_rows):
    lib = in_memory_version_store
    sym = "test_arrow_streaming_reads_invalid_prefetch_rows"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(1)}))
    lib.write(sym, df)
    with pytest.raises(ArcticNativeException if isinstance(prefetch_rows, int) else TypeError):
        lib.read_streaming(sym, prefetch_rows=prefetch_rows)
