"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import polars as pl

# import pyarrow as pa
# import pytest
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
    lib._set_allow_arrow_input()
    sym = "test_arrow_streaming_reads_basic"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    metadata = {"hello": "world"}
    lib.write(sym, df, metadata=metadata)
    gen = lib.read_streaming(sym)
    for idx, df_slice in enumerate(gen):
        expected = df.slice(idx * 10, 10)
        assert df_slice.equals(expected)
    vit = lib.read(sym)
    vit.data = None
    assert vit == gen.versioned_item()
