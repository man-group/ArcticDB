"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb import col, concat, LazyDataFrame, LazyDataFrameCollection, QueryBuilder, ReadRequest
from arcticdb.options import LibraryOptions
from arcticdb.util.test import assert_frame_equal

pytestmark = pytest.mark.pipeline


@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
def test_symbol_concat_basic(lmdb_library_factory, rows_per_segment, columns_per_segment):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    df_1 = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=pd.date_range(pd.Timestamp(0), freq="1000ns", periods=3))
    df_2 = pd.DataFrame({"col": np.arange(4, dtype=np.int64)}, index=pd.date_range(pd.Timestamp(2000), freq="1000ns", periods=4))
    df_3 = pd.DataFrame({"col": np.arange(5, dtype=np.int64)}, index=pd.date_range(pd.Timestamp(6000), freq="1000ns", periods=5))
    lib.write("sym1", df_1)
    lib.write("sym2", df_2)
    lib.write("sym3", df_3)

    lazy_df_1 = lib.read("sym1", lazy=True)
    lazy_df_2 = lib.read("sym2", lazy=True)
    lazy_df_2 = lazy_df_2.date_range((pd.Timestamp(pd.Timestamp(3000)), None))
    lazy_df_3 = lib.read("sym3", lazy=True)

    lazy_df = concat([lazy_df_1, lazy_df_2, lazy_df_3])

    lazy_df.resample("2000ns").agg({"col": "sum"})

    received = lazy_df.collect().data
    expected = pd.concat([df_1, df_2.iloc[1:], df_3]).resample("2000ns").agg({"col": "sum"})
    assert_frame_equal(expected, received)


