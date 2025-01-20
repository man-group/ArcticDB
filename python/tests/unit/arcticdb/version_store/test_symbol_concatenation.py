"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb import col, LazyDataFrame, LazyDataFrameCollection, QueryBuilder, ReadRequest
from arcticdb.util.test import assert_frame_equal


pytestmark = pytest.mark.pipeline


def test_symbol_concat_basic(lmdb_library):
    lib = lmdb_library
    df_1 = pd.DataFrame({"col1": np.arange(2, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=2))
    df_2 = pd.DataFrame({"col1": np.arange(3, dtype=np.int64)}, index=pd.date_range("2025-02-01", periods=3))
    df_3 = pd.DataFrame({"col1": np.arange(4, dtype=np.int64)}, index=pd.date_range("2025-03-01", periods=4))
    lib.write("sym1", df_1)
    lib.write("sym2", df_2)
    lib.write("sym3", df_3)

    q = QueryBuilder()
    q = q.concat()
    received = lib.read_batch(["sym2", "sym3", "sym1"], query_builder=q).data
    expected = pd.concat([df_1, df_2, df_3])
    assert_frame_equal(expected, received.data)

