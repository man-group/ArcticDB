"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pytest
import pandas as pd
import random

from arcticdb.util.test import assert_frame_equal


@pytest.mark.parametrize(
    ["colnum", "rownum", "initial_col_width", "max_col_width", "total_rows"],
    [(10, 5, 20, 500, 20), (10, 5, 450, 500, 20), (10, 5, 200, 1000, 20), (128, 100000, 30000, 40000, 4)],
)
def test_dynamic_bucketize_append_variable_width(
    get_wide_df, sym, basic_store_factory, colnum, rownum, initial_col_width, max_col_width, total_rows
):
    symbol = sym
    lib = basic_store_factory(
        column_group_size=colnum,
        segment_row_size=rownum,
        dynamic_schema=True,
        bucketize_dynamic=True,
        dynamic_strings=True,
        lmdb_config={"map_size": 2**30},
    )
    count = 0
    df1 = get_wide_df(count, initial_col_width, max_col_width)
    count += 1
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)
    while count < total_rows:
        df2 = get_wide_df(count, random.randrange(max_col_width) + 1, max_col_width)
        df1 = pd.concat([df1, df2])
        lib.append(symbol, df2)
        count += 1
    res = lib.read(symbol).data
    df1 = df1.reindex(sorted(list(df1.columns)), axis=1)
    res = res.reindex(sorted(list(res.columns)), axis=1)
    assert_frame_equal(res, df1)
