"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import numpy as np
import random


def test_symbol_stats(lmdb_version_store):
    num_symbols = 10
    symbol_to_df = {}
    initial_timestamp = pd.Timestamp("2019-01-01")
    for i in range(num_symbols):
        symbol = "symbol_{}".format(i)
        num_rows = random.randint(1, 50)
        date_range = pd.date_range(initial_timestamp, periods=num_rows)
        df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=date_range)
        symbol_to_df[symbol] = df
        lmdb_version_store.write(symbol, df)

    lmdb_version_store.refresh_symbol_stats_cache()
    vit = lmdb_version_store.get_symbol_stats()

    for sym in vit.data.index:
        df = symbol_to_df[sym]
        assert vit.data["start_time"][sym] == df.index[0]
        end_time = vit.data["end_time"][sym].floor("ms")
        assert end_time == df.index[-1]
        assert vit.data["num_rows"][sym] == len(df)
