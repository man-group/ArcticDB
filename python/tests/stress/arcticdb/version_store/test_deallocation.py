"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
import numpy as np

from arcticdb.util.test import assert_frame_equal


def test_many_version_store(basic_store_factory):
    idx2 = np.arange(10, 20)
    d2 = {"x": np.arange(20, 30, dtype=np.int64)}
    df2 = pd.DataFrame(data=d2, index=idx2)

    for i in range(10):
        version_store = basic_store_factory(name=f"local.test{i}")

        symbol = "sym_{}".format(i)
        version_store.write(symbol, df2)
        vit = version_store.read(symbol)
        assert_frame_equal(vit.data, df2)
