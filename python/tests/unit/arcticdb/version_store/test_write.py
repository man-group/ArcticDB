"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np


def test_write_numpy_array(lmdb_version_store):
    symbol = "test_write_numpy_arr"
    arr = np.random.rand(2, 2, 2)
    lmdb_version_store.write(symbol, arr)

    np.array_equal(arr, lmdb_version_store.read(symbol).data)
