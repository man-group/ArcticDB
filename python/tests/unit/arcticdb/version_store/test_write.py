"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""
import numpy as np


def test_write_numpy_array(lmdb_version_store):
    symbol = "test_write_numpy_arr"
    arr = np.random.rand(2, 2, 2)
    lmdb_version_store.write(symbol, arr)

    np.array_equal(arr, lmdb_version_store.read(symbol).data)
