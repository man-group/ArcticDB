import numpy as np
import pandas as pd


def get_dataframe_dense_till(total_size=100, dense_till=50):
    arr = []
    for idx in range(total_size):
        if idx < dense_till:
            arr.append(float(idx))
        else:
            arr.append(np.nan)

    return pd.DataFrame({"float": arr})


def get_interleaved_dataframe(size=100):
    arr = []
    for idx in range(size):
        if idx % 2 == 0:
            arr.append(float(idx))
        else:
            arr.append(np.nan)

    return pd.DataFrame({"float": arr})


def test_sparse_interleaved(sym, lmdb_version_store):
    lib = lmdb_version_store
    df = get_interleaved_dataframe(100)
    lib.write(sym, df)
    dd = lib.read(sym).data

    assert dd["float"][2] == df["float"][2]
    assert np.isnan(dd["float"][1])


def test_sparse_chunked(sym, lmdb_version_store):
    lib = lmdb_version_store
    df = get_dataframe_dense_till(100, 30)
    lib.write(sym, df, sparsify_floats=True)
    dd = lib.read(sym).data

    assert dd["float"][2] == df["float"][2]
    assert dd["float"][1] == 1.0
    assert dd["float"][2] == 2.0


def test_sparse_segmented(version_store_factory, sym):
    lib = version_store_factory(column_group_size=100, segment_row_size=100)

    df = get_interleaved_dataframe(100)
    lib.write(sym, df, sparsify_floats=True)
    dd = lib.read(sym).data

    assert dd["float"][2] == df["float"][2]
    assert np.isnan(dd["float"][1])
