"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from arcticdb.exceptions import ArcticNativeNotYetImplemented


def test_categorical(lmdb_version_store, sym):
    c = pd.Categorical(["a", "b", "c", "a", "b", "c"])
    df = pd.DataFrame({"int": np.arange(6), "cat": c})
    lib = lmdb_version_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat.dtype == "category"


def test_categorical_multiple_col(lmdb_version_store, sym):
    c = pd.Categorical(["a", "b", "c", "a", "b", "c"])
    c1 = pd.Categorical(["a", "b", "b", "a", "b", "c"])
    df = pd.DataFrame({"int": np.arange(6), "cat1": c, "cat2": c1})
    lib = lmdb_version_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat1.dtype == "category"
    assert read_df.cat2.dtype == "category"

    assert list(read_df.cat1) == list(c)
    assert list(read_df.cat2) == list(c1)

    assert_frame_equal(df, read_df)


def test_categorical_multiple_col_read_subset(lmdb_version_store, sym):
    c = pd.Categorical(["a", "b", "c", "a", "b", "c"])
    c1 = pd.Categorical(["a", "b", "b", "a", "b", "c"])
    df = pd.DataFrame({"int": np.arange(6), "cat1": c, "cat2": c1})
    lib = lmdb_version_store
    lib.write(sym, df)
    read_df = lib.read(sym, columns=["cat1"]).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat1.dtype == "category"

    assert list(read_df.cat1) == list(c)

    assert_frame_equal(df[["cat1"]], read_df)


def test_categorical_with_None(lmdb_version_store, sym):
    c = pd.Categorical(["a", "b", "c", "a", "b", "c", None])
    df = pd.DataFrame({"int": np.arange(7), "cat": c})
    lib = lmdb_version_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat.dtype == "category"
    assert_frame_equal(df, read_df)


def test_categorical_empty(lmdb_version_store, sym):
    c = pd.Categorical([])
    df = pd.DataFrame({"cat": c})
    lib = lmdb_version_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    assert_frame_equal(df, read_df)


def test_categorical_with_integers(lmdb_version_store, sym):
    c = pd.Categorical(np.arange(6))
    df = pd.DataFrame({"int": np.arange(6), "cat": c})
    lib = lmdb_version_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat.dtype == "category"
    assert_frame_equal(df, read_df)


def test_categorical_with_integers_and_strings(lmdb_version_store, sym):
    c = pd.Categorical(np.arange(6))
    c1 = pd.Categorical(["a", "b", "b", "a", "b", "c"])
    df = pd.DataFrame({"int": np.arange(6), "cat_int": c, "cat_str": c1})
    lib = lmdb_version_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat_int.dtype == "category"
    assert read_df.cat_str.dtype == "category"
    assert_frame_equal(df, read_df)


def test_categorical_batch_write(lmdb_version_store):
    lib = lmdb_version_store
    symbols = ["test_categorical_batch_write_1", "test_categorical_batch_write_2"]
    dfs = [
        pd.DataFrame({"a": ["hello", "hi", "hello"]}, dtype="category"),
        pd.DataFrame({"b": ["hello", "hi", "hello"]}),
    ]
    lib.batch_write(symbols, dfs)
    for idx, symbol in enumerate(symbols):
        assert_frame_equal(lib.read(symbol).data, dfs[idx])


def test_categorical_append(lmdb_version_store):
    lib = lmdb_version_store
    symbol = "test_categorical_append"
    original_df = pd.DataFrame({"a": ["hello", "hi", "hello"]}, dtype="category")
    lib.write(symbol, original_df)
    append_df = pd.DataFrame({"a": ["hi", "hi", "hello"]}, dtype="category")
    with pytest.raises(ArcticNativeNotYetImplemented) as e_info:
        lib.append(symbol, append_df)


def test_categorical_update(lmdb_version_store):
    lib = lmdb_version_store
    symbol = "test_categorical_update"
    original_df = pd.DataFrame({"a": ["hello", "hi", "hello"]}, dtype="category")
    lib.write(symbol, original_df)
    append_df = pd.DataFrame({"a": ["hi", "hi", "hello"]}, dtype="category")
    with pytest.raises(ArcticNativeNotYetImplemented) as e_info:
        lib.update(symbol, append_df)


def test_categorical_series(lmdb_version_store):
    lib = lmdb_version_store
    symbol = "test_categorical_series"
    original_series = pd.Series(["hello", "hi", "hello"], dtype="category")
    lib.write(symbol, original_series)
    append_series = pd.Series(["hi", "hi", "hello"], dtype="category")
    with pytest.raises(ArcticNativeNotYetImplemented) as e_info:
        lib.append(symbol, append_series)


def test_categorical_multi_index(lmdb_version_store):
    lib = lmdb_version_store
    symbol = "test_categorical_multi_index"
    dt1 = pd.datetime(2019, 4, 8, 10, 5, 2, 1)
    dt2 = pd.datetime(2019, 4, 9, 10, 5, 2, 1)
    arr1 = [dt1, dt1, dt2, dt2]
    arr2 = [0, 1, 0, 1]
    original_df = pd.DataFrame(
        data={"a": np.arange(10, 14)},
        dtype="category",
        index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"]),
    )
    lib.write(symbol, original_df)
    dt1 = pd.datetime(2019, 4, 10, 10, 5, 2, 1)
    dt2 = pd.datetime(2019, 4, 11, 10, 5, 2, 1)
    arr1 = [dt1, dt1, dt2, dt2]
    append_df = pd.DataFrame(
        data={"a": np.arange(20, 24)},
        dtype="category",
        index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"]),
    )
    with pytest.raises(ArcticNativeNotYetImplemented) as e_info:
        lib.append(symbol, append_df)
