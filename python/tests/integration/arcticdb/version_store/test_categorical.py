"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import datetime
import sys

import numpy as np
import pandas as pd
import pytest

from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb.util._versions import IS_PANDAS_TWO
from arcticdb.util.test import assert_frame_equal


def test_categorical(basic_store, sym):
    c = pd.Categorical(["a", "b", "c", "a", "b", "c"])
    df = pd.DataFrame({"int": np.arange(6), "cat": c})
    lib = basic_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat.dtype == "category"


def test_categorical_multiple_col(basic_store, sym):
    c = pd.Categorical(["a", "b", "c", "a", "b", "c"])
    c1 = pd.Categorical(["a", "b", "b", "a", "b", "c"])
    df = pd.DataFrame({"int": np.arange(6), "cat1": c, "cat2": c1})
    lib = basic_store
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


def test_categorical_multiple_col_read_subset(basic_store, sym):
    c = pd.Categorical(["a", "b", "c", "a", "b", "c"])
    c1 = pd.Categorical(["a", "b", "b", "a", "b", "c"])
    df = pd.DataFrame({"int": np.arange(6), "cat1": c, "cat2": c1})
    lib = basic_store
    lib.write(sym, df)
    read_df = lib.read(sym, columns=["cat1"]).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat1.dtype == "category"

    assert list(read_df.cat1) == list(c)

    assert_frame_equal(df[["cat1"]], read_df)


def test_categorical_with_None(basic_store, sym):
    c = pd.Categorical(["a", "b", "c", "a", "b", "c", None])
    df = pd.DataFrame({"int": np.arange(7), "cat": c})
    lib = basic_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat.dtype == "category"
    assert_frame_equal(df, read_df)


def test_categorical_empty(basic_store, sym):
    c = pd.Categorical([])
    df = pd.DataFrame({"cat": c})
    lib = basic_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # In Pandas 1.0, an Index is used by default for any an empty dataframe or series is created,
    # except if there are categorical columns in which case a RangeIndex is used.
    #
    # In Pandas 2.0, RangeIndex is used by default for _any_ an empty dataframe or series is created.
    # See: https://github.com/pandas-dev/pandas/issues/49572
    assert isinstance(df.index, pd.RangeIndex)
    assert isinstance(read_df.index, pd.RangeIndex)
    assert_frame_equal(df, read_df)


def test_categorical_with_integers(basic_store, sym):
    c = pd.Categorical(np.arange(6))
    df = pd.DataFrame({"int": np.arange(6), "cat_int": c})
    lib = basic_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat_int.dtype == "category"
    if IS_PANDAS_TWO and sys.platform.startswith("win32"):
        # Pandas 2.0.0 changed the underlying creation from numpy integral arrays:
        # "Instantiating using a numpy numeric array now follows the dtype of the numpy array.
        # Previously, all indexes created from numpy numeric arrays were forced to 64-bit.
        # Now, for example, Index(np.array([1, 2, 3])) will be int32 on 32-bit systems,
        # where it previously would have been int64 even on 32-bit systems.
        # Instantiating Index using a list of numbers will still return 64bit dtypes,
        # e.g. Index([1, 2, 3]) will have a int64 dtype, which is the same as previously."
        # See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#index-can-now-hold-numpy-numeric-dtypes
        # We have not control over the underlying integral array storing code for categorical columns
        # so we replace the categorical column with its codes to perform the comparison with indentical dtypes.
        df.cat_int = df.cat_int.cat.codes.astype(np.int32)
        read_df.cat_int = read_df.cat_int.cat.codes.astype(np.int32)

    assert_frame_equal(df, read_df)


def test_categorical_with_integers_and_strings(basic_store, sym):
    c = pd.Categorical(np.arange(6))
    c1 = pd.Categorical(["a", "b", "b", "a", "b", "c"])
    df = pd.DataFrame({"int": np.arange(6), "cat_int": c, "cat_str": c1})
    lib = basic_store
    lib.write(sym, df)
    read_df = lib.read(sym).data
    # Not pickled
    assert lib.get_info(sym)["type"] == "pandasdf"
    # should be category
    assert read_df.cat_int.dtype == "category"
    assert read_df.cat_str.dtype == "category"
    if IS_PANDAS_TWO and sys.platform.startswith("win32"):
        # Pandas 2.0.0 changed the underlying creation from numpy integral arrays:
        # "Instantiating using a numpy numeric array now follows the dtype of the numpy array.
        # Previously, all indexes created from numpy numeric arrays were forced to 64-bit.
        # Now, for example, Index(np.array([1, 2, 3])) will be int32 on 32-bit systems,
        # where it previously would have been int64 even on 32-bit systems.
        # Instantiating Index using a list of numbers will still return 64bit dtypes,
        # e.g. Index([1, 2, 3]) will have a int64 dtype, which is the same as previously."
        # See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#index-can-now-hold-numpy-numeric-dtypes
        # We have not control over the underlying integral array storing code for categorical columns
        # so we replace the categorical column with its codes to perform the comparison with indentical dtypes.
        df.cat_int = df.cat_int.cat.codes.astype(np.int32)
        read_df.cat_int = read_df.cat_int.cat.codes.astype(np.int32)

    assert_frame_equal(df, read_df)


def test_categorical_batch_write(basic_store):
    lib = basic_store
    symbols = ["test_categorical_batch_write_1", "test_categorical_batch_write_2"]
    dfs = [
        pd.DataFrame({"a": ["hello", "hi", "hello"]}, dtype="category"),
        pd.DataFrame({"b": ["hello", "hi", "hello"]}),
    ]
    lib.batch_write(symbols, dfs)
    for idx, symbol in enumerate(symbols):
        assert_frame_equal(lib.read(symbol).data, dfs[idx])


def test_categorical_append(basic_store):
    lib = basic_store
    symbol = "test_categorical_append"
    original_df = pd.DataFrame({"a": ["hello", "hi", "hello"]}, dtype="category")
    lib.write(symbol, original_df)
    append_df = pd.DataFrame({"a": ["hi", "hi", "hello"]}, dtype="category")
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.append(symbol, append_df)


def test_categorical_update(basic_store):
    lib = basic_store
    symbol = "test_categorical_update"
    original_df = pd.DataFrame({"a": ["hello", "hi", "hello"]}, dtype="category")
    lib.write(symbol, original_df)
    append_df = pd.DataFrame({"a": ["hi", "hi", "hello"]}, dtype="category")
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.update(symbol, append_df)


def test_categorical_series(basic_store):
    lib = basic_store
    symbol = "test_categorical_series"
    original_series = pd.Series(["hello", "hi", "hello"], dtype="category")
    lib.write(symbol, original_series)
    append_series = pd.Series(["hi", "hi", "hello"], dtype="category")
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.append(symbol, append_series)


def test_categorical_multi_index(basic_store):
    lib = basic_store
    symbol = "test_categorical_multi_index"
    dt1 = datetime.datetime(2019, 4, 8, 10, 5, 2, 1)
    dt2 = datetime.datetime(2019, 4, 9, 10, 5, 2, 1)
    arr1 = [dt1, dt1, dt2, dt2]
    arr2 = [0, 1, 0, 1]
    original_df = pd.DataFrame(
        data={"a": np.arange(10, 14)},
        dtype="category",
        index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"]),
    )
    lib.write(symbol, original_df)
    dt1 = datetime.datetime(2019, 4, 10, 10, 5, 2, 1)
    dt2 = datetime.datetime(2019, 4, 11, 10, 5, 2, 1)
    arr1 = [dt1, dt1, dt2, dt2]
    append_df = pd.DataFrame(
        data={"a": np.arange(20, 24)},
        dtype="category",
        index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"]),
    )
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.append(symbol, append_df)
