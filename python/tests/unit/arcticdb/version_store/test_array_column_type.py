"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import pytest


@pytest.fixture(params=("int32", "float32", "int64", "float64"))
def array_type(request):
    yield request.param


def assert_db_in_out_match(version_store, df_in, symbol):
    version_store.write(symbol, df_in)
    df_out = version_store.read(symbol)
    assert_frame_equal(df_in, df_out.data)


@pytest.mark.skip(reason="Arrays occasionally raise segfault")
class TestEmptyArrays:
    def test_single_empty_array(self, lmdb_version_store):
        df_in = pd.DataFrame({"col1": [np.array([])]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_single_empty_array")

    def test_multiple_empty_arrays(self, lmdb_version_store):
        df_in = pd.DataFrame({"col1": [np.array([]), np.array([]), np.array([])]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_multiple_empty_arrays")

    def test_empty_array_can_coexist_with_nonempty_arrays(self, lmdb_version_store, array_type):
        df_in = pd.DataFrame(
            {
                "col1": [
                    np.array([]).astype(array_type),
                    np.array([1, 2, 3, 4, 5]).astype(array_type),
                    np.array([]).astype(array_type),
                ]
            }
        )
        assert_db_in_out_match(lmdb_version_store, df_in, "test_empty_array_can_coexist_with_nonempty_arrays")

    def test_append_to_colum_with_empty_array(self, lmdb_version_store, array_type):
        df_empty = pd.DataFrame({"col1": [np.array([]), np.array([])]})
        lmdb_version_store.write("test_append_to_colum_with_empty_array", df_empty)
        df_to_append = pd.DataFrame({"col1": [np.array([1, 2, 3]).astype(array_type)]})
        lmdb_version_store.append("test_append_to_colum_with_empty_array", df_to_append)
        df_out = lmdb_version_store.read("test_append_to_colum_with_empty_array")
        df_target = pd.concat([df_empty, df_to_append], ignore_index=True)
        assert_frame_equal(df_target, df_out.data)

    def test_append_empty_arrays_to_column(self, lmdb_version_store, array_type):
        df = pd.DataFrame({"col1": [np.array([1, 2, 3]).astype(array_type)]})
        lmdb_version_store.write("test_append_to_colum_with_empty_array", df)
        df_to_append = pd.DataFrame({"col1": [np.array([])]})
        lmdb_version_store.append("test_append_to_colum_with_empty_array", df_to_append)
        df_out = lmdb_version_store.read("test_append_to_colum_with_empty_array")
        df_target = pd.concat([df, df_to_append], ignore_index=True)
        assert_frame_equal(df_target, df_out.data)

    def test_can_mix_empty_array_and_none_in_segment(self, lmdb_version_store):
        df = pd.DataFrame({"col1": [np.array([]), None, None, np.array([])]})
        lmdb_version_store.write("test_can_mix_empty_array_and_none_in_segment", df)
        assert_db_in_out_match(lmdb_version_store, df, "test_can_mix_empty_array_and_none_in_segment")

    def test_can_append_emptyval_to_empty_array(self, lmdb_version_store):
        df = pd.DataFrame({"col1": [np.array([]), np.array([])]})
        lmdb_version_store.write("test_can_append_empty_to_empty_array", df)
        df_to_append = pd.DataFrame({"col1": [None, None]})
        lmdb_version_store.append("test_can_append_empty_to_empty_array", df_to_append)
        df_out = lmdb_version_store.read("test_can_append_empty_to_empty_array")
        df_target = pd.concat([df, df_to_append], ignore_index=True)
        assert_frame_equal(df_target, df_out.data)

    def test_can_append_empty_array_to_emptyval(self, lmdb_version_store):
        df = pd.DataFrame({"col1": [None, None]})
        lmdb_version_store.write("test_can_append_empty_to_empty_array", df)
        df_to_append = pd.DataFrame({"col1": [np.array([]), np.array([])]})
        lmdb_version_store.append("test_can_append_empty_to_empty_array", df_to_append)
        df_out = lmdb_version_store.read("test_can_append_empty_to_empty_array")
        df_target = pd.concat([df, df_to_append], ignore_index=True)
        assert_frame_equal(df_target, df_out.data)


@pytest.mark.skip(reason="Arrays occasionally raise segfault")
class TestNonEmptyArrays:
    def test_single_array(self, lmdb_version_store, array_type):
        df_in = pd.DataFrame({"col1": [np.array([1, 2, 3]).astype(array_type)]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_single_array")

    def test_differently_shaped_rows(self, lmdb_version_store, array_type):
        df_in = pd.DataFrame(
            {
                "col1": [
                    np.array([1, 2, 3]).astype(array_type),
                    np.array([1]).astype(array_type),
                    np.array([1, 2, 3, 4]).astype(array_type),
                    np.array([2, 4]).astype(array_type),
                ]
            }
        )
        assert_db_in_out_match(lmdb_version_store, df_in, "test_differently_shaped_rows")

    def test_rows_can_be_none(self, lmdb_version_store, array_type):
        df_in = pd.DataFrame({"col1": [None, np.array([1, 2]).astype(array_type), None]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_rows_can_be_none")

    def test_can_contain_nan(self, lmdb_version_store):
        # Note integers don't have a NaN value
        df_in = pd.DataFrame({"col1": [np.array([np.NAN, float("NaN")])]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_can_contain_nan")

    def test_can_append(self, lmdb_version_store, array_type):
        df = pd.DataFrame({"col1": [np.array([1, 2, 3]).astype(array_type)]})
        lmdb_version_store.write("test_can_append", df)
        df_to_append = pd.DataFrame(
            {"col1": [np.array([10]).astype(array_type), np.array([20, 30, 40, 50]).astype(array_type)]}
        )
        lmdb_version_store.append("test_can_append", df_to_append)
        df_out = lmdb_version_store.read("test_can_append")
        df_target = pd.concat([df, df_to_append], ignore_index=True)
        assert_frame_equal(df_target, df_out.data)


@pytest.mark.skip(reason="Arrays occasionally raise segfault")
class TestFailure:
    def test_cannot_mix_scalars_and_arrays(self, lmdb_version_store):
        with pytest.raises(Exception):
            df = pd.DataFrame({"col1": [1, np.array([1, 2, 3])]})
            lmdb_version_store.write("test_cannot_mix_scalars_and_arrays", df)

        with pytest.raises(Exception):
            df = pd.DataFrame({"col1": [np.array([1, 2, 3]), 1]})
            lmdb_version_store.write("test_cannot_mix_scalars_and_arrays", df)

    def test_cannot_append_scalar_to_array(self, lmdb_version_store):
        df = pd.DataFrame({"col": [np.array([1, 2, 3])]})
        lmdb_version_store.write("test_cannot_append_scalar_to_array", df)
        df_to_append = pd.DataFrame({"col": [1]})
        with pytest.raises(Exception):
            lmdb_version_store.append("test_cannot_append_scalar_to_array", df_to_append)

    def test_cannot_append_array_to_scalar(self, lmdb_version_store):
        df = pd.DataFrame({"col": [1]})
        lmdb_version_store.write("test_cannot_append_scalar_to_array", df)
        df_to_append = pd.DataFrame({"col": [np.array([1, 2, 3])]})
        with pytest.raises(Exception):
            lmdb_version_store.append("test_cannot_append_scalar_to_array", df_to_append)
