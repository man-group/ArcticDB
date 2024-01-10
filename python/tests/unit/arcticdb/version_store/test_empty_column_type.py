"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import pytest

@pytest.fixture
def int_dtype():
    bit_sizes = ["8", "16", "32", "64"]
    base_numeric_types = ["int", "uint"]
    for t in base_numeric_types:
        for s in bit_sizes:
            yield t + s


@pytest.fixture
def float_dtype():
    bit_sizes = ["32", "64"]
    for s in bit_sizes:
        yield "float" + s

class TestEmptyColumnTypeAppend:
    def test_can_append_non_empty_to_empty(self, lmdb_version_store_v2, int_dtype):
        lmdb_version_store = lmdb_version_store_v2
        df_empty = pd.DataFrame({"col1": [None, None]})
        lmdb_version_store.write("test_can_append_to_empty_column", df_empty)
        df_non_empty = pd.DataFrame({"col1": np.array([1,2,3], dtype=int_dtype)})
        lmdb_version_store.append("test_can_append_to_empty_column", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([0,0,1,2,3], dtype=int_dtype)})
        assert_frame_equal(lmdb_version_store.read("test_can_append_to_empty_column").data, expected_result)

    @pytest.mark.xfail(reason="Not implemented")
    def test_can_append_empty_to_non_empty(self, lmdb_version_store, element_type_constructor):
        df_empty = pd.DataFrame({"col1": [None, None]})
        df_non_empty = pd.DataFrame({"col1": map(element_type_constructor, [1, 2, 3])})
        lmdb_version_store.write("test_can_append_to_empty_column", df_non_empty)
        lmdb_version_store.append("test_can_append_to_empty_column", df_empty)
        expected_result = pd.concat([df_non_empty, df_empty], ignore_index=True)
        assert_frame_equal(lmdb_version_store.read("test_can_append_to_empty_column").data, expected_result)

    @pytest.mark.xfail(reason="Not implemented")
    def test_can_append_empty_to_empty(self, lmdb_version_store, element_type_constructor):
        df_empty_1 = pd.DataFrame({"col1": [None, None]})
        df_empty_2 = pd.DataFrame({"col1": []})
        df_empty_3 = pd.DataFrame({"col1": [None]})
        lmdb_version_store.write("test_can_append_to_empty_column", df_empty_1)
        lmdb_version_store.append("test_can_append_to_empty_column", df_empty_2)
        lmdb_version_store.append("test_can_append_to_empty_column", df_empty_3)
        expected_result = pd.concat([df_empty_1, df_empty_2, df_empty_3], ignore_index=True)
        assert_frame_equal(lmdb_version_store.read("test_can_append_to_empty_column").data, expected_result)
