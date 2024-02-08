"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import pytest

@pytest.fixture(params=(int, float, str))
def element_type_constructor(request):
    yield request.param


@pytest.mark.xfail(reason="Not implemented")
class TestEmptyColumnTypeAppend:
    def test_can_append_non_empty_to_empty(self, lmdb_version_store, element_type_constructor):
        df_empty = pd.DataFrame({"col1": [None, None]})
        lmdb_version_store.write("test_can_append_to_empty_column", df_empty)
        df_non_empty = pd.DataFrame({"col1": map(element_type_constructor, [1, 2, 3])})
        lmdb_version_store.append("test_can_append_to_empty_column", df_non_empty)
        expected_result = pd.concat([df_empty, df_non_empty], ignore_index=True)
        assert_frame_equal(lmdb_version_store.read("test_can_append_to_empty_column").data, expected_result)

    def test_can_append_empty_to_non_empty(self, lmdb_version_store, element_type_constructor):
        df_empty = pd.DataFrame({"col1": [None, None]})
        df_non_empty = pd.DataFrame({"col1": map(element_type_constructor, [1, 2, 3])})
        lmdb_version_store.write("test_can_append_to_empty_column", df_non_empty)
        lmdb_version_store.append("test_can_append_to_empty_column", df_empty)
        expected_result = pd.concat([df_non_empty, df_empty], ignore_index=True)
        assert_frame_equal(lmdb_version_store.read("test_can_append_to_empty_column").data, expected_result)

    def test_can_append_empty_to_empty(self, lmdb_version_store, element_type_constructor):
        df_empty_1 = pd.DataFrame({"col1": [None, None]})
        df_empty_2 = pd.DataFrame({"col1": []})
        df_empty_3 = pd.DataFrame({"col1": [None]})
        lmdb_version_store.write("test_can_append_to_empty_column", df_empty_1)
        lmdb_version_store.append("test_can_append_to_empty_column", df_empty_2)
        lmdb_version_store.append("test_can_append_to_empty_column", df_empty_3)
        expected_result = pd.concat([df_empty_1, df_empty_2, df_empty_3], ignore_index=True)
        assert_frame_equal(lmdb_version_store.read("test_can_append_to_empty_column").data, expected_result)


@pytest.fixture(
    scope="function",
    params=(
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ),
)
def lmdb_version_store_static_and_dynamic(request):
    """
    Designed to test all combinations between schema and encoding version for LMDB
    """
    yield request.getfixturevalue(request.param)
    

def test_roundtrip(lmdb_version_store_static_and_dynamic):
    df = pd.DataFrame({"col": [None, None, None], "another_col": [None, "asd", float("NaN")]})
    lmdb_version_store_static_and_dynamic.write("sym", df)
    print(lmdb_version_store_static_and_dynamic.read("sym").data)
    assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)