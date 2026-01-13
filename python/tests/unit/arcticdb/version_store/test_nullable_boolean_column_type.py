"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb.arctic import Arctic
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import pytest


@pytest.fixture(params=(list, lambda arr: np.array(arr, dtype="bool"), lambda arr: pd.array(arr, dtype="bool")))
def array_constructor(request):
    yield request.param


def assert_db_in_out_match(version_store, df_in):
    version_store.write("test", df_in)
    df_out = version_store.read("test")
    assert_frame_equal(df_in, df_out.data)


@pytest.mark.skip("Temporary disabled")
def test_all_none(lmdb_version_store, array_constructor):
    df_in = pd.DataFrame({"col1": array_constructor([None, None, None])})
    assert_db_in_out_match(lmdb_version_store, df_in)


@pytest.mark.skip("Temporary disabled")
def test_values_and_none(lmdb_version_store, array_constructor):
    df_in = pd.DataFrame({"col1": array_constructor([True, None, False, None])})
    assert_db_in_out_match(lmdb_version_store, df_in)


@pytest.mark.skip("Temporary disabled")
def test_values_only(lmdb_version_store, array_constructor):
    df_in = pd.DataFrame({"col1": array_constructor([True, False])})
    assert_db_in_out_match(lmdb_version_store, df_in)
