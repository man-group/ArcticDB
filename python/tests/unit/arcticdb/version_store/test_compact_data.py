"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.exceptions import StorageException
from arcticdb.exceptions import ArcticNativeException, UserInputException
from arcticdb.util.test import assert_frame_equal


def test_compact_data_symbol_doesnt_exist(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_symbol_doesnt_exist"
    with pytest.raises(StorageException) as e:
        lib.compact_data_experimental(sym)
    assert sym in str(e.value)


def test_compact_data_negative_rows_per_segment(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_negative_rows_per_segment"
    with pytest.raises(ArcticNativeException):
        lib.compact_data_experimental(sym, rows_per_segment=-1)


def test_compact_data_noop(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_noop"
    df = pd.DataFrame({"col": np.arange(200_000)})
    lib.write(sym, df)
    lib.compact_data_experimental(sym)
    assert_frame_equal(df, lib.read(sym).data)
    index = lib.read_index(sym)
    print("fin")
