"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import pytest

from pandas import MultiIndex
from arcticdb.version_store import NativeVersionStore
from arcticdb_ext.exceptions import (
    InternalException,
    NormalizationException,
    SortingException,
)


@pytest.mark.parametrize(
    "operation", ["update", "append", "sort_index", "delete_range", "restore_version", "batch_restore_version"]
)
def test_version_chain_increasing(version_store_factory, operation):
    lib = version_store_factory()
    sym = "sym"

    df = pd.DataFrame({"col": [1, 2, 3]}, index=pd.date_range(start=pd.Timestamp(0), periods=3, freq="ns"))
    df_2 = pd.DataFrame({"col": [1, 2, 6]}, index=pd.date_range(start=pd.Timestamp(0), periods=3, freq="ns"))

    def execute_operation():
        if operation == "update":
            df_update = pd.DataFrame({"col": [4, 5]}, index=pd.date_range(start=pd.Timestamp(1), periods=2, freq="ns"))
            lib.update(sym, df_update)
        elif operation == "append":
            df_append = pd.DataFrame({"col": [4, 5]}, index=pd.date_range(start=pd.Timestamp(3), periods=2, freq="ns"))
            lib.append(sym, df_append)
        elif operation == "sort_index":
            lib.version_store.sort_index(sym, False, False)
        elif operation == "delete_range":
            lib.delete(sym, date_range=(pd.Timestamp(1), pd.Timestamp(1)))
        elif operation == "restore_version":
            lib.restore_version(sym, 0)
        elif operation == "batch_restore_version":
            lib.batch_restore_version([sym], [0])
        else:
            raise "Unknown operation"

    lib.write(sym, df)
    assert lib.read(sym).version == 0

    lib.write(sym, df_2)
    assert lib.read(sym).version == 1

    lib.delete_version(sym, 1)
    assert lib.read(sym).version == 0

    execute_operation()
    assert lib.read(sym).version == 2
