"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import pandas as pd

from arcticdb import Arctic
from arcticdb_ext.exceptions import StorageException


def test_s3_storage_failures(mock_s3_store_with_error_simulation):
    lib = mock_s3_store_with_error_simulation
    symbol_fail_write = "symbol#Failure_Put_99_0"
    symbol_fail_read = "symbol#Failure_Get_17_0"
    df = pd.DataFrame({"a": list(range(100))}, index=list(range(100)))

    with pytest.raises(StorageException, match="Unexpected network error: S3Error#99"):
        lib.write(symbol_fail_write, df)

    with pytest.raises(StorageException, match="Unexpected error: S3Error#17"):
        lib.read(symbol_fail_read)
