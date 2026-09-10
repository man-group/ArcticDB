"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.exceptions import ErrorCode, SchemaException
from arcticdb.version_store.processing import QueryBuilder

ERROR_CODE_FOR = {
    "pickled": ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA,
    "numpy": ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_NUMPY_ARRAY,
    "recursive": ErrorCode.E_OPERATION_NOT_SUPPORTED_WITH_RECURSIVE_NORMALIZED_DATA,
}

TS = pd.date_range("2024-01-01", periods=6, freq="h")
PICKLED_DATA = pd.DataFrame({"a": [[i, i] for i in range(6)]}, index=TS)
NUMPY_DATA = np.arange(6)
RECURSIVE_DATA = {"a": np.arange(5), "b": np.arange(8)}
MERGE_SOURCE = pd.DataFrame({"a": [99]}, index=[pd.Timestamp("2024-01-02")])


def write_unprocessable(nvs, kind, symbol="sym"):
    if kind == "pickled":
        nvs.write(symbol, PICKLED_DATA, pickle_on_failure=True)
    elif kind == "numpy":
        nvs.write(symbol, NUMPY_DATA)
    elif kind == "recursive":
        nvs.write(symbol, RECURSIVE_DATA, recursive_normalizers=True)
    else:
        raise AssertionError(f"unknown data kind {kind}")
    return symbol


def expect_refusal(kind):
    return pytest.raises(SchemaException, match=ERROR_CODE_FOR[kind].name)


def filter_query():
    q = QueryBuilder()
    return q[q["a"] == 0]


def projection_query():
    q = QueryBuilder()
    return q.apply("doubled", q["a"] * 2)


def groupby_agg_query():
    return QueryBuilder().groupby("g").agg({"a": "sum"})


def resample_query():
    return QueryBuilder().resample("2h").agg({"a": "sum"})


PROCESSING_KINDS = {
    "filter": filter_query,
    "projection": projection_query,
    "groupby_agg": groupby_agg_query,
    "resample": resample_query,
}

all_data_kinds = pytest.mark.parametrize("kind", list(ERROR_CODE_FOR))
all_processing_kinds = pytest.mark.parametrize("processing", list(PROCESSING_KINDS))
