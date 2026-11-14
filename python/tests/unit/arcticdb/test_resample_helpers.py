"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from arcticdb.util.test import assert_frame_equal, expected_pandas_resample_generic, generic_resample_test
from arcticdb.util._versions import PANDAS_VERSION
from packaging.version import Version


@pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Pandas < 1.3.0 do not support end_day")
@pytest.mark.parametrize("label", ["left", "right"])
@pytest.mark.parametrize("tz", [None, "UTC"])
@pytest.mark.parametrize("drop_empty_buckets", [False, True])
@pytest.mark.parametrize("check_dtype", [False, True])
def test_empty_resample_reference_preserves_index(label, tz, drop_empty_buckets, check_dtype):
    data = pd.DataFrame(
        {"col": np.array([0], dtype=np.int8)},
        index=pd.DatetimeIndex(["1960-01-01 00:00:01"], name="index", tz=tz),
    )
    aggregations = {"sum": ("col", "sum"), "mean": ("col", "mean"), "count": ("col", "count")}
    expected_types = {"sum": np.int64, "mean": np.float64, "count": np.uint64} if check_dtype else None
    result = expected_pandas_resample_generic(
        data,
        "1min",
        aggregations,
        closed="right",
        label=label,
        offset="1s",
        origin="end_day",
        drop_empty_buckets_for="col" if drop_empty_buckets else None,
        expected_types=expected_types,
    )
    expected = pd.DataFrame(
        {
            "count": np.array([], dtype=np.uint64),
            "mean": np.array([], dtype=np.float64),
            "sum": np.array([], dtype=np.int64),
        },
        index=pd.DatetimeIndex([], name="index", tz=tz),
    )
    assert_frame_equal(result, expected, check_dtype=check_dtype)


@pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Pandas < 1.3.0 do not support end_day")
def test_empty_resample_mismatch_preserves_assertion(monkeypatch):
    monkeypatch.setenv("ARCTICDB_TEST_READ_MODIFY_WRITE", "0")
    data = pd.DataFrame({"col": [0]}, index=pd.DatetimeIndex(["1960-01-01 00:00:01"], name="index"))
    incorrect = pd.DataFrame({"sum": np.array([], dtype=np.int64)}, index=pd.DatetimeIndex([], name="wrong"))
    lib = SimpleNamespace(read=lambda *args, **kwargs: SimpleNamespace(data=incorrect))
    with pytest.raises(AssertionError, match="names"):
        generic_resample_test(
            lib,
            "sym",
            "1min",
            {"sum": ("col", "sum")},
            data,
            closed="right",
            label="left",
            offset="1s",
            origin="end_day",
        )
