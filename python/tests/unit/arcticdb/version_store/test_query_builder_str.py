"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import datetime
import numpy as np
import pandas as pd
import pytest

from arcticdb.version_store.processing import QueryBuilder, where

pytestmark = pytest.mark.pipeline


def test_to_strings():
    q = QueryBuilder().row_range((1, 10))
    assert str(q) == "ROWRANGE: RANGE, start=1, end=10"

    q = QueryBuilder().head(10)
    assert str(q) == "ROWRANGE: HEAD, n=10"

    q = QueryBuilder().tail(9)
    assert str(q) == "ROWRANGE: TAIL, n=9"

    q = QueryBuilder().date_range((pd.Timestamp(1000), pd.Timestamp(2000)))
    assert str(q) == "DATE RANGE 1000 - 2000"

    q = QueryBuilder().date_range((None, pd.Timestamp(2000)))
    assert str(q) == f"DATE RANGE {pd.Timestamp.min.value} - 2000"

    q = QueryBuilder().date_range((pd.Timestamp(1000), None))
    assert str(q) == f"DATE RANGE 1000 - {pd.Timestamp.max.value}"

    q = QueryBuilder()
    q["def"] = 2 * q["abc"]
    assert str(q) == 'PROJECT Column["def"] = (Val(UINT8:2) MUL Column["abc"])'

    q = QueryBuilder()
    q = q[q["abc"] > 3]
    assert str(q) == 'WHERE (Column["abc"] GT Val(UINT8:3))'

    q = QueryBuilder()
    q = q[q["abc"] > 3]
    q = q[q["def"] > q["ghi"]]
    q.row_range((1, 10))
    assert (
        str(q)
        == 'WHERE (Column["abc"] GT Val(UINT8:3)) | WHERE (Column["def"] GT Column["ghi"]) | ROWRANGE: RANGE, start=1, end=10'
    )

    q = QueryBuilder().resample("1min").agg({"col": "sum"})
    assert str(q) == "RESAMPLE(1min) | AGGREGATE {col: (col, sum), }"


@pytest.mark.parametrize(
    "value, expected_leaf_str",
    [
        (True, "Val(BOOL8:true)"),
        (False, "Val(BOOL8:false)"),
        (3, "Val(UINT8:3)"),
        (-3, "Val(INT8:-3)"),
        (300, "Val(UINT16:300)"),
        (100_000, "Val(UINT32:100000)"),
        (3.5, "Val(FLOAT32:3.5)"),
        (np.uint8(3), "Val(UINT8:3)"),
        (np.int16(-3), "Val(INT8:-3)"),
        (np.float32(3.5), "Val(FLOAT32:3.5)"),
        # datetime and date have identical nanosecond payloads, so the Python type is not recoverable.
        (datetime.datetime(2025, 1, 1), "Val(NANOSECONDS_UTC64:2025-01-01 00:00:00.000000000)"),
        (datetime.date(2025, 1, 1), "Val(NANOSECONDS_UTC64:2025-01-01 00:00:00.000000000)"),
        (pd.Timestamp(2025, 1, 1, 12, 30, 15, 123456), "Val(NANOSECONDS_UTC64:2025-01-01 12:30:15.123456000)"),
        (pd.Timestamp("2025-01-01 00:00:00.000000001"), "Val(NANOSECONDS_UTC64:2025-01-01 00:00:00.000000001)"),
        (pd.NaT, "Val(NANOSECONDS_UTC64:NaT)"),
        # There is no duration DataType, so durations stay raw nanoseconds.
        (datetime.timedelta(days=1), "Val(INT64:86400000000000)"),
        (pd.Timedelta("1 days"), "Val(INT64:86400000000000)"),
    ],
)
def test_to_strings_numeric_and_bool_leaf(value, expected_leaf_str):
    q = QueryBuilder()
    q = q[q["c"] > value]
    assert str(q) == f'WHERE (Column["c"] GT {expected_leaf_str})'


def test_to_strings_string_leaf():
    q = QueryBuilder()
    q = q[q["c"] == "hello"]
    assert str(q) == 'WHERE (Column["c"] EQ Val(UTF_DYNAMIC64:"hello"))'


def test_to_strings_regex_leaf():
    q = QueryBuilder()
    q = q[q["c"].regex_match("^abc.*")]
    assert str(q) == 'WHERE (Column["c"] REGEX_MATCH Regex(^abc.*))'


@pytest.mark.parametrize("method, op", [("isin", "ISIN"), ("isnotin", "ISNOTIN")])
@pytest.mark.parametrize(
    "values, expected_leaf_str",
    [
        ([1, 2, 3], "ValueSet(INT64,n=3)"),
        (["a", "b"], "ValueSet(UTF_DYNAMIC64,n=2)"),
        (np.arange(200), "ValueSet(INT64,n=200)"),
    ],
)
def test_to_strings_value_set_leaf(method, op, values, expected_leaf_str):
    q = QueryBuilder()
    q = q[getattr(q["c"], method)(values)]
    assert str(q) == f'WHERE (Column["c"] {op} {expected_leaf_str})'


def test_to_strings_unary_operations():
    q = QueryBuilder()
    q = q[-q["c"] > 0]
    assert str(q) == 'WHERE (NEG(Column["c"]) GT Val(UINT8:0))'

    q = QueryBuilder()
    q = q[abs(q["c"]) > 0]
    assert str(q) == 'WHERE (ABS(Column["c"]) GT Val(UINT8:0))'

    q = QueryBuilder()
    q = q[~(q["c"] > 0)]
    assert str(q) == 'WHERE NOT((Column["c"] GT Val(UINT8:0)))'

    q = QueryBuilder()
    q = q[q["col"]]
    assert str(q) == 'WHERE IDENTITY(Column["col"])'

    q = QueryBuilder()
    q = q[~q["col"]]
    assert str(q) == 'WHERE NOT(Column["col"])'


def test_to_strings_ternary_where():
    q = QueryBuilder()
    q = q.apply("d", where(q["c"] > 0, q["a"], q["b"]))
    assert str(q) == 'PROJECT Column["d"] = Column["a"] if (Column["c"] GT Val(UINT8:0)) else Column["b"]'


def test_to_strings_groupby_and_concat_clauses():
    q = QueryBuilder().groupby("c").agg({"a": "sum"})
    assert str(q) == 'GROUPBY Column["c"] | AGGREGATE {a: (a, sum), }'

    q = QueryBuilder().concat("outer")
    assert str(q) == "CONCAT"


def test_to_strings_datetime_range_filter():
    start_date = datetime.datetime(2025, 1, 1)
    end_date = datetime.datetime(2025, 1, 31)

    q1 = QueryBuilder()
    q1 = q1[q1["date_col"] >= start_date]

    q2 = QueryBuilder()
    q2 = q2[q2["date_col"] < end_date]

    assert f"{q1} | {q2}" == (
        'WHERE (Column["date_col"] GE Val(NANOSECONDS_UTC64:2025-01-01 00:00:00.000000000)) | '
        'WHERE (Column["date_col"] LT Val(NANOSECONDS_UTC64:2025-01-31 00:00:00.000000000))'
    )


def test_to_strings_datetime_projection():
    q = QueryBuilder()
    q["shifted"] = q["date_col"] + datetime.timedelta(days=1)
    assert str(q) == 'PROJECT Column["shifted"] = (Column["date_col"] ADD Val(INT64:86400000000000))'
