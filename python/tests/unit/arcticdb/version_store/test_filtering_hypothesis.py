"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from datetime import datetime
from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes
from hypothesis.extra.pytz import timezones as timezone_st
import hypothesis.strategies as st
import numpy as np
from packaging.version import Version
import pandas as pd
import pytest
from pytz import timezone

try:
    from pandas.errors import UndefinedVariableError
except ImportError:
    from pandas.core.computation.ops import UndefinedVariableError

from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException, StorageException, UserInputException
from arcticdb.util.test import assert_frame_equal, make_dynamic, PANDAS_VERSION, regularize_dataframe
from arcticdb.util._versions import PANDAS_VERSION
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    integral_type_strategies,
    unsigned_integral_type_strategies,
    signed_integral_type_strategies,
    numeric_type_strategies,
    non_zero_numeric_type_strategies,
    string_strategy,
    dataframes_with_names_and_dtypes,
)

# from tests.util.mark import MACOS_CONDA_BUILD


pytestmark = pytest.mark.pipeline


def generic_filter_test(lib, symbol, df, arctic_query, pandas_query, dynamic_strings=True):
    # lib.write(symbol, df, dynamic_strings=dynamic_strings)
    expected = df.query(pandas_query)
    received = lib.read(symbol, query_builder=arctic_query).data
    if not np.array_equal(expected, received):
        print(f"\nOriginal dataframe:\n{df}\ndtypes:\n{df.dtypes}")
        print(f"\nPandas query: {pandas_query}")
        print(f"\nPandas returns:\n{expected}")
        print(f"\nQueryBuilder returns:\n{received}")
        assert False
    assert True


# For string queries, test both with and without dynamic strings, and with the query both optimised for speed and memory
def generic_filter_test_strings(lib, symbol, df, arctic_query, pandas_query):
    expected = df.query(pandas_query)
    for dynamic_strings in [True, False]:
        arctic_query.optimise_for_speed()
        generic_filter_test(lib, symbol, df, arctic_query, pandas_query, dynamic_strings)
        arctic_query.optimise_for_memory()
        generic_filter_test(lib, symbol, df, arctic_query, pandas_query, dynamic_strings)


def generic_dynamic_filter_test(lib, symbol, df, arctic_query, pandas_query, dynamic_strings=True):
    lib.delete(symbol)
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    try:
        expected = expected.query(pandas_query)
        received = lib.read(symbol, query_builder=arctic_query).data
        expected = regularize_dataframe(expected)
        received = regularize_dataframe(received)
        if not len(expected) == 0 and len(received) == 0:
            if not np.array_equal(expected, received):
                print("Original dataframe\n{}".format(expected))
                print("Pandas query\n{}".format(pandas_query))
                print("Expected\n{}".format(expected))
                print("Received\n{}".format(received))
                assert False
    except UndefinedVariableError:
        # Might have edited out the query columns entirely
        pass

    assert True


# TODO: Clean this mess up
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_binary_comparison(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_filter_binary_comparison"
    lib.write(symbol, df)
    for op in ["<", "<=", ">", ">=", "==", "!="]:
        for comp in ["col op col", "col op val", "val op col"]:
            if op == "<":
                q = QueryBuilder()
                if comp == "col op col":
                    q = q[q["a"] < q["b"]]
                else:
                    q = q[q["a"] < val] if comp == "col op val" else q[val < q["a"]]
            elif op == "<=":
                q = QueryBuilder()
                if comp == "col op col":
                    q = q[q["a"] <= q["b"]]
                else:
                    q = q[q["a"] <= val] if comp == "col op val" else q[val <= q["a"]]
            elif op == ">":
                q = QueryBuilder()
                if comp == "col op col":
                    q = q[q["a"] > q["b"]]
                else:
                    q = q[q["a"] > val] if comp == "col op val" else q[val > q["a"]]
            elif op == ">=":
                q = QueryBuilder()
                if comp == "col op col":
                    q = q[q["a"] >= q["b"]]
                else:
                    q = q[q["a"] >= val] if comp == "col op val" else q[val >= q["a"]]
            elif op == "==":
                q = QueryBuilder()
                if comp == "col op col":
                    q = q[q["a"] == q["b"]]
                else:
                    q = q[q["a"] == val] if comp == "col op val" else q[val == q["a"]]
            elif op == "!=":
                q = QueryBuilder()
                if comp == "col op col":
                    q = q[q["a"] != q["b"]]
                else:
                    q = q[q["a"] != val] if comp == "col op val" else q[val != q["a"]]
            if comp == "col op col":
                pandas_query = f"a {op} b"
            else:
                pandas_query = f"a {op} {val}" if comp == "col op val" else f"{val} {op} a"
            generic_filter_test(lib, symbol, df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
# Restrict datetime range to a couple of years, as this should be sufficient to catch most weird corner cases
@settings(deadline=None)
@given(
    df_dt=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2022, 1, 1), timezones=timezone_st()),
    comparison_dt=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2022, 1, 1), timezones=timezone_st()),
)
def test_filter_datetime_timezone_aware_hypothesis(version_store_factory, df_dt, comparison_dt):
    lmdb_version_store = version_store_factory(name="_unique_")
    symbol = "test_filter_datetime_timezone_aware_hypothesis"
    df = pd.DataFrame({"a": [df_dt]})
    for ts in [comparison_dt, pd.Timestamp(comparison_dt)]:
        q = QueryBuilder()
        q = q[q["a"] < ts]
        pandas_query = "a < @ts"
        # Cannot use generic_filter_test as roundtripping a dataframe with datetime64 columns does not preserve tz info
        lmdb_version_store.write(symbol, df)
        expected = df.query(pandas_query)
        # Convert to UTC and strip tzinfo to match behaviour of roundtripping through Arctic
        expected["a"] = expected["a"].apply(lambda x: x.tz_convert(timezone("utc")).tz_localize(None))
        received = lmdb_version_store.read(symbol, query_builder=q).data
        if not np.array_equal(expected, received) and (not expected.empty and not received.empty):
            print("ts\n{}".format(ts))
            print("Original dataframe\n{}".format(df))
            print("Expected\n{}".format(expected))
            print("Received\n{}".format(received))
            assert False
        assert True


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=numeric_type_strategies())
def test_filter_compare_string_number_col_val(lmdb_version_store_v1, df, val):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] < val]
    symbol = "test_filter_compare_string_number_col_val"
    lib.write(symbol, df)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val < q["a"])]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()), val=string_strategy)
def test_filter_compare_string_number_val_col(lmdb_version_store_v1, df, val):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val < q["a"]]
    symbol = "test_filter_compare_string_number_val_col"
    lib.write(symbol, df)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[q["a"] < val]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=numeric_type_strategies())], index=range_indexes()
    )
)
def test_filter_compare_string_number_col_col(lmdb_version_store_v1, df):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] < q["b"]]
    symbol = "test_filter_compare_string_number_col_col"
    lib.write(symbol, df)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[q["b"] < q["a"]]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
# Note min_size=1 for the sets in the following two tests, as an empty set does not have an associated type
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_isin_string_number_signed(lmdb_version_store_v1, df, vals):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    symbol = "test_filter_isin_string_number"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
# Note min_size=1 for the sets in the following two tests, as an empty set does not have an associated type
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_isin_string_number_unsigned(lmdb_version_store_v1, df, vals):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    symbol = "test_filter_isin_string_number"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=integral_type_strategies())], index=range_indexes()),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_isin_number_string(lmdb_version_store_v1, df, vals):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    symbol = "test_filter_isin_number_string"
    lib.write(symbol, df)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isin_signed(lmdb_version_store_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test(lmdb_version_store_v1, "test_filter_numeric_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isin_unsigned(lmdb_version_store_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test(lmdb_version_store_v1, "test_filter_numeric_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
@pytest.mark.skipif(PANDAS_VERSION < Version("2.0.0"), reason="Early Pandas filtering does not handle unsigned well")
def test_filter_numeric_isnotin_unsigned(lmdb_version_store_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_filter_test(lmdb_version_store_v1, "test_filter_numeric_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isnotin_signed(lmdb_version_store_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_filter_test(lmdb_version_store_v1, "test_filter_numeric_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()))
def test_filter_numeric_isnotin_empty_set(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    vals = []
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(vals)
    generic_filter_test(lmdb_version_store_v1, "test_filter_numeric_isnotin_empty_set", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_isin(lmdb_version_store_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()))
def test_filter_string_isin_empty_set(lmdb_version_store_v1, df):
    assume(not df.empty)
    vals = []
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_isin_empty_set", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_isnotin(lmdb_version_store_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()))
def test_filter_string_isnotin_empty_set(lmdb_version_store_v1, df):
    assume(not df.empty)
    vals = []
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_isnotin_empty_set", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_and(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < 5) & (q["b"] > 10)]
    pandas_query = "(a < 5) & (b > 10)"
    generic_filter_test(lmdb_version_store_v1, "test_filter_and", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_or(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < 5) | (q["b"] > 10)]
    pandas_query = "(a < 5) | (b > 10)"
    generic_filter_test(lmdb_version_store_v1, "test_filter_or", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_xor(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < 5) ^ (q["b"] > 10)]
    # Pandas doesn't support '^' for xor
    pandas_query = "((a < 5) & ~(b > 10)) | (~(a < 5) & (b > 10))"
    generic_filter_test(lmdb_version_store_v1, "test_filter_xor", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_add_col_val(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    pandas_query = "(a + {}) < 10".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_add_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_add_val_col(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    pandas_query = "({} + a) < 10".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_add_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_add_col_col(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + q["b"]) < 10]
    pandas_query = "(a + b) < 10"
    generic_filter_test(lmdb_version_store_v1, "test_filter_add_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_sub_col_val(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] - val) < 10]
    pandas_query = "(a - {}) < 10".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_sub_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_sub_val_col(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(val - q["a"]) < 10]
    pandas_query = "({} - a) < 10".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_sub_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_sub_col_col(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] - q["b"]) < 10]
    pandas_query = "(a - b) < 10"
    generic_filter_test(lmdb_version_store_v1, "test_filter_sub_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_times_col_val(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] * val) < 10]
    pandas_query = "(a * {}) < 10".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_times_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_times_val_col(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(val * q["a"]) < 10]
    pandas_query = "({} * a) < 10".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_times_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_times_col_col(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] * q["b"]) < 10]
    pandas_query = "(a * b) < 10"
    generic_filter_test(lmdb_version_store_v1, "test_filter_times_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=non_zero_numeric_type_strategies(),
)
def test_filter_divide_col_val(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] / val) < 10]
    pandas_query = "(a / {}) < 10".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_divide_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=non_zero_numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_divide_val_col(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(val / q["a"]) < 10]
    pandas_query = "({} / a) < 10".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_divide_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=non_zero_numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_divide_col_col(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] / q["b"]) < 10]
    pandas_query = "(a / b) < 10"
    generic_filter_test(lmdb_version_store_v1, "test_filter_divide_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=numeric_type_strategies())
def test_filter_arithmetic_string_number_col_val(lmdb_version_store_v1, df, val):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    symbol = "test_filter_arithmetic_string_number_col_val"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()), val=string_strategy)
def test_filter_arithmetic_string_number_val_col(lmdb_version_store_v1, df, val):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    symbol = "test_filter_arithmetic_string_number_val_col"
    lib.write(symbol, df)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=numeric_type_strategies())], index=range_indexes()
    )
)
def test_filter_arithmetic_string_number_col_col(lmdb_version_store_v1, df):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + q["b"]) < 10]
    symbol = "test_filter_arithmetic_string_number_val_col"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(q["b"] + q["a"]) < 10]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
def test_filter_arithmetic_string_string_col_val(lmdb_version_store_v1, df, val):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    symbol = "test_filter_arithmetic_string_string_col_val"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
def test_filter_arithmetic_string_string_val_col(lmdb_version_store_v1, df, val):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    symbol = "test_filter_arithmetic_string_string_val_col"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    )
)
def test_filter_arithmetic_string_string_col_col(lmdb_version_store_v1, df):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + q["b"]) < 10]
    symbol = "test_filter_arithmetic_string_string_col_col"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(q["b"] + q["a"]) < 10]
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_abs(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[abs(q["a"]) < val]
    pandas_query = "abs(a) < {}".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_abs", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
def test_filter_abs_string(lmdb_version_store_v1, df, val):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[abs(q["a"]) < val]
    symbol = "test_filter_abs_string"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_neg(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[-q["a"] < val]
    pandas_query = "-a < {}".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_neg", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
def test_filter_neg_string(lmdb_version_store_v1, df, val):
    lib = lmdb_version_store_v1
    assume(not df.empty)
    q = QueryBuilder()
    q = q[-q["a"] < val]
    symbol = "test_filter_neg_string"
    lib.write(symbol, df, dynamic_strings=True)
    with pytest.raises(UserInputException) as e_info:
        lib.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_filter_more_columns_than_fit_in_one_segment(lmdb_version_store_tiny_segment, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < q["c"]) | (q["a"] < q["b"]) | (q["b"] < q["c"])]
    pandas_query = "(a < c) | (a < b) | (b < c)"
    generic_filter_test(
        lmdb_version_store_tiny_segment, "test_filter_more_columns_than_fit_in_one_segment", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_filter_with_column_slicing(lmdb_version_store_tiny_segment, df):
    lib = lmdb_version_store_tiny_segment
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < q["c"]) | (q["a"] < q["b"]) | (q["b"] < q["c"])]
    pandas_query = "(a < c) | (a < b) | (b < c)"
    symbol = "test_filter_with_column_filtering"
    lib.write(symbol, df)
    expected = df.query(pandas_query).loc[:, ["a", "c"]]
    received = lib.read(symbol, columns=["a", "c"], query_builder=q).data
    assert np.array_equal(expected, received)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_not(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[~(q["a"] < val)]
    pandas_query = "~(a < {})".format(val)
    generic_filter_test(lmdb_version_store_v1, "test_filter_not", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
# @pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds. GH#1048")
def test_filter_string_equals_col_val(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] == val]
    pandas_query = "a == '{}'".format(val)
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_equals_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
# @pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds. GH#1048")
def test_filter_string_equals_val_col(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val == q["a"]]
    pandas_query = "'{}' == a".format(val)
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_equals_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    )
)
# @pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds. GH#1048")
def test_filter_string_equals_col_col(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] == q["b"]]
    pandas_query = "a == b"
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_equals_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
# @pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds. GH#1048")
def test_filter_string_not_equals_col_val(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] != val]
    pandas_query = "a != '{}'".format(val)
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_not_equals_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
# @pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds. GH#1048")
def test_filter_string_not_equals_val_col(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val != q["a"]]
    pandas_query = "'{}' != a".format(val)
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_not_equals_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    )
)
# @pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds. GH#1048")
def test_filter_string_not_equals_col_col(lmdb_version_store_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] != q["b"]]
    pandas_query = "a != b"
    generic_filter_test_strings(lmdb_version_store_v1, "test_filter_string_not_equals_col_col", df, q, pandas_query)


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    )
)
def test_filter_less_than_col_col(lmdb_version_store_dynamic_schema_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] < q["b"]]
    pandas_query = "a < b"
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema_v1, "test_filter_less_than_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_less_than_equals_col_val(lmdb_version_store_dynamic_schema_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] <= val]
    pandas_query = "a <= {}".format(val)
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema_v1, "test_filter_less_than_equals_col_val", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_less_than_equals_val_col(lmdb_version_store_dynamic_schema_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val <= q["a"]]
    pandas_query = "{} <= a".format(val)
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema_v1, "test_filter_less_than_equals_val_col", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    )
)
def test_filter_less_than_equals_col_col(lmdb_version_store_dynamic_schema_v1, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] <= q["b"]]
    pandas_query = "a <= b"
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema_v1, "test_filter_less_than_equals_col_col", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_greater_than_col_val(lmdb_version_store_dynamic_schema_v1, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] > val]
    pandas_query = "a > {}".format(val)
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema_v1, "test_filter_greater_than_col_val", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    ),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isin_signed(lmdb_version_store_dynamic_schema_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema_v1, "test_filter_numeric_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    ),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isin_unsigned(lmdb_version_store_dynamic_schema_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema_v1, "test_filter_numeric_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    ),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isnotin_signed(lmdb_version_store_dynamic_schema_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema_v1, "test_filter_numeric_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    ),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isnotin_unsigned(lmdb_version_store_dynamic_schema_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema_v1, "test_filter_numeric_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    ),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_isin(lmdb_version_store_dynamic_schema_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema_v1, "test_filter_string_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    )
)
def test_filter_string_isin_empty_set(lmdb_version_store_dynamic_schema_v1, df):
    assume(not df.empty)
    vals = []
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema_v1, "test_filter_string_isin_empty_set", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    ),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_isnotin(lmdb_version_store_dynamic_schema_v1, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema_v1, "test_filter_string_isnotin", df, q, pandas_query)
