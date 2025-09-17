"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import numpy as np
import pandas as pd
from pandas import DataFrame

from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException, SchemaException
from arcticdb.util.test import (
    assert_frame_equal,
    generic_aggregation_test,
    make_dynamic,
    common_sum_aggregation_dtype,
    valid_common_type,
)

pytestmark = pytest.mark.pipeline


def test_group_on_float_column_with_nans(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_group_on_float_column_with_nans"
    df = pd.DataFrame({"grouping_column": [1.0, 2.0, np.nan, 1.0, 2.0, 2.0], "agg_column": [1, 2, 3, 4, 5, 6]})
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"agg_column": "sum"})


# TODO: Add first and last once un-feature flagged
@pytest.mark.parametrize("aggregator", ("sum", "min", "max", "mean", "count"))
def test_aggregate_float_columns_with_nans(lmdb_version_store_v1, aggregator):
    lib = lmdb_version_store_v1
    symbol = "test_aggregate_float_columns_with_nans"
    df = pd.DataFrame(
        {
            "grouping_column": 3 * ["some nans", "only nans"],
            "agg_column": [1.0, np.nan, 2.0, np.nan, np.nan, np.nan],
        }
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"agg_column": aggregator})


def test_count_aggregation(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_count_aggregation"
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2", "group_3"],
            "to_count": [100, 1, 3, 2, 2, np.nan],
        },
        index=np.arange(6),
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_count": "count"})


@pytest.mark.skip(reason="Feature flagged off until working with string columns and dynamic schema")
def test_first_aggregation(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_first_aggregation"
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_2", "group_4", "group_2", "group_1", "group_3", "group_1"],
            "to_first": [100.0, np.nan, np.nan, 2.7, 1.4, 5.8, 3.45],
        },
        index=np.arange(7),
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_first": "first"})


@pytest.mark.skip(reason="Feature flagged off until working with string columns and dynamic schema")
def test_first_agg_with_append(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_first_agg_with_append"
    df_0 = pd.DataFrame({"grouping_column": [0], "to_first": [10.0]})
    df_1 = pd.DataFrame({"grouping_column": [1], "to_first": [30.0]})
    df_2 = pd.DataFrame({"grouping_column": [0], "to_first": [20.0]})
    lib.write(symbol, df_0)
    lib.append(symbol, df_1)
    lib.append(symbol, df_2)
    generic_aggregation_test(lib, symbol, pd.concat([df_0, df_1, df_2]), "grouping_column", {"to_first": "first"})


@pytest.mark.skip(reason="Feature flagged off until working with string columns and dynamic schema")
def test_last_aggregation(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_last_aggregation"
    df = DataFrame(
        {
            "grouping_column": [
                "group_1",
                "group_2",
                "group_4",
                "group_5",
                "group_2",
                "group_1",
                "group_3",
                "group_1",
                "group_5",
            ],
            "to_last": [100.0, 2.7, np.nan, np.nan, np.nan, 1.4, 5.8, 3.45, 6.9],
        },
        index=np.arange(9),
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_last": "last"})


@pytest.mark.skip(reason="Feature flagged off until working with string columns and dynamic schema")
def test_last_agg_with_append(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_last_agg_with_append"
    df_0 = pd.DataFrame({"grouping_column": [0], "to_last": [10.0]})
    df_1 = pd.DataFrame({"grouping_column": [1], "to_last": [30.0]})
    df_2 = pd.DataFrame({"grouping_column": [0], "to_last": [20.0]})
    lib.write(symbol, df_0)
    lib.append(symbol, df_1)
    lib.append(symbol, df_2)
    generic_aggregation_test(lib, symbol, pd.concat([df_0, df_1, df_2]), "grouping_column", {"to_last": "last"})


def test_sum_aggregation(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_sum_aggregation"
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"], "to_sum": [1, 1, 2, 2, 2]},
        index=np.arange(5),
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_sum": "sum"})


def test_sum_aggregation_bool(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_sum_aggregation"
    df = DataFrame(
        {
            "grouping_column": ["0", "0", "0", "1", "1", "2", "2", "3", "4"],
            "to_sum": [True, False, True, True, True, False, False, True, False],
        },
        index=np.arange(9),
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_sum": "sum"})


def test_mean_aggregation(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_mean_aggregation"
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"], "to_mean": [1, 1, 2, 2, 2]},
        index=np.arange(5),
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_mean": "mean"})


def test_mean_aggregation_float(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_mean_aggregation_float"
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"],
            "to_mean": [1.1, 1.4, 2.5, 2.2, 2.2],
        },
        index=np.arange(5),
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_mean": "mean"})


def test_mean_aggregation_timestamp(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_mean_aggregation_float"
    df = DataFrame(
        {
            "grouping_column": ["3", "2", "1", "0", "3", "1", "2", "0", "2", "0", "1", "3", "5", "4"],
            "to_mean": [
                pd.Timestamp(0),
                pd.Timestamp(-4),
                pd.Timestamp(5),
                pd.Timestamp(1),
                pd.Timestamp(-6),
                pd.Timestamp(0),
                pd.Timestamp(-5),
                pd.Timestamp(5),
                pd.Timestamp(-1),
                pd.Timestamp(4),
                pd.Timestamp(6),
                pd.Timestamp(-5),
                pd.Timestamp(-10),
                pd.Timestamp(10),
            ],
        },
        index=np.arange(14),
    )
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_mean": "mean"})


def test_named_agg(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_named_agg"
    gen = np.random.default_rng()
    df = DataFrame({"grouping_column": [1, 1, 1, 2, 3, 4], "agg_column": gen.random(6)})
    lib.write(symbol, df)
    expected = df.groupby("grouping_column").agg(
        agg_column_sum=pd.NamedAgg("agg_column", "sum"),
        agg_column_mean=pd.NamedAgg("agg_column", "mean"),
        agg_column=pd.NamedAgg("agg_column", "min"),
    )
    expected = expected.reindex(columns=sorted(expected.columns))
    q = (
        QueryBuilder()
        .groupby("grouping_column")
        .agg(
            {
                "agg_column_sum": ("agg_column", "sum"),
                "agg_column_mean": ("agg_column", "MEAN"),
                "agg_column": "MIN",
            }
        )
    )
    received = lib.read(symbol, query_builder=q).data
    received = received.reindex(columns=sorted(received.columns))
    received.sort_index(inplace=True)
    assert_frame_equal(expected, received, check_dtype=False)


def test_max_minus_one(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_max_minus_one"
    df = pd.DataFrame({"grouping_column": ["thing"], "to_max": [-1]})
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_max": "max"})


def test_group_empty_dataframe(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_group_empty_dataframe"
    df = DataFrame({"grouping_column": [], "to_mean": []})
    lib.write(symbol, df)
    q = QueryBuilder().groupby("grouping_column").agg({"to_mean": "mean"})
    with pytest.raises(SchemaException):
        lib.read(symbol, query_builder=q)


def test_group_pickled_symbol(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_group_pickled_symbol"
    lib.write(symbol, np.arange(100).tolist())
    assert lib.is_symbol_pickled(symbol)
    q = QueryBuilder().groupby("grouping_column").agg({"to_mean": "mean"})
    with pytest.raises(InternalException):
        _ = lib.read(symbol, query_builder=q)


def test_group_column_not_present(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_group_column_not_present"
    df = DataFrame({"a": np.arange(2)}, index=np.arange(2))
    lib.write(symbol, df)
    q = QueryBuilder().groupby("grouping_column").agg({"to_mean": "mean"})
    with pytest.raises(SchemaException):
        lib.read(symbol, query_builder=q)


def test_group_column_splitting(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_group_column_splitting"
    df = DataFrame(
        {
            "grouping_column": [1, 2, 3, 4, 1, 2, 3, 4],
            "sum1": [1, 2, 3, 4, 1, 2, 3, 4],
            "max1": [1, 2, 3, 4, 1, 2, 3, 4],
            "sum2": [2, 3, 4, 5, 2, 3, 4, 5],
            "max2": [2, 3, 4, 5, 2, 3, 4, 5],
        }
    )
    lib.write(symbol, df)
    generic_aggregation_test(
        lib,
        symbol,
        df,
        "grouping_column",
        {"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"},
    )


def test_group_column_splitting_strings(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_group_column_splitting"
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2", "group_1", "group_2", "group_1"],
            "sum1": [1, 2, 3, 4, 1, 2, 3, 4],
            "max1": [1, 2, 3, 4, 1, 2, 3, 4],
            "sum2": [2, 3, 4, 5, 2, 3, 4, 5],
            "max2": [2, 3, 4, 5, 2, 3, 4, 5],
        }
    )
    lib.write(symbol, df)
    generic_aggregation_test(
        lib,
        symbol,
        df,
        "grouping_column",
        {"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"},
    )


def test_aggregation_with_nones_and_nans_in_string_grouping_column(version_store_factory):
    lib = version_store_factory(column_group_size=2, segment_row_size=2, dynamic_strings=True)
    symbol = "test_aggregation_with_nones_and_nans_in_string_grouping_column"
    # Structured so that the row-slices of the grouping column contain:
    # 1 - All strings
    # 2 - Strings and Nones
    # 3 - Strings and NaNs
    # 4 - All Nones
    # 5 - All NaNs
    # 6 - Nones and NaNs
    df = DataFrame(
        {
            "grouping_column": [
                "group_1",
                "group_2",
                "group_1",
                None,
                np.nan,
                "group_2",
                None,
                None,
                np.nan,
                np.nan,
                None,
                np.nan,
            ],
            "to_sum": np.arange(12),
        },
        index=np.arange(12),
    )
    lib.write(symbol, df, dynamic_strings=True)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_sum": "sum"})


def test_doctring_example_query_builder_groupby_max(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = DataFrame({"grouping_column": ["group_1", "group_1", "group_1"], "to_max": [1, 5, 4]}, index=np.arange(3))
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_max": "max"})

    lib.write("symbol", df)
    res = lib.read("symbol", query_builder=q)
    df = pd.DataFrame({"to_max": [5]}, index=["group_1"])
    df.index.rename("grouping_column", inplace=True)
    assert_frame_equal(res.data, df)


def test_docstring_example_query_builder_groupby_max_and_mean(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1"], "to_mean": [1.1, 1.4, 2.5], "to_max": [1.1, 1.4, 2.5]},
        index=np.arange(3),
    )
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_max": "max", "to_mean": "mean"})

    lib.write("symbol", df)
    res = lib.read("symbol", query_builder=q).data
    res.sort_index(axis=1, inplace=True)
    df = pd.DataFrame({"to_max": [2.5], "to_mean": [(1.1 + 1.4 + 2.5) / 3]}, index=["group_1"])
    df.index.rename("grouping_column", inplace=True)
    df.sort_index(axis=1, inplace=True)
    assert_frame_equal(res, df)


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


def test_count_aggregation_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_count_aggregation_dynamic"
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2", "group_3"],
            "to_count": [100, 1, 3, 2, 2, np.nan],
        },
        index=np.arange(6),
    )
    df, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_count": "count"})


@pytest.mark.xfail(reason="Not supported yet")
def test_first_aggregation_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_first_aggregation_dynamic"
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_2", "group_4", "group_2", "group_1", "group_3", "group_1"],
            "to_first": [100.0, np.nan, np.nan, 2.7, 1.4, 5.8, 3.45],
        },
        index=np.arange(7),
    )
    df, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_first": "first"})


@pytest.mark.xfail(reason="Not supported yet")
def test_last_aggregation_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_last_aggregation_dynamic"
    df = DataFrame(
        {
            "grouping_column": [
                "group_1",
                "group_2",
                "group_4",
                "group_5",
                "group_2",
                "group_1",
                "group_3",
                "group_1",
                "group_5",
            ],
            "to_last": [100.0, 2.7, np.nan, np.nan, np.nan, 1.4, 5.8, 3.45, 6.9],
        },
        index=np.arange(9),
    )
    df, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_last": "last"})


def test_sum_aggregation_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_sum_aggregation_dynamic"
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"], "to_sum": [1, 1, 2, 2, 2]},
        index=np.arange(5),
    )
    df, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_sum": "sum"})


def test_sum_aggregation_dynamic_bool_missing_aggregated_column(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_sum_aggregation_dynamic"
    df = DataFrame(
        {"grouping_column": ["group_1", "group_2"], "to_sum": [True, False]},
        index=np.arange(2),
    )
    lib.write(symbol, df)
    lib.append(symbol, pd.DataFrame({"grouping_column": ["group_1", "group_2"]}, index=np.arange(2)))
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_sum": "sum"})


def test_sum_aggregation_with_range_index_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_sum_aggregation_with_range_index_dynamic"
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"], "to_sum": [1, 1, 2, 2, 2]}
    )
    df, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_sum": "sum"})


def test_group_empty_dataframe_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_group_empty_dataframe_dynamic"
    df = DataFrame({"grouping_column": [], "to_mean": []})
    lib.write(symbol, df)
    q = QueryBuilder().groupby("grouping_column").agg({"to_mean": "mean"})
    with pytest.raises(SchemaException):
        lib.read(symbol, query_builder=q)


def test_group_pickled_symbol_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_group_pickled_symbol_dynamic"
    lib.write(symbol, np.arange(100).tolist())
    assert lib.is_symbol_pickled(symbol)
    q = QueryBuilder().groupby("grouping_column").agg({"to_mean": "mean"})
    with pytest.raises(InternalException):
        lib.read(symbol, query_builder=q)


def test_group_column_not_present_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_group_column_not_present_dynamic"
    df = DataFrame({"a": np.arange(2)}, index=np.arange(2))
    lib.write(symbol, df)
    q = QueryBuilder().groupby("grouping_column").agg({"to_mean": "mean"})
    with pytest.raises(SchemaException):
        lib.read(symbol, query_builder=q)


@pytest.mark.parametrize("agg", ("max", "min", "mean", "sum"))
def test_segment_without_aggregation_column(lmdb_version_store_dynamic_schema_v1, agg):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_segment_without_aggregation_column"
    write_df = pd.DataFrame({"grouping_column": ["group_0"], "aggregation_column": [10330.0]})
    lib.write(symbol, write_df)
    append_df = pd.DataFrame({"grouping_column": ["group_1"]})
    lib.append(symbol, append_df)
    generic_aggregation_test(
        lib, symbol, pd.concat([write_df, append_df]), "grouping_column", {"aggregation_column": agg}
    )


def test_minimal_repro_type_change(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_minimal_repro_type_change"
    write_df = pd.DataFrame({"grouping_column": ["group_1"], "to_sum": [np.uint8(1)]})
    lib.write(symbol, write_df)
    append_df = pd.DataFrame({"grouping_column": ["group_1"], "to_sum": [1.5]})
    lib.append(symbol, append_df)
    generic_aggregation_test(lib, symbol, pd.concat([write_df, append_df]), "grouping_column", {"to_sum": "sum"})


def test_minimal_repro_type_change_max(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_minimal_repro_type_change_max"
    write_df = pd.DataFrame({"grouping_column": ["group_1"], "to_max": [np.uint8(1)]})
    lib.write(symbol, write_df)
    append_df = pd.DataFrame({"grouping_column": ["group_1"], "to_max": [0.5]})
    lib.append(symbol, append_df)
    generic_aggregation_test(lib, symbol, pd.concat([write_df, append_df]), "grouping_column", {"to_max": "max"})


def test_minimal_repro_type_sum_similar_string_group_values(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_minimal_repro_type_sum_similar_string_group_values"
    df = pd.DataFrame({"grouping_column": ["0", "000"], "to_sum": [1.0, 1.0]})
    lib.write(symbol, df)
    generic_aggregation_test(lib, symbol, df, "grouping_column", {"to_sum": "sum"})


def test_aggregation_grouping_column_missing_from_row_group(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_aggregation_grouping_column_missing_from_row_group"
    write_df = DataFrame(
        {"to_sum": [1, 2], "grouping_column": ["group_1", "group_2"]},
        index=np.arange(2),
    )
    lib.write(symbol, write_df)
    append_df = DataFrame(
        {"to_sum": [3, 4]},
        index=np.arange(2, 4),
    )
    lib.append(symbol, append_df)
    generic_aggregation_test(lib, symbol, pd.concat([write_df, append_df]), "grouping_column", {"to_sum": "sum"})


@pytest.mark.parametrize(
    "first_dtype,",
    [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64, np.float32, np.float64, bool],
)
@pytest.mark.parametrize(
    "second_dtype",
    [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64, np.float32, np.float64, bool],
)
@pytest.mark.parametrize("first_group", ["0", "1"])
@pytest.mark.parametrize("second_group", ["0", "1"])
def test_sum_aggregation_type(
    lmdb_version_store_dynamic_schema_v1, first_dtype, second_dtype, first_group, second_group
):
    """
    Sum aggregation promotes to the largest type of the respective category. int -> int64, uint -> uint64, float -> float64
    Dynamic schema allows mixing int and uint. In the case of sum aggregation, this will require mixing uint64 and int64
    in the end segment, and those do not have a common type. In that case we use int64 (pyarrow does the same). In this
    test we test all configurations of dtypes and grouping options (same group vs different group)
    """
    lib = lmdb_version_store_dynamic_schema_v1
    df1 = pd.DataFrame({"grouping_column": [first_group], "to_sum": np.array([1], first_dtype)})
    df2 = pd.DataFrame({"grouping_column": [second_group], "to_sum": np.array([1], second_dtype)})
    lib.write("sym", df1)
    if valid_common_type(first_dtype, second_dtype) is None:
        with pytest.raises(SchemaException):
            lib.append("sym", df2)
    else:
        lib.append("sym", df2)
        q = QueryBuilder()
        q = q.groupby("grouping_column").agg({"to_sum": "sum"})
        data = lib.read("sym", query_builder=q).data
        expected_type = common_sum_aggregation_dtype(first_dtype, second_dtype)
        if first_group == second_group:
            expected_df = pd.DataFrame({"to_sum": np.array([2], expected_type)}, index=[first_group])
        else:
            expected_df = pd.DataFrame({"to_sum": np.array([1, 1], expected_type)}, index=["1", "0"])
        expected_df.index.name = "grouping_column"
        expected_df.sort_index(inplace=True)
        data.sort_index(inplace=True)
        assert_frame_equal(expected_df, data, check_dtype=True)


@pytest.mark.parametrize("extremum", ["min", "max"])
@pytest.mark.parametrize("dtype, default_value", [(np.int32, 0), (np.float32, np.nan), (bool, False)])
def test_extremum_aggregation_with_missing_aggregation_column(
    lmdb_version_store_dynamic_schema_v1, extremum, dtype, default_value
):
    """
    Test that a sparse column will be backfilled with the correct values.
    d1 will be skipped because there is no grouping colum, df2 will form the first row which. The first row is sparse
    because the aggregation column is missing, d2 will be the second row which will be dense and not backfilled.
    """
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "sym"
    df1 = pd.DataFrame({"agg_column": np.array([0, 0], dtype)})
    df2 = pd.DataFrame({"grouping_column": ["a"]})
    df3 = pd.DataFrame({"grouping_column": ["b"], "agg_column": np.array([0], dtype)})
    for df in [df1, df2, df3]:
        lib.append(sym, df)
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"agg_column": extremum})
    data = lib.read("sym", query_builder=q).data
    data = data.sort_index()
    expected = pd.DataFrame({"agg_column": np.array([default_value, 0], dtype)}, index=["a", "b"])
    expected.index.name = "grouping_column"
    expected = expected.sort_index()
    assert_frame_equal(data, expected)


def test_mean_timestamp_aggregation_with_missing_aggregation_column(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "sym"
    df1 = pd.DataFrame({"agg": [pd.Timestamp(1)], "grouping": [0]})
    df2 = pd.DataFrame({"grouping": [0, 1, 2]})
    df3 = pd.DataFrame({"agg": [pd.Timestamp(2), pd.Timestamp(5)], "grouping": [0, 1]})
    for df in [df1, df2, df3]:
        lib.append(sym, df)
    q = QueryBuilder()
    q = q.groupby("grouping").agg({"agg": "mean"})
    data = lib.read("sym", query_builder=q).data
    data.sort_index(inplace=True)
    expected = pd.DataFrame({"agg": [pd.Timestamp(1), pd.Timestamp(5), pd.NaT]}, index=[0, 1, 2])
    expected.index.name = "grouping"
    expected.sort_index(inplace=True)
    assert_frame_equal(data, expected)
