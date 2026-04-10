import numpy as np
import pytest
import pandas as pd

from arcticdb.util.test import assert_frame_equal, query_stats_operation_count
from arcticdb.version_store.processing import QueryBuilder
import arcticdb.toolbox.query_stats as qs


def get_table_data_read_count():
    stats = qs.get_query_stats()
    return query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA")


sym = "sym"


@pytest.mark.parametrize(
    "isin_values,expected_reads",
    [
        pytest.param([1, 3, 15, 35], 0, id="all_outside_both"),
        pytest.param([1, 7, 35], 1, id="hits_seg0_misses_seg1"),
        pytest.param([1, 3, 25], 1, id="hits_seg1_misses_seg0"),
    ],
)
def test_column_stats_isin_per_element_pruning(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, isin_values, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [5, 10]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [20, 30]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[q["col_1"].isin(isin_values)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1])
    expected = full_df[full_df["col_1"].isin(isin_values)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"


@pytest.mark.parametrize(
    "query_expr,pandas_expr,expected_reads",
    [
        pytest.param(
            lambda q: q["col"].isin([np.nan, 5.0]),
            lambda df: df["col"].isin([np.nan, 5.0]),
            1,
            id="isin_nan_and_in_range_value",
        ),
        pytest.param(
            lambda q: q["col"].isin([np.nan, 50.0]),
            lambda df: df["col"].isin([np.nan, 50.0]),
            0,
            id="isin_nan_and_out_of_range_value",
        ),
        pytest.param(
            lambda q: q["col"].isnotin([np.nan, 5.0]),
            lambda df: ~df["col"].isin([np.nan, 5.0]),
            2,
            id="isnotin_nan_and_in_range_value",
        ),
        pytest.param(
            lambda q: q["col"].isnotin([np.nan, 50.0]),
            lambda df: ~df["col"].isin([np.nan, 50.0]),
            2,
            id="isnotin_nan_and_out_of_range_value",
        ),
    ],
)
def test_column_stats_isin_with_nan_in_set(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr, pandas_expr, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col": [1.0, 10.0]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col": [20.0, 30.0]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1])
    expected = full_df[pandas_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"


@pytest.mark.parametrize("value_set,expected_reads", [([np.nan, 10.0], 1), ([np.nan, np.nan], 0), ([10.0, 10.0], 1)])
def test_column_stats_isin_all_nan_segment(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, value_set, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col": [np.nan, np.nan]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col": [10.0, 20.0]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[q["col"].isin(value_set)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    overall_df = pd.concat([df0, df1])
    # Our isin filtering drops NaN
    expected = overall_df[overall_df["col"].isin(value_set)].dropna()
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads


@pytest.mark.parametrize(
    "query_expr,pandas_expr,expected_reads",
    [
        # seg0=[1,2] (min=1,max=2), seg1=[5,5] (min=5,max=5)
        pytest.param(
            lambda q: q["col_1"].isin([3, 5, 6]), lambda df: df["col_1"].isin([3, 5, 6]), 1, id="isin_prunes_seg0"
        ),
        pytest.param(lambda q: q["col_1"].isin([1, 2]), lambda df: df["col_1"].isin([1, 2]), 1, id="isin_prunes_seg1"),
        pytest.param(
            lambda q: q["col_1"].isin([100, 200]), lambda df: df["col_1"].isin([100, 200]), 0, id="isin_prunes_both"
        ),
        pytest.param(
            lambda q: q["col_1"].isin([1, 2, 5, 6]),
            lambda df: df["col_1"].isin([1, 2, 5, 6]),
            2,
            id="isin_overlaps_both",
        ),
        pytest.param(
            lambda q: ~q["col_1"].isin([5, 6, 7]),
            lambda df: ~df["col_1"].isin([5, 6, 7]),
            1,
            id="negated_isin_prunes_seg1",
        ),
        pytest.param(
            lambda q: q["col_1"].isnotin([5, 6, 7]),
            lambda df: ~df["col_1"].isin([5, 6, 7]),
            1,
            id="isnotin_prunes_seg1",
        ),
        pytest.param(
            lambda q: q["col_1"].isnotin([100, 200]),
            lambda df: ~df["col_1"].isin([100, 200]),
            2,
            id="isnotin_no_pruning_disjoint",
        ),
        pytest.param(lambda q: q["col_1"].isnotin([5]), lambda df: ~df["col_1"].isin([5]), 1, id="isnotin_prunes_seg1"),
        pytest.param(
            lambda q: ~q["col_1"].isnotin([5]), lambda df: df["col_1"].isin([5]), 1, id="negated_isnotin_prunes_seg0"
        ),
    ],
)
def test_column_stats_isin_isnotin(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr, pandas_expr, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [5, 5]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1])
    expected = full_df[pandas_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"


@pytest.mark.parametrize(
    "dtype,seg0_vals,seg1_vals,query_expr,pandas_expr,expected_reads",
    [
        pytest.param(
            np.uint8,
            [0, 1],
            [254, 255],
            lambda q: q["col_1"].isin([256]),
            lambda df: df["col_1"].isin([256]),
            0,
            id="uint8_above_range",
        ),
        pytest.param(
            np.uint8,
            [0, 1],
            [254, 255],
            lambda q: q["col_1"].isin([-1]),
            lambda df: df["col_1"].isin([-1]),
            0,
            id="uint8_negative",
        ),
        pytest.param(
            np.uint8,
            [0, 1],
            [254, 255],
            lambda q: q["col_1"].isin([1, 254]),
            lambda df: df["col_1"].isin([1, 254]),
            2,
            id="uint8_in_range_both_segs",
        ),
        pytest.param(
            np.int8,
            [-128, -1],
            [0, 127],
            lambda q: q["col_1"].isin([128]),
            lambda df: df["col_1"].isin([128]),
            0,
            id="int8_above_range",
        ),
        pytest.param(
            np.int8,
            [-128, -1],
            [0, 127],
            lambda q: q["col_1"].isin([-129]),
            lambda df: df["col_1"].isin([-129]),
            0,
            id="int8_below_range",
        ),
        pytest.param(
            np.uint16,
            [0, 1],
            [65534, 65535],
            lambda q: q["col_1"].isin([65536]),
            lambda df: df["col_1"].isin([65536]),
            0,
            id="uint16_above_range",
        ),
        pytest.param(
            np.uint64,
            [50, 100],
            [200, 300],
            lambda q: q["col_1"].isin([50, 75]),
            lambda df: df["col_1"].isin([50, 75]),
            1,
            id="uint64_positive_only",
        ),
        pytest.param(
            np.uint64,
            [50, 100],
            [200, 300],
            lambda q: q["col_1"].isin([-1, 50]),
            lambda df: df["col_1"].isin([-1, 50]),
            1,
            id="uint64_negative_with_valid_match",
        ),
        pytest.param(
            np.uint64,
            [50, 100],
            [200, 300],
            lambda q: q["col_1"].isin([-1]),
            lambda df: df["col_1"].isin([-1]),
            0,
            id="uint64_negative_only",
        ),
    ],
)
def test_column_stats_isin_mixed_types(
    in_memory_version_store,
    clear_query_stats,
    column_stats_filtering_enabled,
    dtype,
    seg0_vals,
    seg1_vals,
    query_expr,
    pandas_expr,
    expected_reads,
):
    lib = in_memory_version_store

    idx0 = pd.date_range("2000-01-01", periods=len(seg0_vals))
    idx1 = pd.date_range("2000-01-03", periods=len(seg1_vals))
    df0 = pd.DataFrame({"col_1": np.array(seg0_vals, dtype=dtype)}, index=idx0)
    df1 = pd.DataFrame({"col_1": np.array(seg1_vals, dtype=dtype)}, index=idx1)

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1])
    expected = full_df[pandas_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"


def test_column_stats_isin_with_and(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [10, 20]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [30, 40]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5, 6], "col_2": [50, 60]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[(q["col_1"] > 2) & q["col_2"].isin([30, 40])]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1, df2])
    expected = full_df[(full_df["col_1"] > 2) & full_df["col_2"].isin([30, 40])]
    assert_frame_equal(expected, result)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read(s), got {table_data_reads}"
