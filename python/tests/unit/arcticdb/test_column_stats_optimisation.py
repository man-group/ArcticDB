from datetime import datetime

import numpy as np
import pytest
from arcticdb_ext.storage import KeyType

from arcticdb.util.test import assert_frame_equal, query_stats_operation_count
from arcticdb.version_store.processing import QueryBuilder
import arcticdb.toolbox.query_stats as qs
import pandas as pd

from arcticdb_ext.exceptions import UserInputException


def get_table_data_read_count():
    """Get the number of TABLE_DATA keys read from query stats."""
    stats = qs.get_query_stats()
    return query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA")


def get_column_stats_read_count():
    """Get the number of COLUMN_STATS keys read from query stats."""
    stats = qs.get_query_stats()
    return query_stats_operation_count(stats, "Memory_GetObject", "COLUMN_STATS")


sym = "sym"


@pytest.mark.parametrize(
    "query_expr, expected_reads",
    [
        (lambda q: q["col_1"] > 2, 1),  # seg0 max=2 <= 2, pruned
        (lambda q: q["col_1"] > 4, 0),  # both segments pruned: max <= 4
        (lambda q: q["col_1"] < 2, 1),  # seg1 min=3 >= 2, pruned
        (lambda q: q["col_1"] == 3, 1),  # seg0 max=2 < 3, pruned
        (lambda q: q["col_1"] >= 1, 2),  # no pruning possible
    ],
    ids=["gt_2", "gt_4", "lt_2", "eq_3", "gte_1"],
)
def test_column_stats_query_optimisation(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr, expected_reads
):
    """Test that column stats are used to optimize QueryBuilder queries by pruning segments."""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

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
    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"


def test_column_stats_query_optimisation_disabled(in_memory_version_store, clear_query_stats):
    """Check that we don't use column stats by default (yet)."""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    assert get_table_data_read_count() == 2
    assert get_column_stats_read_count() == 0
    assert_frame_equal(df1, result)


def test_column_stats_query_optimisation_no_stats(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    """Check that everything still works if there aren't any column stats."""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    qs.enable()
    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()
    assert_frame_equal(df1, result)
    assert table_data_reads == 2


def test_column_stats_query_optimisation_column_not_in_stats(
    lmdb_version_store_tiny_segment, column_stats_filtering_enabled
):
    """
    Test that queries work when column stats exist but not for the filtered column.
    """
    lib = lmdb_version_store_tiny_segment

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [10, 20]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [30, 40]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    # Create column stats for col_2 only, not col_1
    lib.create_column_stats(sym, {"col_2": {"MINMAX"}})

    # Query on col_1 (no stats) should still work
    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    result = lib.read(sym, query_builder=q).data
    assert_frame_equal(df1, result)


def test_column_stats_query_optimisation_empty_segment(
    in_memory_version_store_tiny_segment, column_stats_filtering_enabled
):
    lib = in_memory_version_store_tiny_segment

    df0 = pd.DataFrame({"col_1": []}, dtype=np.int64)
    df1 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df2 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    assert lib.has_symbol(sym)  # check the empty write didn't fail silently
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    qs.reset_stats()
    lib.read(sym, query_builder=q)
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 1


@pytest.mark.parametrize(
    "query_expr,expected_reads",
    [
        pytest.param(lambda q: ((q["col_1"] > 2) & (q["col_2"] < 50)), 1, id="and_prunes_both_sides"),
        pytest.param(lambda q: ((q["col_1"] >= 3) & (q["col_2"] > 35)), 2, id="and_prunes_one_side"),
        pytest.param(lambda q: ((q["col_1"] == 1) & (q["col_2"] == 10)), 1, id="and_eq_single_segment"),
        pytest.param(lambda q: ((q["col_1"] > 6) & (q["col_2"] > 0)), 0, id="and_all_pruned"),
        pytest.param(lambda q: (q["col_1"] > 4), 1, id="single_col_filter_with_multi_col_stats"),
    ],
)
def test_column_stats_query_optimisation_multiple_filters(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr, expected_reads
):
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
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1, df2])
    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"
    assert get_column_stats_read_count() == 1


@pytest.mark.parametrize(
    "dtype,values_seg0,values_seg1,filter_val",
    [
        # Integer types
        (np.int8, [-100, -50], [50, 100], 0),  # filter > 0 prunes seg0
        (np.int16, [-1000, -500], [500, 1000], 0),
        (np.int32, [-100000, -50000], [50000, 100000], 0),
        (np.int64, [-(10**15), -(10**14)], [10**14, 10**15], 0),
        # Unsigned integer types
        (np.uint8, [1, 10], [200, 250], 100),  # filter > 100 prunes seg0
        (np.uint16, [1, 100], [50000, 60000], 1000),
        (np.uint32, [1, 1000], [3000000000, 4000000000], 2000000000),
        (np.uint64, [1, 1000], [10**18, 10**18 + 1000], 10**17),
        # Float types
        (np.float32, [1.5, 2.5], [10.5, 11.5], 5.0),  # filter > 5.0 prunes seg0
        (np.float64, [1.5e10, 2.5e10], [10.5e10, 11.5e10], 5.0e10),
    ],
)
def test_column_stats_query_optimisation_different_types(
    in_memory_version_store,
    clear_query_stats,
    column_stats_filtering_enabled,
    dtype,
    values_seg0,
    values_seg1,
    filter_val,
):
    lib = in_memory_version_store

    df0 = pd.DataFrame(
        {"col": np.array(values_seg0, dtype=dtype)}, index=pd.date_range("2000-01-01", periods=len(values_seg0))
    )
    df1 = pd.DataFrame(
        {"col": np.array(values_seg1, dtype=dtype)}, index=pd.date_range("2000-01-03", periods=len(values_seg1))
    )

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[q["col"] > filter_val]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read(s) for dtype {dtype.__name__}, got {table_data_reads}"
    assert_frame_equal(df1, result)


def test_column_stats_query_optimisation_with_date_range(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5, 6]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()

    # date_range restricts to segments 0 and 1, column filter col_1 > 2 prunes segment 0
    # Only segment 1 should be read
    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    date_range = (pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-04"))
    qs.reset_stats()
    result = lib.read(sym, query_builder=q, date_range=date_range).data
    table_data_reads = get_table_data_read_count()
    assert_frame_equal(df1, result)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"


@pytest.mark.parametrize(
    "col_stats,filter_exprs,expected_reads",
    [
        pytest.param(
            {"col_1": {"MINMAX"}},
            [lambda q: (q["col_1"] > 4) & (q["col_2"] > 7)],
            1,
            id="and_single_col_stats",
        ),
        pytest.param(
            {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}},
            [lambda q: (q["col_1"] > 4) & (q["col_2"] < 9)],
            0,
            id="and_both_col_stats",
        ),
        pytest.param(
            {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}},
            [lambda q: q["col_1"] > 4, lambda q: q["col_2"] < 9],
            0,
            id="chained_filter_clauses",
        ),
    ],
)
def test_column_stats_and_filter_one_column_with_stats(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, col_stats, filter_exprs, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [6, 5]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [8, 7]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5, 6], "col_2": [10, 9]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, col_stats)

    qs.enable()
    q = QueryBuilder()
    for expr in filter_exprs:
        q = q[expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1, df2])
    for expr in filter_exprs:
        full_df = full_df[expr(full_df)]
    assert_frame_equal(result, full_df)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"


def test_column_stats_or_filter(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5, 6]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[(q["col_1"] < 2) | (q["col_1"] > 5)]
    result = lib.read(sym, query_builder=q).data

    table_data_reads = get_table_data_read_count()

    expected = pd.concat([df0, df1, df2])
    expected = expected[(expected["col_1"] < 2) | (expected["col_1"] > 5)]
    assert_frame_equal(result, expected)

    # Should read 2 segments (seg0 and seg2), seg1 is pruned
    assert table_data_reads == 2, f"Expected 2 TABLE_DATA reads but got {table_data_reads}"


def test_column_stats_negation(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    """Negation needs careful handling because if a block's statistics satisfy a comparison, we
    may still need to visit that block when the comparison is negated."""
    lib = in_memory_version_store

    df_0 = pd.DataFrame({"col_1": [1, 3]}, index=pd.date_range("2000-01-01", periods=2), dtype=np.float64)
    df_1 = pd.DataFrame({"col_1": [4, 6]}, index=pd.date_range("2000-01-03", periods=2), dtype=np.float64)
    df_2 = pd.DataFrame({"col_1": [6, 8]}, index=pd.date_range("2000-01-05", periods=2), dtype=np.float64)
    lib.write(sym, df_0)
    lib.append(sym, df_1)
    lib.append(sym, df_2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[~(q["col_1"] > 5)]
    result = lib.read(sym, query_builder=q).data

    table_data_reads = get_table_data_read_count()

    expected = pd.concat([df_0, df_1, df_2])
    expected = expected[~(expected["col_1"] > 5)]
    assert_frame_equal(result, expected)

    assert table_data_reads == 2


@pytest.mark.parametrize("filter_first", [True, False])
@pytest.mark.parametrize("extra_clause", ["PROJECTION", "RESAMPLE", "GROUPBY"])
def test_column_stats_projection_before_filter_disables_pruning(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, extra_clause, filter_first
):
    """Filter clause after a projection should NOT use column stats pruning at the moment."""
    lib = in_memory_version_store

    df_0 = pd.DataFrame({"col_1": [1, 3]}, index=pd.date_range("2000-01-01", periods=2), dtype=np.float64)
    df_1 = pd.DataFrame({"col_1": [4, 6]}, index=pd.date_range("2000-01-03", periods=2), dtype=np.float64)
    df_2 = pd.DataFrame({"col_1": [6, 8]}, index=pd.date_range("2000-01-05", periods=2), dtype=np.float64)

    lib.write(sym, df_0)
    lib.append(sym, df_1)
    lib.append(sym, df_2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()

    if filter_first:
        q = q[q["col_1"] > 4]

    if extra_clause == "PROJECTION":
        q = q.apply("col_1", q["col_1"] * 2)
    elif extra_clause == "RESAMPLE":
        q = q.resample("D").agg({"col_1": "first"})
    elif extra_clause == "GROUPBY":
        q = q.groupby("col_1").agg({"col_1": "min"})
    else:
        raise RuntimeError(f"Unexpected parameter {extra_clause}")

    if not filter_first:
        q = q[q["col_1"] > 4]

    lib.read(sym, query_builder=q)

    table_data_reads = get_table_data_read_count()
    expected_reads = 2 if filter_first else 3
    assert table_data_reads == expected_reads, f"Expected {expected_reads} was {table_data_reads}"
    assert get_column_stats_read_count() == (1 if filter_first else 0)


@pytest.mark.parametrize("append_type", (np.int32, np.float64))
def test_column_stats_dynamic_schema_column_type_varies(
    in_memory_version_store_dynamic_schema, clear_query_stats, column_stats_filtering_enabled, append_type
):
    """Test pruning when column type varies across segments (dynamic schema)."""
    lib = in_memory_version_store_dynamic_schema

    df_int8 = pd.DataFrame({"col_1": np.array([1, 2], dtype=np.int8)}, index=pd.date_range("2000-01-01", periods=2))
    df_different_type = pd.DataFrame(
        {"col_1": np.array([200, 300], dtype=append_type)}, index=pd.date_range("2000-01-03", periods=2)
    )
    lib.write(sym, df_int8)
    lib.append(sym, df_different_type)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})
    qs.enable()
    q = QueryBuilder()
    q = q[q["col_1"] > 50]

    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    expected = pd.concat([df_int8, df_different_type])
    expected = expected[expected["col_1"] > 50]
    assert_frame_equal(result, expected)

    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read but got {table_data_reads}"


def test_column_stats_dynamic_schema_new_column_added(
    in_memory_version_store_dynamic_schema, clear_query_stats, column_stats_filtering_enabled
):
    """Test when a column is added in later segments (not present in older segments)."""
    lib = in_memory_version_store_dynamic_schema

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [10, 20]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5, 6], "col_2": [30, 40]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_2": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[q["col_2"] > 25]
    result = lib.read(sym, query_builder=q).data

    table_data_reads = get_table_data_read_count()

    expected = pd.concat([df0, df1, df2])
    expected = expected[expected["col_2"] > 25]
    # Use check_dtype=False because NaN in dynamic schema causes float64 promotion
    assert_frame_equal(result, expected, check_dtype=False)

    assert table_data_reads == 1, f"Expected at most 1 TABLE_DATA reads but got {table_data_reads}"


@pytest.mark.parametrize(
    "query_expr,expected_reads",
    [
        (lambda q: q["col_1"] > 4, 1),
        (lambda q: 4 < q["col_1"], 1),
        (lambda q: q["col_1"] >= 6, 1),
        (lambda q: 6 <= q["col_1"], 1),
        (lambda q: q["col_1"] >= 7, 0),
        (lambda q: q["col_1"] >= 5, 1),
        (lambda q: q["col_1"] < 2, 1),
        (lambda q: 2 > q["col_1"], 1),
        (lambda q: q["col_1"] < 1, 0),
        (lambda q: q["col_1"] <= 1, 1),
        (lambda q: 1 >= q["col_1"], 1),
        (lambda q: q["col_1"] == 3, 1),
        (lambda q: 3 == q["col_1"], 1),
        (lambda q: q["col_1"] != 3, 3),
        (lambda q: 3 != q["col_1"], 3),
    ],
    ids=[
        "col_1 > 4",
        "4 < col_1",
        "col_1 >= 6",
        "6 <= col_1",
        "col_1 >= 7",
        "col_1 >= 5",
        "col_1 < 2",
        "2 > col_1",
        "col_1 < 1",
        "col_1 <= 1",
        "1 >= col_1",
        "col_1 == 3",
        "3 == col_1",
        "col_1 != 3",
        "3 != col_1",
    ],
)
def test_column_stats_comparison_operators(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr, expected_reads
):
    lib = in_memory_version_store
    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2), dtype=np.float64)
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2), dtype=np.float64)
    df2 = pd.DataFrame({"col_1": [6, 5]}, index=pd.date_range("2000-01-05", periods=2), dtype=np.float64)
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    qs.reset_stats()

    q = QueryBuilder()
    q = q[query_expr(q)]
    lib.read(sym, query_builder=q)

    table_data_reads = get_table_data_read_count()

    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA reads but got {table_data_reads}"


@pytest.mark.xfail(reason="Creating column stats on multi-indexed symbols is not supported yet")
def test_column_stats_multiindex_index_col(in_memory_version_store):
    """Test column stats creation and usage with a multi-index DataFrame, with column stats created
    on part of the multi-index."""
    lib = in_memory_version_store

    index0 = pd.MultiIndex.from_tuples(
        [(datetime(2000, 1, 1), "A"), (datetime(2000, 1, 1), "B")], names=["date", "category"]
    )
    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [10, 20]}, index=index0)

    index1 = pd.MultiIndex.from_tuples(
        [(datetime(2000, 1, 2), "C"), (datetime(2000, 1, 2), "D")], names=["date", "category"]
    )
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [30, 40]}, index=index1)

    lib.write(sym, df0)
    lib.append(sym, df1)

    column_stats_dict = {"category": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)


ROWCOUNT_INDEXES = [
    np.arange(0, 3, dtype=np.int64),
    np.arange(4, 6, dtype=np.int64),
]


STRING_INDEXES = [["a", "b", "c"], ["d", "e"]]

RANGE_INDEXES = [pd.RangeIndex(start=0, stop=3, step=1), pd.RangeIndex(start=3, stop=5, step=1)]


@pytest.mark.parametrize(
    "indexes", [ROWCOUNT_INDEXES, STRING_INDEXES, RANGE_INDEXES], ids=["rowcount", "string", "range"]
)
def test_column_stats_query_optimisation_index_types(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, indexes
):
    """Test how column stats filtering copes with different index types. Datetime indexes are covered repeatedly in other tests in this file."""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0], dtype=np.int64)
    df1 = pd.DataFrame({"col_1": [3, 4], "col2": [5, 6]}, index=indexes[1], dtype=np.int64)

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    # col_1 > 3 should only read segment 0
    q = QueryBuilder()
    q = q[q["col_1"] > 3]
    qs.reset_stats()
    lib.read(sym, query_builder=q)
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 0 only), got {table_data_reads}"


@pytest.mark.parametrize("create_stats", [True, False], ids=["with_stats", "no_stats"])
def test_column_stats_no_deadlock_single_thread(
    in_memory_version_store, column_stats_filtering_enabled, create_stats, tiny_thread_pool
):
    lib = in_memory_version_store
    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))
    lib.write(sym, df0)
    lib.append(sym, df1)
    if create_stats:
        lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    result = lib.read(sym, query_builder=q).data
    assert_frame_equal(df1, result)


DATETIME_INDEXES_THREE_SEGMENTS = [
    pd.date_range("2000-01-01", periods=2),
    pd.date_range("2000-01-03", periods=2),
    pd.date_range("2000-01-05", periods=2),
]

ROWCOUNT_INDEXES_THREE_SEGMENTS = [
    np.arange(0, 2, dtype=np.int64),
    np.arange(2, 4, dtype=np.int64),
    np.arange(4, 6, dtype=np.int64),
]

STRING_INDEXES_THREE_SEGMENTS = [["a", "b"], ["c", "d"], ["e", "f"]]

RANGE_INDEXES_THREE_SEGMENTS = [
    pd.RangeIndex(start=0, stop=2),
    pd.RangeIndex(start=2, stop=4),
    pd.RangeIndex(start=4, stop=6),
]

THREE_SEGMENT_INDEXES = [
    DATETIME_INDEXES_THREE_SEGMENTS,
    ROWCOUNT_INDEXES_THREE_SEGMENTS,
    STRING_INDEXES_THREE_SEGMENTS,
    RANGE_INDEXES_THREE_SEGMENTS,
]

THREE_SEGMENT_INDEX_IDS = ["datetime", "rowcount", "string", "range"]


@pytest.mark.parametrize("indexes", THREE_SEGMENT_INDEXES, ids=THREE_SEGMENT_INDEX_IDS)
def test_column_stats_with_column_slicing(
    in_memory_store_factory, clear_query_stats, column_stats_filtering_enabled, indexes
):
    lib = in_memory_store_factory(column_group_size=1, segment_row_size=10)

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [10, 20], "col_3": [100, 200]}, index=indexes[0], dtype=np.int64)
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [30, 40], "col_3": [300, 400]}, index=indexes[1], dtype=np.int64)
    df2 = pd.DataFrame({"col_1": [5, 6], "col_2": [50, 60], "col_3": [500, 600]}, index=indexes[2], dtype=np.int64)

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lt = lib.library_tool()
    data_keys = lt.find_keys_for_symbol(KeyType.TABLE_DATA, sym)
    # The index is stored in every block for datetime indexes even though the column slicing policy is "1"
    # The range index is not physically stored. Other indexes (eg string / integer index) are just saved
    # like normal columns in their own block.
    expected_data_keys = 9 if isinstance(indexes[0], pd.DatetimeIndex) or isinstance(indexes[0], pd.RangeIndex) else 12
    assert len(data_keys) == expected_data_keys

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    q = QueryBuilder()
    q = q[q["col_1"] > 4]

    qs.enable()
    qs.reset_stats()
    result = lib.read(sym, query_builder=q, columns=["col_1"]).data
    table_data_reads = get_table_data_read_count()
    # Get an extra read for the index if it's stored in its own segment
    expected_reads = 1 if isinstance(indexes[0], pd.DatetimeIndex) or isinstance(indexes[0], pd.RangeIndex) else 2
    assert table_data_reads == expected_reads, f"Expected 1 TABLE_DATA read, got {table_data_reads}"

    expected = df2[["col_1"]]

    if isinstance(indexes[0], pd.RangeIndex):
        expected.index = pd.RangeIndex(0, 2)
    assert_frame_equal(result, expected)


@pytest.mark.parametrize("indexes", THREE_SEGMENT_INDEXES, ids=THREE_SEGMENT_INDEX_IDS)
def test_column_stats_with_row_range(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, indexes
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=indexes[0], dtype=np.int64)
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=indexes[1], dtype=np.int64)
    df2 = pd.DataFrame({"col_1": [5, 6]}, index=indexes[2], dtype=np.int64)

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    q = QueryBuilder()
    q = q[q["col_1"] > 2]

    qs.enable()
    qs.reset_stats()
    result = lib.read(sym, query_builder=q, row_range=(0, 4)).data
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read, got {table_data_reads}"

    expected = df1
    if isinstance(indexes[0], pd.RangeIndex):
        expected.index = pd.RangeIndex(0, 2, 1)
    assert_frame_equal(result, expected)


def test_column_stats_with_date_range(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2), dtype=np.int64)
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2), dtype=np.int64)
    df2 = pd.DataFrame({"col_1": [5, 6]}, index=pd.date_range("2000-01-05", periods=2), dtype=np.int64)

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    q = QueryBuilder()
    q = q[q["col_1"] > 2]

    date_range = (pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-04"))

    qs.enable()
    qs.reset_stats()
    result = lib.read(sym, query_builder=q, date_range=date_range).data
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read, got {table_data_reads}"

    assert_frame_equal(result, df1)


@pytest.mark.parametrize("negated", (True, False))
def test_column_stats_bool_column_filters(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, negated
):
    lib = in_memory_version_store

    df_0 = pd.DataFrame({"col_1": [True, True]}, index=pd.date_range("2000-01-01", periods=2))
    df_1 = pd.DataFrame({"col_1": [False, True]}, index=pd.date_range("2000-01-03", periods=2))
    df_2 = pd.DataFrame({"col_1": [True, False]}, index=pd.date_range("2000-01-05", periods=2))
    df_3 = pd.DataFrame({"col_1": [False, False]}, index=pd.date_range("2000-01-07", periods=2))
    df_4 = pd.DataFrame({"col_1": [False, False]}, index=pd.date_range("2000-01-09", periods=2))
    lib.write(sym, df_0)
    lib.append(sym, df_1)
    lib.append(sym, df_2)
    lib.append(sym, df_3)
    lib.append(sym, df_4)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    if negated:
        q = q[~q["col_1"]]
    else:
        q = q[q["col_1"]]
    result = lib.read(sym, query_builder=q).data

    table_data_reads = get_table_data_read_count()

    expected = pd.concat([df_0, df_1, df_2, df_3, df_4])

    if negated:
        expected = expected[~expected["col_1"]]
    else:
        expected = expected[expected["col_1"]]
    assert_frame_equal(result, expected)

    # When negated=True, can prune off only the True, True block
    # When negated=False, can prune off only the two False, False blocks
    assert table_data_reads == (4 if negated else 3)


def test_column_stats_three_chained_filter_clauses(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    lib = in_memory_version_store

    df0 = pd.DataFrame(
        {"col_1": [1, 2], "col_2": [10, 20], "col_3": [100, 200]}, index=pd.date_range("2000-01-01", periods=2)
    )
    df1 = pd.DataFrame(
        {"col_1": [3, 4], "col_2": [30, 40], "col_3": [300, 400]}, index=pd.date_range("2000-01-03", periods=2)
    )
    df2 = pd.DataFrame(
        {"col_1": [5, 6], "col_2": [50, 60], "col_3": [500, 600]}, index=pd.date_range("2000-01-05", periods=2)
    )

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}, "col_3": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    q = q[q["col_2"] < 50]
    q = q[q["col_3"] > 200]

    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    assert_frame_equal(result, df1)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"


def test_column_stats_repeated_expressions(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    """Nonreg test: ConstMap.set_value is called repeatedly with name=Num(2) here. We shouldn't validate against the repetition."""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    q = q[q["col_1"] > 2]
    q = q[(q["col_1"] > 2) & (q["col_1"] > 2)]

    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    assert_frame_equal(result, df1)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"


def test_column_stats_nan_values(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col": [1.0, np.nan, 3.0]}, index=pd.date_range("2000-01-01", periods=3))
    df1 = pd.DataFrame({"col": [10.0, 20.0]}, index=pd.date_range("2000-01-04", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    q = QueryBuilder()
    q = q[q["col"] > 2]
    result = lib.read(sym, query_builder=q).data

    full_df = pd.concat([df0, df1])
    expected = full_df[full_df["col"] > 2]
    assert_frame_equal(expected, result)


def test_column_stats_nat_values(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store

    ts = pd.date_range("2000-01-01", periods=2)
    df0 = pd.DataFrame({"col": [pd.NaT, pd.NaT]}, index=ts)
    df1 = pd.DataFrame(
        {"col": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-06-01")]},
        index=pd.date_range("2000-01-03", periods=2),
    )

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    qs.enable()
    qs.reset_stats()

    q = QueryBuilder()
    q = q[q["col"] > pd.Timestamp("2024-01-01")]
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1])
    expected = full_df[full_df["col"] > pd.Timestamp("2024-01-01")]
    assert_frame_equal(expected, result)

    assert table_data_reads == 1


@pytest.mark.parametrize(
    "col_dtype,col_values_seg0,col_values_seg1,filter_val",
    [
        (np.int32, [1, 2], [3, 4], 2.5),
        (np.float64, [1.0, 2.0], [3.0, 4.0], 2),
    ],
    ids=["int_col_float_filter", "float_col_int_filter"],
)
def test_column_stats_cross_type_comparison(
    in_memory_version_store,
    clear_query_stats,
    column_stats_filtering_enabled,
    col_dtype,
    col_values_seg0,
    col_values_seg1,
    filter_val,
):
    lib = in_memory_version_store

    df0 = pd.DataFrame(
        {"col": np.array(col_values_seg0, dtype=col_dtype)}, index=pd.date_range("2000-01-01", periods=2)
    )
    df1 = pd.DataFrame(
        {"col": np.array(col_values_seg1, dtype=col_dtype)}, index=pd.date_range("2000-01-03", periods=2)
    )

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[q["col"] > filter_val]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1])
    expected = full_df[full_df["col"] > filter_val]
    assert_frame_equal(expected, result)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read, got {table_data_reads}"


@pytest.mark.parametrize(
    "filter_expr,expected_reads",
    [
        (lambda q: q["col"] > 0, 2),
        (lambda q: q["col"] < -3, 2),
        (lambda q: q["col"] >= -1, 3),
        (lambda q: q["col"] < -10, 0),
    ],
    ids=["gt_0", "lt_neg3", "gte_neg1", "lt_neg10"],
)
def test_column_stats_negative_value_ranges(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, filter_expr, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col": [-10, -1]}, index=pd.date_range("2000-01-01", periods=2), dtype=np.int64)
    df1 = pd.DataFrame({"col": [-5, 5]}, index=pd.date_range("2000-01-03", periods=2), dtype=np.int64)
    df2 = pd.DataFrame({"col": [1, 10]}, index=pd.date_range("2000-01-05", periods=2), dtype=np.int64)

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[filter_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1, df2])
    expected = full_df[filter_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA reads, got {table_data_reads}"


@pytest.mark.parametrize(
    "query_expr,expected_result_segs,expected_reads",
    [
        pytest.param(lambda q: q["col"] == 10, [1], 1, id="eq"),
        pytest.param(lambda q: q["col"] != 10, [0, 2], 2, id="ne"),
    ],
)
def test_column_stats_single_value_segments(
    in_memory_version_store,
    clear_query_stats,
    column_stats_filtering_enabled,
    query_expr,
    expected_result_segs,
    expected_reads,
):
    lib = in_memory_version_store

    dfs = [
        pd.DataFrame({"col": [5, 5]}, index=pd.date_range("2000-01-01", periods=2), dtype=np.int64),
        pd.DataFrame({"col": [10, 10]}, index=pd.date_range("2000-01-03", periods=2), dtype=np.int64),
        pd.DataFrame({"col": [15, 15]}, index=pd.date_range("2000-01-05", periods=2), dtype=np.int64),
    ]

    lib.write(sym, dfs[0])
    lib.append(sym, dfs[1])
    lib.append(sym, dfs[2])

    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    expected = pd.concat([dfs[i] for i in expected_result_segs])
    assert_frame_equal(result, expected)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} reads, got {table_data_reads}"


def test_column_stats_snapshot_read(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats(sym, {"col": {"MINMAX"}})
    lib.snapshot("snap1")

    qs.enable()

    q = QueryBuilder()
    q = q[q["col"] > 2]
    qs.reset_stats()
    result = lib.read(sym, as_of="snap1", query_builder=q).data
    table_data_reads = get_table_data_read_count()

    assert_frame_equal(result, df1)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read, got {table_data_reads}"


def test_column_stats_batch_read(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store

    sym1, sym2 = "batch_sym1", "batch_sym2"

    for s in [sym1, sym2]:
        df0 = pd.DataFrame({"col": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
        df1 = pd.DataFrame({"col": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))
        lib.write(s, df0)
        lib.append(s, df1)
        lib.create_column_stats(s, {"col": {"MINMAX"}})

    sym3 = "batch_sym3"
    sym3_df = pd.DataFrame({"col": [5, 6]}, index=pd.date_range("2000-01-05", periods=2))
    lib.write(sym3, sym3_df)  # no stats for this one

    qs.enable()

    q = QueryBuilder()
    q = q[q["col"] > 2]
    qs.reset_stats()
    results = lib.batch_read([sym1, sym2, sym3], query_builder=q)
    table_data_reads = get_table_data_read_count()

    expected = pd.DataFrame({"col": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))
    assert_frame_equal(results[sym1].data, expected)
    assert_frame_equal(results[sym2].data, expected)
    assert_frame_equal(results[sym3].data, sym3_df)
    # 3 symbols x 1 segment each = 3 reads
    assert table_data_reads == 3, f"Expected 3 TABLE_DATA reads (1 per symbol), got {table_data_reads}"


@pytest.mark.parametrize(
    "dtype",
    (
        np.int64,
        np.int32,
        np.int16,
        np.int8,
        np.uint64,
        np.uint32,
        np.uint16,
        np.uint8,
    ),
)
def test_column_stats_boundary_values_integral(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, dtype
):
    lib = in_memory_version_store

    test_cases = [
        (lambda q: q["col"] >= np.iinfo(dtype).min, 4),
        (lambda q: q["col"] <= np.iinfo(dtype).max, 4),
        (lambda q: q["col"] > np.iinfo(dtype).min, 3),
        (lambda q: q["col"] < np.iinfo(dtype).max, 3),
    ]

    df0 = pd.DataFrame(
        {"col": np.array([np.iinfo(dtype).min, np.iinfo(dtype).min], dtype=dtype)},
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame(
        {"col": np.array([np.iinfo(dtype).min + 1, np.iinfo(dtype).max], dtype=dtype)},
        index=pd.date_range("2000-01-03", periods=2),
    )
    df2 = pd.DataFrame(
        {"col": np.array([np.iinfo(dtype).max - 1, np.iinfo(dtype).max - 1], dtype=dtype)},
        index=pd.date_range("2000-01-05", periods=2),
    )
    df3 = pd.DataFrame(
        {"col": np.array([np.iinfo(dtype).max, np.iinfo(dtype).max], dtype=dtype)},
        index=pd.date_range("2000-01-07", periods=2),
    )
    full_df = pd.concat([df0, df1, df2, df3])

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    for filter_expr, expected_reads in test_cases:
        q = QueryBuilder()
        q = q[filter_expr(q)]
        qs.reset_stats()
        qs.enable()
        result = lib.read(sym, query_builder=q).data

        expected = full_df[filter_expr(full_df)]
        assert_frame_equal(expected, result)

        assert get_table_data_read_count() == expected_reads


@pytest.mark.parametrize("dtype", (np.float64, np.float32))
def test_column_stats_boundary_values_float(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, dtype
):
    lib = in_memory_version_store

    test_cases = [
        (lambda q: q["col"] > np.finfo(dtype).min, 2),
        (lambda q: q["col"] < np.finfo(dtype).max, 2),
        (lambda q: q["col"] >= np.finfo(dtype).min, 4),
        (lambda q: q["col"] <= np.finfo(dtype).max, 4),
    ]

    df0 = pd.DataFrame(
        {"col": np.array([np.finfo(dtype).min], dtype=dtype)}, index=pd.date_range("2000-01-01", periods=1)
    )
    df1 = pd.DataFrame(
        {"col": np.array([np.finfo(dtype).min + 1], dtype=dtype)}, index=pd.date_range("2000-01-02", periods=1)
    )
    df2 = pd.DataFrame(
        {"col": np.array([np.finfo(dtype).max - 1], dtype=dtype)}, index=pd.date_range("2000-01-03", periods=1)
    )
    df3 = pd.DataFrame(
        {"col": np.array([np.finfo(dtype).max], dtype=dtype)}, index=pd.date_range("2000-01-04", periods=1)
    )
    full_df = pd.concat([df0, df1, df2, df3])

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    for filter_expr, expected_reads in test_cases:
        q = QueryBuilder()
        q = q[filter_expr(q)]
        qs.reset_stats()
        qs.enable()
        result = lib.read(sym, query_builder=q).data

        expected = full_df[filter_expr(full_df)]
        assert_frame_equal(expected, result)
        assert get_table_data_read_count() == expected_reads


def test_column_stats_boundary_values_ts(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store

    test_cases = [
        (lambda q: q["col"] >= pd.Timestamp.min, 4),
        (lambda q: q["col"] <= pd.Timestamp.max, 4),
        (lambda q: q["col"] > pd.Timestamp.min, 3),
        (lambda q: q["col"] < pd.Timestamp.max, 3),
    ]

    df0 = pd.DataFrame(
        {"col": np.array([pd.Timestamp.min, pd.Timestamp.min], dtype=pd.Timestamp)},
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame(
        {"col": np.array([pd.Timestamp.min + pd.Timedelta(1), pd.Timestamp.min + pd.Timedelta(1)], dtype=pd.Timestamp)},
        index=pd.date_range("2000-01-03", periods=2),
    )
    df2 = pd.DataFrame(
        {"col": np.array([pd.Timestamp.max, pd.Timestamp.max], dtype=pd.Timestamp)},
        index=pd.date_range("2000-01-05", periods=2),
    )
    df3 = pd.DataFrame(
        {"col": np.array([pd.Timestamp.max - pd.Timedelta(1), pd.Timestamp.max - pd.Timedelta(1)], dtype=pd.Timestamp)},
        index=pd.date_range("2000-01-07", periods=2),
    )

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    full_df = pd.concat([df0, df1, df2, df3])
    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    for filter_expr, expected_reads in test_cases:
        q = QueryBuilder()
        q = q[filter_expr(q)]
        qs.enable()
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data

        expected = full_df[filter_expr(full_df)]
        assert_frame_equal(expected, result)
        assert get_table_data_read_count() == expected_reads


@pytest.mark.parametrize(
    "query_expr,expected_reads",
    [
        pytest.param(lambda q: q["ts"] > pd.Timestamp("2001-01-02"), 1, id="gt_prunes_seg0"),
        pytest.param(lambda q: q["ts"] < pd.Timestamp("2001-01-03"), 1, id="lt_prunes_seg1"),
        pytest.param(lambda q: q["ts"] == pd.Timestamp("2001-01-03"), 1, id="eq_prunes_seg0"),
        pytest.param(lambda q: q["ts"] >= pd.Timestamp("2001-01-01"), 2, id="gte_no_pruning"),
    ],
)
def test_column_stats_timestamp_column(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame(
        {"ts": pd.to_datetime(["2001-01-01", "2001-01-02"]), "val": [1, 2]},
        index=pd.date_range("2000-01-01", periods=2),
    )
    df1 = pd.DataFrame(
        {"ts": pd.to_datetime(["2001-01-03", "2001-01-04"]), "val": [3, 4]},
        index=pd.date_range("2000-01-03", periods=2),
    )

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"ts": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1])
    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA read(s), got {table_data_reads}"


def test_column_stats_empty_dataframe(in_memory_version_store, column_stats_filtering_enabled):
    lib = in_memory_version_store

    df = pd.DataFrame({"col": pd.Series([], dtype=np.float64)})
    lib.write(sym, df)

    q = QueryBuilder()
    q = q[q["col"] > 0]
    result = lib.read(sym, query_builder=q).data
    assert len(result) == 0


def test_column_stats_duplicate_timestamp_index_drops_ambiguous_stats(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    """
    When two row-slices share the same (start_index, end_index) the stats for that key are
    ambiguous and we should not use them.
    """
    lib = in_memory_version_store

    ts = pd.Timestamp("2000-01-01")

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=[ts, ts])
    df1 = pd.DataFrame({"col_1": [5, 6]}, index=[ts, ts])

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[q["col_1"] < 3]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data

    # Both segments are read (stats are ambiguous for the duplicate key) and the filter in the
    # processing pipeline keeps only rows where col_1 < 3.
    expected = pd.DataFrame({"col_1": [1, 2]}, index=[ts, ts])
    assert_frame_equal(expected, result)
    # Both segments must be read because their stats were dropped.
    assert get_table_data_read_count() == 2


def test_column_stats_duplicate_timestamp_index_still_prunes_unique_keys(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    """
    When some row-slices share the same (start_index, end_index) but others are unique, only the
    ambiguous entries are dropped. The unique entries should still be used for pruning.
    """
    lib = in_memory_version_store

    ts1 = pd.Timestamp("2000-01-01")
    ts2 = pd.Timestamp("2000-01-02")

    df0 = pd.DataFrame({"col_1": [1, 2]}, index=[ts1, ts1])
    df1 = pd.DataFrame({"col_1": [10, 20]}, index=[ts1, ts1])
    df2 = pd.DataFrame({"col_1": [100, 200]}, index=[ts2, ts2])

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[q["col_1"] < 3]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data

    expected = pd.DataFrame({"col_1": [1, 2]}, index=[ts1, ts1])
    assert_frame_equal(expected, result)
    # seg0 and seg1 read (duplicate key, no pruning), seg2 pruned (unique key, min=100 > 3)
    assert get_table_data_read_count() == 2


def test_column_stats_empty_stats(in_memory_version_store, column_stats_filtering_enabled):
    lib = in_memory_version_store

    df = pd.DataFrame({"col": pd.Series([-1, 0, 1, 2, 3], dtype=np.float64)})
    lib.write(sym, df)

    # This is a no-op at the moment, but this is still a useful regression test in case we ever
    # start writing empty column stats segments.
    lib.create_column_stats(sym, {"col": set()})

    q = QueryBuilder()
    q = q[q["col"] > 0]
    result = lib.read(sym, query_builder=q).data
    assert len(result) == 3


@pytest.mark.parametrize(
    "query_expr,expected_reads",
    [
        pytest.param(lambda q: q["bool_col"] == True, 2, id="eq_true"),  # noqa: E712
        pytest.param(lambda q: q["bool_col"] == False, 2, id="eq_false"),  # noqa: E712
        pytest.param(lambda q: q["bool_col"] != True, 2, id="ne_true"),  # noqa: E712
        pytest.param(lambda q: q["bool_col"] != False, 2, id="ne_false"),  # noqa: E712
        pytest.param(lambda q: True == q["bool_col"], 2, id="true_eq"),  # noqa: E712
        pytest.param(lambda q: False == q["bool_col"], 2, id="false_eq"),  # noqa: E712
    ],
)
def test_column_stats_bool_comparison_operators(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"bool_col": [True, True]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"bool_col": [False, True]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"bool_col": [False, False]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"bool_col": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1, df2])
    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA reads, got {table_data_reads}"


@pytest.mark.parametrize(
    "query_expr,expected_reads",
    [
        pytest.param(lambda q: (q["int_col"] > 4) & q["bool_col"], 1, id="comparison_and_bool_col"),
        pytest.param(lambda q: q["bool_col"] & (q["int_col"] > 4), 1, id="bool_col_and_comparison"),
        pytest.param(lambda q: (q["int_col"] > 4) & q["bool_col"], 1, id="bool_col_and_comparison_flipped"),
        pytest.param(lambda q: (q["int_col"] > 4) | q["bool_col"], 1, id="comparison_or_bool_col"),
    ],
)
def test_column_stats_bool_col_combined_with_comparison(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr, expected_reads
):
    lib = in_memory_version_store

    df0 = pd.DataFrame({"int_col": [1, 2], "bool_col": [False, False]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"int_col": [5, 6], "bool_col": [True, True]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"int_col": {"MINMAX"}, "bool_col": {"MINMAX"}})

    qs.enable()
    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    full_df = pd.concat([df0, df1])
    expected = full_df[query_expr(full_df)]
    assert_frame_equal(expected, result)
    assert table_data_reads == expected_reads, f"Expected {expected_reads} TABLE_DATA reads, got {table_data_reads}"


@pytest.mark.parametrize(
    "query_expr",
    [
        lambda q: q["b"] & True,
        lambda q: q["b"] | False,
        lambda q: True & q["b"],
        lambda q: False | q["b"],
    ],
    ids=["and_true", "or_false", "true_and", "false_or"],
)
def test_column_stats_bool_col_and_bool_value(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled, query_expr
):
    """Test expressions like `q["b"] & True`"""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"b": [False, False]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"b": [True, True]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats(sym, {"b": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[query_expr(q)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()
    assert_frame_equal(result, df1)
    assert table_data_reads == 1, f"Expected 1 read, got {table_data_reads}"


def test_column_stats_two_bool_cols(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    """Test binary boolean operation between two bool columns"""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"b1": [True, True], "b2": [False, False]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"b1": [True, True], "b2": [True, True]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats(sym, {"b1": {"MINMAX"}, "b2": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[q["b1"] & q["b2"]]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()
    assert_frame_equal(result, df1)
    assert table_data_reads == 1, f"Expected 1 read, got {table_data_reads}"


@pytest.mark.xfail("Needs support for col-col comparisons")
def test_column_stats_two_bool_cols_comparison(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    """Test binary boolean operation between two bool columns"""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"b1": [True, True], "b2": [False, False]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"b1": [True, True], "b2": [True, True]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats(sym, {"b1": {"MINMAX"}, "b2": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[q["b1"] > q["b2"]]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()
    assert_frame_equal(result, df0)
    assert table_data_reads == 1, f"Expected 1 read, got {table_data_reads}"


def test_column_stats_bool_vs_int_cross_type(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    """Cross-type bool-vs-int comparisons (e.g. bool_col > 0) are rejected by the
    processing layer."""
    lib = in_memory_version_store

    # seg0: all-true, seg1: all-false
    df0 = pd.DataFrame({"bool_col": [True, True]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"bool_col": [False, False]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats(sym, {"bool_col": {"MINMAX"}})

    # When a surviving segment is processed, the cross-type comparison is rejected
    q = QueryBuilder()
    q = q[q["bool_col"] > 0]
    with pytest.raises(UserInputException):
        lib.read(sym, query_builder=q)

    # When column stats prune ALL segments, schema validation still rejects it
    df_only_false = pd.DataFrame({"bool_col": [False, False]}, index=pd.date_range("2000-01-01", periods=2))
    lib.write(sym, df_only_false)
    lib.create_column_stats(sym, {"bool_col": {"MINMAX"}})

    q = QueryBuilder()
    q = q[q["bool_col"] > 0]
    with pytest.raises(UserInputException):
        lib.read(sym, query_builder=q)
