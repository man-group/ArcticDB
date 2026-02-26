from datetime import datetime

import numpy as np
import pytest
from arcticdb.util.test import assert_frame_equal
from arcticdb.version_store.processing import QueryBuilder
import arcticdb.toolbox.query_stats as qs
import pandas as pd

from arcticdb.util.test import config_context, config_context_multi


@pytest.fixture
def column_stats_filtering_enabled():
    with config_context("ColumnStats.UseForQueries", 1):
        yield


def get_table_data_read_count():
    """Get the number of TABLE_DATA keys read from query stats."""
    stats = qs.get_query_stats()
    return (stats or {}).get("storage_operations", {}).get("Memory_GetObject", {}).get("TABLE_DATA", {}).get("count", 0)


sym = "sym"


def test_column_stats_query_optimisation(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    """Test that column stats are used to optimize QueryBuilder queries by pruning segments."""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()
    # Test 1: col_1 > 2 should only read segment 1 (segment 0 max=2 <= 2, pruned)
    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()
    assert_frame_equal(df1, result)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"

    # Test 2: col_1 > 4 should read no segments (both segments pruned: max <= 4)
    q = QueryBuilder()
    q = q[q["col_1"] > 4]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()
    assert len(result) == 0
    assert table_data_reads == 0, f"Expected 0 TABLE_DATA reads (all segments pruned), got {table_data_reads}"

    # Test 3: col_1 < 2 should only read segment 0 (segment 1 min=3 >= 2, pruned)
    q = QueryBuilder()
    q = q[q["col_1"] < 2]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    expected = pd.DataFrame(
        {
            "col_1": [
                1,
            ],
            "col_2": [
                "a",
            ],
        },
        index=pd.date_range("2000-01-01", periods=1),
    )
    assert_frame_equal(expected, result)
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 0 only), got {table_data_reads}"

    # Test 4: col_1 == 3 should only read segment 1 (segment 0 max=2 < 3, pruned)
    q = QueryBuilder()
    q = q[q["col_1"] == 3]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    expected = pd.DataFrame(
        {
            "col_1": [
                3,
            ],
            "col_2": [
                "c",
            ],
        },
        index=pd.date_range("2000-01-03", periods=1),
    )
    assert_frame_equal(expected, result)
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"

    # Test 5: col_1 >= 1 should read both segments (no pruning possible)
    q = QueryBuilder()
    q = q[q["col_1"] >= 1]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    expected = pd.concat([df0, df1])
    assert_frame_equal(expected, result)
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 2, f"Expected 2 TABLE_DATA reads (both segments), got {table_data_reads}"


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
    table_data_reads = get_table_data_read_count()
    assert_frame_equal(df1, result)
    assert table_data_reads == 2


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


def test_column_stats_query_optimisation_column_not_in_stats(lmdb_version_store_tiny_segment):
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


def test_column_stats_query_optimisation_multiple_filters(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    """
    Test that column stats pruning works correctly with multiple filters on different columns.

    The data is structured so that:
    - Segment 0: col_1 = [1, 2], col_2 = [10, 20]  (col_1 min=1, max=2; col_2 min=10, max=20)
    - Segment 1: col_1 = [3, 4], col_2 = [30, 40]  (col_1 min=3, max=4; col_2 min=30, max=40)
    - Segment 2: col_1 = [5, 6], col_2 = [50, 60]  (col_1 min=5, max=6; col_2 min=50, max=60)

    Test various combinations of filters on both columns.
    """
    lib = in_memory_version_store

    # Create 3 segments with distinct ranges for both columns
    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [10, 20]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [30, 40]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5, 6], "col_2": [50, 60]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    # Create column stats for both columns
    lib.create_column_stats(sym, {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}})

    qs.enable()
    qs.reset_stats()
    # Test 1: col_1 > 2 AND col_2 < 50 should only read segment 1
    # Segment 0: col_1 max=2 <= 2, pruned by col_1 filter
    # Segment 1: col_1 in [3,4], col_2 in [30,40], both filters pass
    # Segment 2: col_2 min=50 >= 50, pruned by col_2 filter
    q = QueryBuilder()
    q = q[(q["col_1"] > 2) & (q["col_2"] < 50)]
    result = lib.read(sym, query_builder=q).data
    assert_frame_equal(df1, result)
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"

    # Test 2: col_1 >= 3 AND col_2 > 35 should read segments 1 and 2
    # Segment 0: col_1 max=2 < 3, pruned by col_1 filter
    # Segment 1: might have col_2 > 35 (has 40)
    # Segment 2: both filters pass
    q = QueryBuilder()
    q = q[(q["col_1"] >= 3) & (q["col_2"] > 35)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    expected = pd.DataFrame({"col_1": [4, 5, 6], "col_2": [40, 50, 60]}, index=pd.date_range("2000-01-04", periods=3))
    assert_frame_equal(expected, result)
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 2, f"Expected 2 TABLE_DATA reads (segments 1 and 2), got {table_data_reads}"

    # Test 3: col_1 == 1 AND col_2 == 10 should only read segment 0
    # Only segment 0 can have col_1=1 (range [1,2]) and col_2=10 (range [10,20])
    q = QueryBuilder()
    q = q[(q["col_1"] == 1) & (q["col_2"] == 10)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()
    expected = pd.DataFrame(
        {
            "col_1": [
                1,
            ],
            "col_2": [
                10,
            ],
        },
        index=pd.date_range("2000-01-01", periods=1),
    )
    assert_frame_equal(expected, result)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 0 only), got {table_data_reads}"

    # Test 4: col_1 > 6 AND col_2 > 0 should read no segments
    # All segments have col_1 max <= 6, so all pruned
    q = QueryBuilder()
    q = q[(q["col_1"] > 6) & (q["col_2"] > 0)]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()
    assert len(result) == 0
    assert table_data_reads == 0, f"Expected 0 TABLE_DATA reads (all segments pruned), got {table_data_reads}"

    # Test 5: Filter only on col_1 with stats on both - col_1 > 4 should only read segment 2
    q = QueryBuilder()
    q = q[q["col_1"] > 4]
    qs.reset_stats()
    result = lib.read(sym, query_builder=q).data
    expected = pd.DataFrame({"col_1": [5, 6], "col_2": [50, 60]}, index=pd.date_range("2000-01-05", periods=2))
    assert_frame_equal(expected, result)
    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 2 only), got {table_data_reads}"


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
    """
    Test that column stats pruning works correctly with different numeric column types.
    """
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
    """
    Test that column stats pruning works correctly when the read includes
    both a column filter and a date_range.

    Segments:
    - Segment 0: col_1=[1,2], dates 2000-01-01 to 2000-01-02
    - Segment 1: col_1=[3,4], dates 2000-01-03 to 2000-01-04
    - Segment 2: col_1=[5,6], dates 2000-01-05 to 2000-01-06
    """
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
    date_range = (pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-04"))
    qs.reset_stats()
    result = lib.read(sym, query_builder=q, date_range=date_range).data
    table_data_reads = get_table_data_read_count()
    assert_frame_equal(df1, result)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"


def test_column_stats_and_filter_one_column_with_stats(
    in_memory_version_store, clear_query_stats, column_stats_filtering_enabled
):
    """AND filter with one column having stats should prune based on that column."""
    lib = in_memory_version_store

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [6, 5]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [8, 7]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5, 6], "col_2": [10, 9]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    qs.enable()

    q = QueryBuilder()
    q = q[(q["col_1"] > 4) & (q["col_2"] > 7)]
    result = lib.read(sym, query_builder=q).data

    table_data_reads = get_table_data_read_count()

    expected = pd.concat([df0, df1, df2])
    expected = expected[(expected["col_1"] > 4) & (expected["col_2"] > 7)]
    assert_frame_equal(result, expected)

    # Should read 1 segment (only seg2 can match col_1 > 4)
    assert table_data_reads == 1, f"Expected 1 TABLE_DATA read but got {table_data_reads}"

    # Now check an AND filter where both sides do some filtering
    lib.create_column_stats(sym, {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}})
    qs.reset_stats()
    q = QueryBuilder()
    q = q[(q["col_1"] > 4) & (q["col_2"] < 9)]
    result = lib.read(sym, query_builder=q).data
    assert result.empty

    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 0

    qs.reset_stats()
    q = QueryBuilder()
    q = q[q["col_1"] > 4]
    q = q[q["col_2"] < 9]
    result = lib.read(sym, query_builder=q).data
    assert result.empty

    table_data_reads = get_table_data_read_count()
    assert table_data_reads == 0


def test_column_stats_or_filter(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    """OR filter should only prune segments ruled out by ALL conditions."""
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


def test_column_stats_dynamic_schema_column_type_varies(
    in_memory_version_store_dynamic_schema, clear_query_stats, column_stats_filtering_enabled
):
    """Test pruning when column type varies across segments (dynamic schema)."""
    lib = in_memory_version_store_dynamic_schema

    df_int8 = pd.DataFrame({"col_1": np.array([1, 2], dtype=np.int8)}, index=pd.date_range("2000-01-01", periods=2))
    df_int32 = pd.DataFrame(
        {"col_1": np.array([100, 200], dtype=np.int32)}, index=pd.date_range("2000-01-03", periods=2)
    )
    lib.write(sym, df_int8)
    lib.append(sym, df_int32)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})
    qs.enable()
    q = QueryBuilder()
    q = q[q["col_1"] > 50]

    result = lib.read(sym, query_builder=q).data
    table_data_reads = get_table_data_read_count()

    expected = pd.concat([df_int8, df_int32])
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


def test_column_stats_comparison_operators(in_memory_version_store, clear_query_stats, column_stats_filtering_enabled):
    lib = in_memory_version_store
    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2), dtype=np.float64)
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2), dtype=np.float64)
    df2 = pd.DataFrame({"col_1": [6, 5]}, index=pd.date_range("2000-01-05", periods=2), dtype=np.float64)
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    test_cases = [
        (lambda q: q["col_1"] > 4, 1, "col_1 > 4"),
        (lambda q: q["col_1"] >= 6, 1, "col_1 >= 6"),
        (lambda q: q["col_1"] >= 7, 0, "col_1 >= 7"),
        (lambda q: q["col_1"] >= 5, 1, "col_1 >= 5"),
        (lambda q: q["col_1"] < 2, 1, "col_1 < 2"),
        (lambda q: q["col_1"] < 1, 0, "col_1 < 1"),
        (lambda q: q["col_1"] <= 1, 1, "col_1 <= 1"),
        (lambda q: q["col_1"] == 3, 1, "col_1 == 3"),
        (lambda q: q["col_1"] != 3, 3, "col_1 != 3"),
    ]

    for query_expr, expected_reads, desc in test_cases:
        qs.enable()
        qs.reset_stats()

        q = QueryBuilder()
        q = q[query_expr(q)]
        lib.read(sym, query_builder=q)

        table_data_reads = get_table_data_read_count()

        assert (
            table_data_reads == expected_reads
        ), f"For {desc}: expected {expected_reads} TABLE_DATA reads but got {table_data_reads}"


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


def test_column_stats_no_deadlock_single_thread(in_memory_version_store, column_stats_filtering_enabled):
    """Verify no deadlock with 1 IO and 1 CPU thread when reading with column stats."""
    lib = in_memory_version_store
    df0 = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4]}, index=pd.date_range("2000-01-03", periods=2))
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    with config_context_multi({"VersionStore.NumIOThreads": 1, "VersionStore.NumCPUThreads": 1}):
        q = QueryBuilder()
        q = q[q["col_1"] > 2]
        result = lib.read(sym, query_builder=q).data
        assert_frame_equal(df1, result)


def test_column_stats_no_deadlock_single_thread_no_stats(in_memory_version_store, column_stats_filtering_enabled):
    """Verify no deadlock when column stats don't exist (KeyNotFoundException path)."""
    lib = in_memory_version_store
    df = pd.DataFrame({"col_1": [1, 2]}, index=pd.date_range("2000-01-01", periods=2))
    lib.write(sym, df)

    with config_context_multi({"VersionStore.NumIOThreads": 1, "VersionStore.NumCPUThreads": 1}):
        q = QueryBuilder()
        q = q[q["col_1"] > 1]
        result = lib.read(sym, query_builder=q).data
        assert len(result) == 1


# TODO aseaton test that column slicing and row slicing still work!!
