import numpy as np
import pytest
from arcticdb.util.test import assert_frame_equal
from arcticdb.version_store.processing import QueryBuilder
import arcticdb.toolbox.query_stats as qs
import pandas as pd

from arcticdb.util.test import config_context


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


# TODO aseaton there are more tests worth porting here https://github.com/man-group/ArcticDB/compare/master...aseaton/column-stats-read#diff-aee6edf7c9863ea50d8bf13666b84d051eb90b99ae26e5e02459c31e18f35495
