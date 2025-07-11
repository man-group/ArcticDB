from collections import defaultdict
from pprint import pformat

import pytest
import pandas as pd
import numpy as np

from arcticdb.util.test import (
    create_df,
    assert_frame_equal,
    config_context,
    config_context_string,
    dataframe_simulate_arcticdb_update_static,
)
import arcticdb.toolbox.query_stats as qs


def sum_operations(stats):
    """Sum up all operations from query stats.

    Args:
        stats: Dictionary containing query stats

    Returns:
        Dictionary with total counts, sizes, and times for each operation type
    """
    totals = {}
    for op_type, key_types in stats["storage_operations"].items():
        totals[op_type] = {"count": 0, "size_bytes": 0, "total_time_ms": 0}

        for key_type, metrics in key_types.items():
            totals[op_type]["count"] += metrics["count"]
            totals[op_type]["size_bytes"] += metrics["size_bytes"]
            totals[op_type]["total_time_ms"] += metrics["total_time_ms"]

    return totals


def sum_all_operations(stats):
    totals = sum_operations(stats)
    total_count = 0
    for op_type, metrics in totals.items():
        total_count += metrics["count"]
    return total_count


def sum_operations_by_type(stats, type):
    totals = sum_operations(stats)
    return totals[type]["count"]


def visualize_stats_diff(stats1, stats2):
    """Visualize count differences between two stats dictionaries in a table format.

    Args:
        stats1: First stats dictionary
        stats2: Second stats dictionary

    Returns:
        String containing formatted count differences in a table
    """

    def get_counts(stats):
        counts = defaultdict(lambda: defaultdict(int))
        for op_type, key_types in stats.get("storage_operations", {}).items():
            for key_type, metrics in key_types.items():
                counts[op_type][key_type] = metrics.get("count", 0)
        return counts

    counts1 = get_counts(stats1)
    counts2 = get_counts(stats2)

    # Get all unique operations and key types
    all_ops = sorted(set(counts1.keys()) | set(counts2.keys()))
    all_key_types = set()
    for op in all_ops:
        all_key_types.update(counts1[op].keys())
        all_key_types.update(counts2[op].keys())
    all_key_types = sorted(all_key_types)

    # Build the table
    output = []
    output.append("Count Differences:")
    output.append("=" * 80)

    # Header
    header = "Operation".ljust(30) + "Key Type".ljust(20) + "Before".rjust(10) + "After".rjust(10) + "Diff".rjust(10)
    output.append(header)
    output.append("-" * 80)

    # Table rows
    for op in all_ops:
        for key_type in all_key_types:
            count1 = counts1[op][key_type]
            count2 = counts2[op][key_type]
            if count1 != count2:
                diff = count2 - count1
                diff_str = f"{diff:+d}" if diff != 0 else "0"
                row = f"{op[:28]:<30} {key_type[:18]:<20} {count1:>10} {count2:>10} {diff_str:>10}"
                output.append(row)

    if len(output) == 3:  # Only header, separator, and header row
        return "No count differences found"

    # Add summary
    output.append("-" * 80)
    total1 = sum(sum(counts.values()) for counts in counts1.values())
    total2 = sum(sum(counts.values()) for counts in counts2.values())
    total_diff = total2 - total1
    diff_str = f"{total_diff:+d}" if total_diff != 0 else "0"
    summary = f"Total:".ljust(50) + f"{total1:>10} {total2:>10} {diff_str:>10}"
    output.append(summary)

    return "\n".join(output)


def test_delete_over_time(lib_name, s3_and_nfs_storage_bucket, clear_query_stats):
    qs.enable()
    expected_ops = 14
    lib = s3_and_nfs_storage_bucket.create_version_store_factory(lib_name)()

    with config_context("VersionMap.ReloadInterval", 0):
        # Setup
        # First write and delete will add an extra couple of version keys
        lib.write("s", data=create_df())
        qs.reset_stats()
        lib.delete("s")

        assert sum_all_operations(qs.get_query_stats()) == expected_ops
        lib.write("s", data=create_df())
        qs.reset_stats()

        lib.delete("s")
        base_stats = qs.get_query_stats()
        base_ops_count = sum_all_operations(base_stats)
        # expected_ops + 2 (read the new version and the tombstone all key)
        assert base_ops_count == (expected_ops + 2)
        qs.reset_stats()

        iters = 10

        # make sure that the delete makes a constant number of operations
        for i in range(iters):
            lib.write("s", data=create_df())
            qs.reset_stats()

            lib.delete("s")
            stats = qs.get_query_stats()
            qs.reset_stats()
            assert sum_all_operations(stats) == base_ops_count == (expected_ops + 2), visualize_stats_diff(
                base_stats, stats
            )


def test_write_and_prune_previous_over_time(lib_name, s3_and_nfs_storage_bucket, clear_query_stats):
    expected_ops = 17
    with config_context("VersionMap.ReloadInterval", 0):
        lib = s3_and_nfs_storage_bucket.create_version_store_factory(lib_name)()
        qs.enable()
        lib.write("s", data=create_df())
        lib.write("s", data=create_df(), prune_previous_version=True)
        qs.reset_stats()

        lib.write("s", data=create_df(), prune_previous_version=True)

        base_stats = qs.get_query_stats()
        base_ops_count = sum_all_operations(base_stats)
        assert base_ops_count == expected_ops, pformat(base_stats)
        qs.reset_stats()

        iters = 10

        # make sure that the write and prune makes a constant number of operations
        for i in range(iters):
            lib.write("s", data=create_df(), prune_previous_version=True)
            stats = qs.get_query_stats()
            qs.reset_stats()
            assert sum_all_operations(stats) == base_ops_count, visualize_stats_diff(base_stats, stats)


def test_read_after_write_and_prune_previous(lib_name, s3_and_nfs_storage_bucket, clear_query_stats):
    expected_ops = 3
    lib = s3_and_nfs_storage_bucket.create_version_store_factory(lib_name)()
    lib.write("s", data=create_df())
    lib.write("s", data=create_df(), prune_previous_version=True)

    with config_context("VersionMap.ReloadInterval", 0):
        qs.enable()
        lib.read("s")
        stats = qs.get_query_stats()
        qs.reset_stats()

        assert sum_all_operations(stats) == expected_ops, pformat(stats)
        # there should be only get object operations
        assert stats["storage_operations"].keys() == {"S3_GetObject"}, pformat(stats)
        get_obj_ops = stats["storage_operations"]["S3_GetObject"]
        # We expect 3 get object operations:
        # - 1 for the version ref key
        # - 1 for the index key
        # - 1 for the data key
        assert len(get_obj_ops.keys()) == 3, pformat(stats)
        assert get_obj_ops["VERSION_REF"]["count"] == 1, pformat(stats)
        assert get_obj_ops["TABLE_INDEX"]["count"] == 1, pformat(stats)
        assert get_obj_ops["TABLE_DATA"]["count"] == 1, pformat(stats)


def get_dataframe_for_range_edge_cases():
    index = [pd.Timestamp(x) for x in [10, 11, 11, 13, 13, 13, 13, 14, 14, 15, 16, 16, 16, 16, 18]]
    data = {f"col_{i}": np.arange(i, i + 15) for i in range(5)}
    return pd.DataFrame(data=data, index=index)


def get_update_dataframe(start_index, end_index):
    num_rows = (end_index - start_index).value + 1
    index = pd.date_range(start=start_index, periods=num_rows, freq="ns")
    data = {f"col_{i}": np.arange(100, 100 + num_rows) for i in range(5)}
    return pd.DataFrame(data=data, index=index)


date_ranges_to_test = [
    (None, None),  # All
    (None, 9),  # Before beginning
    (None, 10),  # Only first
    (None, 13),  # Up to 13
    (13, None),  # From 13
    (18, None),  # Only last
    (19, None),  # After end
    (12, 12),  # No values but within segment
    (17, 17),  # No values but between segments
    (11, 11),  # Single value
    (13, 13),
    (14, 14),
    (16, 16),
    (11, 12),  # 2 values
    (13, 14),
    (14, 15),
    (10, 12),  # 3 values
    (11, 13),
    (11, 16),  # More values
    (10, 18),
]


def get_num_data_keys_intersecting_row_range(index, start, end):
    count = 0
    for index, row in index.iterrows():
        if (start is None or start < row["end_row"]) and (end is None or end > row["start_row"]):
            count += 1
    return count


def get_num_data_keys_intersecting_date_range(index, start, end, exclude_fully_included=False):
    count = 0
    for _, row in index.reset_index().iterrows():
        # end is inclusive when doing date_range but end_index in the column is exclusive
        if (start is None or start < row["end_index"]) and (end is None or end >= row["start_index"]):
            if exclude_fully_included:
                # When reading during an update we should only read the slices which include both elements within the
                # range and elements outside the range.
                # The above if checks the range has elements within the range and
                # the below if checks the range has elements outside the range.
                if (start is not None and row["start_index"] < start) or (
                    end is not None and end + pd.Timedelta(1) < row["end_index"]
                ):
                    count += 1
            else:
                count += 1
    return count


@pytest.mark.parametrize(
    "row_range_start, row_range_end", [(0, 0), (5, 5), (0, 1), (1, 2), (5, 6), (0, 4), (1, 5), (0, 6), (6, 15), (0, 15)]
)
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_row_range_num_reads(s3_store_factory, clear_query_stats, dynamic_schema, row_range_start, row_range_end):
    with config_context("VersionMap.ReloadInterval", 0):
        lib = s3_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=dynamic_schema)
        qs.enable()
        sym = "sym"
        df = get_dataframe_for_range_edge_cases()

        lib.write(sym, df)
        qs.reset_stats()

        index = lib.read_index(sym)
        stats = qs.get_query_stats()
        qs.reset_stats()
        # We expect one read for vref and one for index key (we use the vref shortcut to bypass the version key)
        assert sum_operations_by_type(stats, "S3_GetObject") == 2

        expected_df = df.iloc[row_range_start:row_range_end]
        result_df = lib.read(sym, row_range=(row_range_start, row_range_end)).data
        stats = qs.get_query_stats()
        qs.reset_stats()
        assert_frame_equal(result_df, expected_df)

        expected_data_keys = get_num_data_keys_intersecting_row_range(index, row_range_start, row_range_end)
        assert sum_operations_by_type(stats, "S3_GetObject") == expected_data_keys + 2


@pytest.mark.parametrize("date_range_start, date_range_end", date_ranges_to_test)
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_date_range_num_reads(s3_store_factory, clear_query_stats, dynamic_schema, date_range_start, date_range_end):
    with config_context("VersionMap.ReloadInterval", 0):
        lib = s3_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=dynamic_schema)
        qs.enable()
        sym = "sym"
        df = get_dataframe_for_range_edge_cases()
        date_range_start = pd.Timestamp(date_range_start) if date_range_start is not None else None
        date_range_end = pd.Timestamp(date_range_end) if date_range_end is not None else None

        lib.write(sym, df)
        qs.reset_stats()

        index = lib.read_index(sym)
        stats = qs.get_query_stats()
        qs.reset_stats()
        # We expect one read for vref and one for index key (we use the vref shortcut to bypass the version key)
        assert sum_operations_by_type(stats, "S3_GetObject") == 2

        expected_df = df.loc[date_range_start:date_range_end]
        result_df = lib.read(sym, date_range=(date_range_start, date_range_end)).data
        stats = qs.get_query_stats()
        qs.reset_stats()
        assert_frame_equal(result_df, expected_df)

        expected_data_keys = get_num_data_keys_intersecting_date_range(index, date_range_start, date_range_end)
        assert sum_operations_by_type(stats, "S3_GetObject") == expected_data_keys + 2


@pytest.mark.parametrize("update_range_start, update_range_end", date_ranges_to_test)
@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_update_num_reads(s3_store_factory, clear_query_stats, dynamic_schema, update_range_start, update_range_end):
    with config_context("VersionMap.ReloadInterval", 0):
        lib = s3_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=dynamic_schema)
        qs.enable()
        sym = "sym"
        init_df = get_dataframe_for_range_edge_cases()
        update_range_start = (
            pd.Timestamp(update_range_start) if update_range_start is not None else init_df.index[0] - pd.Timedelta(10)
        )
        update_range_end = (
            pd.Timestamp(update_range_end) if update_range_end is not None else init_df.index[-1] + pd.Timedelta(10)
        )
        update_df = get_update_dataframe(update_range_start, update_range_end)
        assert update_df.index[0] == update_range_start
        assert update_df.index[-1] == update_range_end

        lib.write(sym, init_df)
        qs.reset_stats()

        index = lib.read_index(sym)
        stats = qs.get_query_stats()
        qs.reset_stats()
        # We expect one read for vref and one for index key (we use the vref shortcut to bypass the version key)
        assert sum_operations_by_type(stats, "S3_GetObject") == 2

        lib.update(sym, update_df)
        stats = qs.get_query_stats()
        qs.reset_stats()
        expected_data_keys = get_num_data_keys_intersecting_date_range(
            index, update_range_start, update_range_end, exclude_fully_included=True
        )
        if update_range_start == pd.Timestamp(12) and update_range_end == pd.Timestamp(12):
            # Currently if we're updating a range completely within a single slice (as the case for 12-12) we read each
            # of these slices twice:
            # - Once to construct the new slice before the updated range
            # - Second time to construct the new slice after the updated range
            expected_data_keys *= 2

        # Update does 4 extra reads to the data keys:
        # - 2 reads of VERSION_REF
        # - read VERSION
        # - read TABLE_INDEX
        assert sum_operations_by_type(stats, "S3_GetObject") == expected_data_keys + 4

        expected_df = dataframe_simulate_arcticdb_update_static(init_df, update_df)
        result_df = lib.read(sym).data
        qs.reset_stats()
        assert_frame_equal(result_df, expected_df)
