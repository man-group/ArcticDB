"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd

from arcticdb import LibraryOptions
import arcticdb.toolbox.query_stats as qs
from arcticdb.util.append_and_defrag import _generate_levels, _generate_date_to_read_from, _append_and_defrag_idempotent
from arcticdb.util.test import assert_frame_equal


def test_generate_levels():
    assert _generate_levels(64, 2) == [64, 32, 16, 8, 4, 2]
    assert _generate_levels(64, 4) == [64, 16, 4]
    assert _generate_levels(5_000, 2) == [5_000, 2_500, 1_250, 625, 312, 156, 78, 39, 19, 9, 4, 2]
    assert _generate_levels(5_000, 10) == [5_000, 500, 50, 5]
    assert _generate_levels(5_000, 5) == [5_000, 1_000, 200, 40, 8]


def test_generate_date_to_read_from():
    assert _generate_date_to_read_from([pd.Timestamp("2025-01-01")], [0], [99], 1, [100], 1) == pd.Timestamp(
        "2025-01-01"
    )
    assert _generate_date_to_read_from([pd.Timestamp("2025-01-01")], [0], [99], 1, [1000, 100], 1) == pd.Timestamp(
        "2025-01-01"
    )
    assert _generate_date_to_read_from(
        [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")], [0, 50], [50, 99], 1, [100], 1
    ) == pd.Timestamp("2025-01-01")
    assert _generate_date_to_read_from(
        [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")], [0, 50], [50, 99], 1, [1000, 100], 1
    ) == pd.Timestamp("2025-01-01")
    assert _generate_date_to_read_from([pd.Timestamp("2025-01-01")], [0], [99], 99, [100], 1) == pd.Timestamp(
        "2025-01-01"
    )
    assert _generate_date_to_read_from([pd.Timestamp("2025-01-01")], [0], [100], 1, [100], 1) is None
    assert _generate_date_to_read_from(
        [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")], [0, 100], [100, 109], 1, [100, 10], 1
    ) == pd.Timestamp("2025-01-02")
    assert _generate_date_to_read_from(
        [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")], [0, 100], [100, 109], 42, [100, 50, 10], 1
    ) == pd.Timestamp("2025-01-02")


def test_basic_flow_single_symbol(lmdb_library_factory):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=64))
    sym = "test_basic_flow_single_symbol"
    rows_per_df = 3
    factor = 4  # levels will be [64, 16, 4]
    ts = pd.Timestamp("2025-01-01")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = df
    # First call will upsert with row slice 0-3
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 1
    assert_frame_equal(expected_result, lib.read(sym).data)
    # Second call will combine with existing data as 6 rows > 4 lowest level to produce row slice 0-6
    ts += pd.Timedelta(1, unit="days")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = pd.concat([expected_result, df])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 1
    assert_frame_equal(expected_result, lib.read(sym).data)
    # Third call will do no compaction to produce row slices 0-6 and 6-9
    ts += pd.Timedelta(1, unit="days")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = pd.concat([expected_result, df])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 2
    assert_frame_equal(expected_result, lib.read(sym).data)
    # Fourth call will combine with last row slice, but not first to produce row slices 0-6 and 6-12
    ts += pd.Timedelta(1, unit="days")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = pd.concat([expected_result, df])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 2
    assert_frame_equal(expected_result, lib.read(sym).data)
    # Fifth call will do no compaction to produce row slices 0-6, 6-12, and 12-15
    ts += pd.Timedelta(1, unit="days")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = pd.concat([expected_result, df])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 3
    assert_frame_equal(expected_result, lib.read(sym).data)
    # Sixth call will compact everything into 1 segment as we now have >16 rows, to produce row slice 0-18
    ts += pd.Timedelta(1, unit="days")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = pd.concat([expected_result, df])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 1
    assert_frame_equal(expected_result, lib.read(sym).data)
    # At 21 appends we will have 63 rows total and be at the most fragmented state
    # 0-18, 18-36, 36-54, 54-60, 60-63
    for _ in range(15):
        ts += pd.Timedelta(1, unit="days")
        df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
        expected_result = pd.concat([expected_result, df])
        _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
        assert_frame_equal(expected_result, lib.read(sym).data)
    assert len(lib._nvs.read_index(sym)) == 5
    # One more append will take us over 64 rows, and so get sliced on the update call to produce row slices 0-64 and 64-66
    ts += pd.Timedelta(1, unit="days")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = pd.concat([expected_result, df])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 2
    assert_frame_equal(expected_result, lib.read(sym).data)
    # The next append will actually re-slice the 0-64 into 0-63 and 63-69, as we have to defragment based on date_range,
    # and the index has the same timestamp in the last row of the first row slice and both rows in the second row slice
    ts += pd.Timedelta(1, unit="days")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = pd.concat([expected_result, df])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 2
    assert_frame_equal(expected_result, lib.read(sym).data)
    # We then reslice again into 0-64 and 64-72
    ts += pd.Timedelta(1, unit="days")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = pd.concat([expected_result, df])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
    assert len(lib._nvs.read_index(sym)) == 2
    assert_frame_equal(expected_result, lib.read(sym).data)


def test_basic_flow_multi_symbol(lmdb_library_factory):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=64))
    sym_0 = "test_basic_flow_multi_symbol_0"
    sym_1 = "test_basic_flow_multi_symbol_1"
    rows_per_df_0 = 3
    rows_per_df_1 = 4
    factor = 4  # levels will be [64, 16, 4]
    ts = pd.Timestamp("2025-01-01")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    expected_result_0 = df_0
    expected_result_1 = df_1
    # First call will upsert
    # sym_0: 0-3
    # sym_1: 0-4
    _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
    assert len(lib._nvs.read_index(sym_0)) == 1
    assert_frame_equal(expected_result_0, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_1)) == 1
    assert_frame_equal(expected_result_1, lib.read(sym_1).data)
    # Second call
    # sym_0: 0-6
    # sym_1: 0-4, 4-8
    ts += pd.Timedelta(1, unit="days")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    expected_result_0 = pd.concat([expected_result_0, df_0])
    expected_result_1 = pd.concat([expected_result_1, df_1])
    _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
    assert len(lib._nvs.read_index(sym_0)) == 1
    assert_frame_equal(expected_result_0, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_1)) == 2
    assert_frame_equal(expected_result_1, lib.read(sym_1).data)
    # Third call
    # sym_0: 0-6, 6-9
    # sym_1: 0-4, 4-8, 8-12
    ts += pd.Timedelta(1, unit="days")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    expected_result_0 = pd.concat([expected_result_0, df_0])
    expected_result_1 = pd.concat([expected_result_1, df_1])
    _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
    assert len(lib._nvs.read_index(sym_0)) == 2
    assert_frame_equal(expected_result_0, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_1)) == 3
    assert_frame_equal(expected_result_1, lib.read(sym_1).data)
    # Fourth call
    # sym_0: 0-6, 6-12
    # sym_1: 0-16
    ts += pd.Timedelta(1, unit="days")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    expected_result_0 = pd.concat([expected_result_0, df_0])
    expected_result_1 = pd.concat([expected_result_1, df_1])
    _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
    assert len(lib._nvs.read_index(sym_0)) == 2
    assert_frame_equal(expected_result_0, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_1)) == 1
    assert_frame_equal(expected_result_1, lib.read(sym_1).data)
    # 16 calls produces a perfectly compacted segment for sym_1
    # sym_0 has 48 rows at this point: 0-18, 18-36, 36-42, 42-48
    for _ in range(12):
        ts += pd.Timedelta(1, unit="days")
        df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
        df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
        expected_result_0 = pd.concat([expected_result_0, df_0])
        expected_result_1 = pd.concat([expected_result_1, df_1])
        _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
    assert len(lib._nvs.read_index(sym_0)) == 4
    assert_frame_equal(expected_result_0, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_1)) == 1
    assert_frame_equal(expected_result_1, lib.read(sym_1).data)
    # One more restarts the cycle for sym_1
    # sym_0 goes to 0-18, 18-36, 36-42, 42-48, 48-51
    ts += pd.Timedelta(1, unit="days")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    expected_result_0 = pd.concat([expected_result_0, df_0])
    expected_result_1 = pd.concat([expected_result_1, df_1])
    _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
    assert len(lib._nvs.read_index(sym_0)) == 5
    assert_frame_equal(expected_result_0, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_1)) == 2
    assert_frame_equal(expected_result_1, lib.read(sym_1).data)


def test_single_symbol_idempotency(lmdb_library_factory):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=64))
    sym = "test_single_symbol_idempotency"
    rows_per_df = 3
    factor = 4  # levels will be [64, 16, 4]
    ts = pd.Timestamp("2025-01-01")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = df
    # First call will upsert with row slice 0-3
    _append_and_defrag_idempotent(lib, [(sym, df)], factor)
    assert len(lib._nvs.read_index(sym)) == 1
    assert_frame_equal(expected_result, lib.read(sym).data)
    # If we call again with the same index value then nothing should change
    df = pd.DataFrame({"col": np.arange(rows_per_df, 2 * rows_per_df)}, index=rows_per_df * [ts])
    _append_and_defrag_idempotent(lib, [(sym, df)], factor)
    index = lib._nvs.read_index(sym)
    assert all(version == 0 for version in index["version_id"].to_list())
    assert len(index) == 1
    assert_frame_equal(expected_result, lib.read(sym).data)


def test_multi_symbol_idempotency(lmdb_library_factory):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=64))
    sym_0 = "test_multi_symbol_idempotency_0"
    sym_1 = "test_multi_symbol_idempotency_1"
    rows_per_df = 3
    factor = 4  # levels will be [64, 16, 4]
    ts = pd.Timestamp("2025-01-01")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result = df
    # First call will upsert with row slice 0-3
    _append_and_defrag_idempotent(lib, [(sym_0, df), (sym_1, df)], factor)
    assert len(lib._nvs.read_index(sym_0)) == 1
    assert_frame_equal(expected_result, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_0)) == 1
    assert_frame_equal(expected_result, lib.read(sym_1).data)
    # Call with just sym_0 to represent a half-completed call with sym_0 and sym_1
    ts += pd.Timedelta(1, unit="days")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result_0 = pd.concat([expected_result, df_0])
    expected_result_1 = expected_result
    _append_and_defrag_idempotent(lib, [(sym_0, df_0)], factor)
    assert len(lib._nvs.read_index(sym_0)) == 1
    assert_frame_equal(expected_result_0, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_1)) == 1
    assert_frame_equal(expected_result_1, lib.read(sym_1).data)
    # Now call again with new data for sym_0 and including sym_1
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df, 2 * rows_per_df)}, index=rows_per_df * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    expected_result_1 = pd.concat([expected_result_1, df_1])
    _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor)
    index_0 = lib._nvs.read_index(sym_0)
    assert all(version == 1 for version in index_0["version_id"].to_list())
    assert len(index_0) == 1
    assert_frame_equal(expected_result_0, lib.read(sym_0).data)
    assert len(lib._nvs.read_index(sym_1)) == 1
    assert_frame_equal(expected_result_1, lib.read(sym_1).data)


def assert_data_key_ios(stats, expected_reads, expected_writes):
    try:
        read_count = stats["S3_GetObject"]["TABLE_DATA"]["count"]
    except KeyError:
        read_count = 0
    try:
        write_count = stats["S3_PutObject"]["TABLE_DATA"]["count"]
    except KeyError:
        write_count = 0
    assert read_count == expected_reads
    assert write_count == expected_writes


def test_io_count_basic(s3_library_factory):
    lib = s3_library_factory(LibraryOptions(rows_per_segment=64))
    sym_0 = "test_io_count_basic_0"
    sym_1 = "test_io_count_basic_1"
    rows_per_df_0 = 3
    rows_per_df_1 = 4
    factor = 4  # levels will be [64, 16, 4]
    ts = pd.Timestamp("2025-01-01")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    # First call will upsert
    # sym_0: 0-3
    # sym_1: 0-4
    with qs.query_stats():
        _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
        stats = qs.get_query_stats()["storage_operations"]
    qs.reset_stats()
    assert_data_key_ios(stats, 0, 2)
    # Make the same call again - idempotent so no data keys written the second time
    with qs.query_stats():
        _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
        stats = qs.get_query_stats()["storage_operations"]
    qs.reset_stats()
    assert_data_key_ios(stats, 0, 0)
    # Second call
    # sym_0: 0-6
    # sym_1: 0-4, 4-8
    ts += pd.Timedelta(1, unit="days")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    with qs.query_stats():
        _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
        stats = qs.get_query_stats()["storage_operations"]
    qs.reset_stats()
    # sym_0 will have to read the existing data key to combine it with the new data
    assert_data_key_ios(stats, 1, 2)
    # Third call
    # sym_0: 0-6, 6-9
    # sym_1: 0-4, 4-8, 8-12
    ts += pd.Timedelta(1, unit="days")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    with qs.query_stats():
        _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
        stats = qs.get_query_stats()["storage_operations"]
    qs.reset_stats()
    # No defrag happening so no data keys will be read
    assert_data_key_ios(stats, 0, 2)
    # Fourth call
    # sym_0: 0-6, 6-12
    # sym_1: 0-16
    ts += pd.Timedelta(1, unit="days")
    df_0 = pd.DataFrame({"col": np.arange(rows_per_df_0)}, index=rows_per_df_0 * [ts])
    df_1 = pd.DataFrame({"col": np.arange(rows_per_df_1)}, index=rows_per_df_1 * [ts])
    with qs.query_stats():
        _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, 1)
        stats = qs.get_query_stats()["storage_operations"]
    qs.reset_stats()
    # sym_0 will read 1 data key to defrag
    # sym_1 will read all 3 data keys to defrag
    assert_data_key_ios(stats, 4, 2)


def test_io_count_many_iterations(s3_library_factory):
    lib = s3_library_factory(LibraryOptions(rows_per_segment=64))
    sym = "test_io_count_many_iterations"
    rows_per_df = 4
    factor = 4  # levels will be [64, 16, 4]
    ts = pd.Timestamp("2025-01-01")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    num_iterations = 32
    with qs.query_stats():
        for _ in range(num_iterations):
            _append_and_defrag_idempotent(lib, [(sym, df)], factor, 1)
            ts += pd.Timedelta(1, unit="days")
            df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
        stats = qs.get_query_stats()["storage_operations"]
    qs.reset_stats()
    # Iterations:
    # 0:    read 0 data keys, write 1 data key 0-4
    # 1:    read 0 data keys, write 1 data key 0-4, 4-8
    # 2:    read 0 data keys, write 1 data key 0-4, 4-8, 8-12
    # 3:    read 3 data keys, write 1 data key 0-16
    # 4-14: repeat this pattern, so total read 9 data keys, written 15 data keys 0-16, 16-32, 32-48, 48-52, 52-56, 56-60
    # 15:   read 6 data keys, write 1 data key 0-64
    # Cumulative total read 15 and written 16 data keys
    # 15-31: repeat this pattern for cumulative total read 30 and written 32 data keys
    assert_data_key_ios(stats, 30, 32)


def test_realistic(lmdb_library_factory):
    rng = np.random.default_rng(0)
    rows_per_segment = 1_000
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=rows_per_segment))
    sym_0 = "test_multi_symbol_idempotency_0"
    sym_1 = "test_multi_symbol_idempotency_1"
    factor = 5  # levels will be [1000, 200, 40, 8]
    threshold = 0.9  # The default
    # Probabilities:
    # sym_0 will append 1, 2, or 3 rows per day with equal probability
    rows_to_append_0 = np.array([1, 2, 3])
    # sym_1 will append 4, 5, or 6 rows per day with equal probability
    rows_to_append_1 = np.array([4, 5, 6])
    # Make the same update twice in a row 10% of the time
    repeat_probability = 0.1
    expected_0 = pd.DataFrame()
    expected_1 = pd.DataFrame()
    ts = pd.Timestamp("2000-01-01")
    for _ in range(1_000):
        rows_per_df_0 = rng.choice(rows_to_append_0)
        rows_per_df_1 = rng.choice(rows_to_append_1)
        df_0 = pd.DataFrame({"col": rng.random(rows_per_df_0)}, index=rows_per_df_0 * [ts])
        df_1 = pd.DataFrame({"col": rng.random(rows_per_df_1)}, index=rows_per_df_1 * [ts])
        expected_0 = pd.concat([expected_0, df_0])
        expected_1 = pd.concat([expected_1, df_1])
        _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, threshold)
        if rng.random() <= repeat_probability:
            _append_and_defrag_idempotent(lib, [(sym_0, df_0), (sym_1, df_1)], factor, threshold)
        assert_frame_equal(expected_0, lib.read(sym_0).data)
        assert_frame_equal(expected_1, lib.read(sym_1).data)
        ts += pd.Timedelta(1, unit="days")
        # It is impossible to assert the exact number of row slices we will have at this point, but we can at least
        # check that the first n blocks are all compacted to approximately rows_per_segment
        levels = _generate_levels(rows_per_segment, factor)
        index_0 = lib._nvs.read_index(sym_0)
        row_counts_0 = (index_0["end_row"] - index_0["start_row"]).to_list()
        min_nicely_compacted_slices_0 = sum(row_counts_0) // rows_per_segment
        nicely_compacted_slices_0 = [row_count for row_count in row_counts_0 if row_count >= threshold * levels[0]]
        assert len(nicely_compacted_slices_0) >= min_nicely_compacted_slices_0

        index_1 = lib._nvs.read_index(sym_1)
        row_counts_1 = (index_1["end_row"] - index_1["start_row"]).to_list()
        min_nicely_compacted_slices_1 = sum(row_counts_1) // rows_per_segment
        nicely_compacted_slices_1 = [row_count for row_count in row_counts_1 if row_count >= threshold * levels[0]]
        assert len(nicely_compacted_slices_1) >= min_nicely_compacted_slices_1
