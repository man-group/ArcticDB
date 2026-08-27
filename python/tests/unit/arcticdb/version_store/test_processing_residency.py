"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb import LibraryOptions, QueryBuilder, concat
from arcticdb.util.test import (
    assert_frame_equal,
    config_context,
    config_context_multi,
    generic_aggregation_test,
    generic_resample_test,
    segment_residency_tracking,
)
from arcticdb_ext.util import reset_segment_residency_tracking, set_segment_residency_tracking

pytestmark = pytest.mark.pipeline

RESIDENCY_KEY = "VersionStore.NumProcessingUnitsLive"
READ_WINDOW_KEY = "VersionStore.SegmentReadWindow"

NUM_ROWS = 200
SEGMENT_ROW_SIZE = 10
NUM_ROW_SLICES = NUM_ROWS // SEGMENT_ROW_SIZE

# The frame has fewer columns than the default column_group_size, so each row slice is a single column slice and a
# processing unit is one segment for every clause structured by row slice. That makes the expected residency an exact
# number rather than a multiple of the column slice count, and assert_one_segment_per_row_slice checks it holds.
# Column slicing interacting with the bound is covered in C++ by
# ColumnStatsMixedColSlicing.ResidencyBoundedWithUnevenUnits.
SYM = "residency_sym"


def sample_df():
    n = np.arange(NUM_ROWS)
    floats = n.astype(np.float64) / 8.0
    floats[n % 7 == 3] = np.nan
    return pd.DataFrame(
        {
            # 0..9 within every row slice, so a "< 5" filter selects exactly half of every row slice
            "int_col": (n % 10).astype(np.int64),
            "uint_col": (n % 4).astype(np.uint32),
            "float_col": floats,
            "str_col": [f"g{i % 4}" for i in n],
            "sparse_str": [None if i % 11 == 0 else f"s{i % 3}" for i in n],
            "bool_col": n % 3 != 0,
            "dt_col": pd.to_datetime("2020-01-01") + pd.to_timedelta(n, unit="h"),
        },
        index=pd.date_range("2024-01-02", periods=NUM_ROWS, freq="min"),
    )


def assert_one_segment_per_row_slice(lib, symbol):
    index_df = lib.read_index(symbol).reset_index()
    assert index_df["start_row"].nunique() == NUM_ROW_SLICES
    assert len(index_df) == NUM_ROW_SLICES, "expected a single column slice per row slice"


@pytest.fixture
def residency_lib(version_store_factory):
    lib = version_store_factory(segment_row_size=SEGMENT_ROW_SIZE, dynamic_strings=True)
    df = sample_df()
    lib.write(SYM, df)
    assert_one_segment_per_row_slice(lib, SYM)
    return lib, df


@pytest.fixture(autouse=True)
def residency_tracking_off():
    """Leave the process-wide tracker disabled and zeroed even if a test fails inside a tracking block."""
    yield
    set_segment_residency_tracking(False)
    reset_segment_residency_tracking()


def high_water_for_read(lib, query, k):
    with config_context(RESIDENCY_KEY, k):
        with segment_residency_tracking() as residency:
            received = lib.read(SYM, query_builder=query).data
            high_water = residency.high_water
            del received
    return high_water


# k units are in flight, and the next unit is admitted when the previous one finishes processing rather than when its
# segments are destructed, so an outgoing unit can still be draining when the incoming unit's reads land. The straddling
# case below reaches (k + 1) * max_unit_size locally, so allow two units of slack. What these bounds establish is that
# residency does not scale with NUM_ROW_SLICES.
def residency_bound(k, max_unit_size):
    return (k + 2) * max_unit_size


# Every row slice overlaps the single daily bucket, so they all land in one processing unit and folly::collect holds
# all of them at once regardless of the admission budget. Without this the bounds asserted below would also hold
# against a tracker that could never report more than one.
def test_single_processing_unit_holds_every_segment(residency_lib):
    lib, _ = residency_lib
    q = QueryBuilder().resample("1D").agg({"int_col": "sum"})
    assert high_water_for_read(lib, q, k=1) == NUM_ROW_SLICES


@pytest.mark.parametrize("k", [1, 2])
def test_filter_residency_bounded_by_admission(residency_lib, k):
    lib, _ = residency_lib
    max_unit_size = 1
    bound = residency_bound(k, max_unit_size)
    assert NUM_ROW_SLICES > bound

    q = QueryBuilder()
    # int_col holds 0..9 in every row slice, so this is a strict non-empty subset everywhere. A predicate matching a
    # whole row slice takes FilterClause's FullResult branch, which re-pushes the decoded segment instead of
    # replacing it, and residency would then not be bounded at all.
    q = q[q["int_col"] < 5]
    high_water = high_water_for_read(lib, q, k)

    assert high_water >= max_unit_size, "tracker recorded nothing"
    assert high_water <= bound


@pytest.mark.parametrize("k", [1, 2])
def test_groupby_residency_bounded_by_admission(residency_lib, k):
    lib, _ = residency_lib
    max_unit_size = 1
    bound = residency_bound(k, max_unit_size)
    assert NUM_ROW_SLICES > bound

    q = QueryBuilder().groupby("str_col").agg({"int_col": "sum", "float_col": "mean"})
    high_water = high_water_for_read(lib, q, k)

    assert high_water >= max_unit_size, "tracker recorded nothing"
    assert high_water <= bound


@pytest.mark.parametrize("k", [1, 2])
def test_aligned_resample_residency_bounded_by_admission(residency_lib, k):
    lib, _ = residency_lib
    # Buckets are the same width as a row slice and share its boundaries, so no bucket spans two row slices
    max_unit_size = 1
    bound = residency_bound(k, max_unit_size)
    assert NUM_ROW_SLICES > bound

    q = QueryBuilder().resample("10min", closed="left").agg({"int_col": "sum"})
    high_water = high_water_for_read(lib, q, k)

    assert high_water >= max_unit_size, "tracker recorded nothing"
    assert high_water <= bound


@pytest.mark.parametrize("k", [1, 2])
def test_straddling_resample_residency_bounded_by_admission(residency_lib, k):
    lib, _ = residency_lib
    # A 7 minute bucket over 10 minute row slices can pull in at most ceil(7/10) = 1 following slice, so a unit is at
    # most two segments
    max_unit_size = 2
    bound = residency_bound(k, max_unit_size)
    assert NUM_ROW_SLICES > bound

    q = QueryBuilder().resample("7min", closed="left").agg({"int_col": "sum"})
    high_water = high_water_for_read(lib, q, k)

    # The lower bound is the important half: it fails if bucket generation stops straddling row slices and this case
    # silently degenerates into the aligned one above.
    assert high_water >= max_unit_size, f"expected a straddling unit of {max_unit_size} segments, got {high_water}"
    assert high_water <= bound


AGGS = {"int_col": "sum", "uint_col": "max", "float_col": "mean", "dt_col": "min", "bool_col": "count"}

# generic_resample_test feeds these to pandas' agg as named aggregators
NAMED_AGGS = {
    "int_total": ("int_col", "sum"),
    "uint_peak": ("uint_col", "max"),
    "float_mean": ("float_col", "mean"),
    "dt_first": ("dt_col", "min"),
    "bool_count": ("bool_col", "count"),
}


def assert_read_matches(lib, query, expected):
    # assert_frame_equal rather than generic_filter_test because the frame carries NaNs and Nones, which
    # np.array_equal treats as unequal to themselves
    assert_frame_equal(expected, lib.read(SYM, query_builder=query).data, check_dtype=False)


def case_filter_numeric(lib, df):
    q = QueryBuilder()
    q = q[q["int_col"] < 5]
    assert_read_matches(lib, q, df[df["int_col"] < 5])


def case_filter_string_isin(lib, df):
    q = QueryBuilder()
    q = q[q["str_col"].isin(["g0", "g2"])]
    assert_read_matches(lib, q, df[df["str_col"].isin(["g0", "g2"])])


def case_filter_none_strings(lib, df):
    q = QueryBuilder()
    q = q[q["sparse_str"] == "s1"]
    assert_read_matches(lib, q, df[df["sparse_str"] == "s1"])


def case_filter_compound(lib, df):
    q = QueryBuilder()
    q = q[(q["int_col"] >= 3) & q["bool_col"]]
    assert_read_matches(lib, q, df[(df["int_col"] >= 3) & df["bool_col"]])


def case_filter_matching_whole_slices(lib, df):
    # Matches every row, so FilterClause takes the FullResult branch on every row slice
    q = QueryBuilder()
    q = q[q["int_col"] >= 0]
    assert_read_matches(lib, q, df[df["int_col"] >= 0])


def case_project(lib, df):
    q = QueryBuilder()
    q = q.apply("proj", (q["int_col"] * q["float_col"]) + 1)
    expected = df.copy()
    expected["proj"] = (df["int_col"] * df["float_col"]) + 1
    assert_read_matches(lib, q, expected)


def case_filter_then_project(lib, df):
    q = QueryBuilder()
    q = q[q["int_col"] < 5]
    q = q.apply("proj", q["int_col"] + q["uint_col"])
    expected = df[df["int_col"] < 5].copy()
    expected["proj"] = expected["int_col"] + expected["uint_col"]
    assert_read_matches(lib, q, expected)


def case_groupby_string_key(lib, df):
    generic_aggregation_test(lib, SYM, df, "str_col", AGGS)


def case_filter_then_groupby(lib, df):
    q = QueryBuilder()
    q = q[q["bool_col"]]
    q = q.groupby("str_col").agg({"int_col": "sum"})
    expected = df[df["bool_col"]].groupby("str_col").agg({"int_col": "sum"})
    received = lib.read(SYM, query_builder=q).data
    received.sort_index(inplace=True)
    assert_frame_equal(expected, received, check_dtype=False)


def case_resample_aligned(lib, df):
    generic_resample_test(lib, SYM, "10min", NAMED_AGGS, df, closed="left")


def case_resample_straddling(lib, df):
    # origin pinned because 7 minutes does not divide a day, so ArcticDB's epoch-phased buckets would otherwise land
    # elsewhere than pandas' start_day-phased ones
    generic_resample_test(lib, SYM, "7min", NAMED_AGGS, df, closed="left", label="right", origin="epoch")


def case_resample_single_bucket(lib, df):
    generic_resample_test(lib, SYM, "1D", NAMED_AGGS, df, closed="left")


def case_date_range_clause(lib, df):
    # Both ends fall part way through a row slice, so those units truncate rather than passing the segment through
    start, end = df.index[13], df.index[157]
    q = QueryBuilder().date_range((start, end))
    assert_read_matches(lib, q, df.loc[start:end])


def case_head(lib, df):
    q = QueryBuilder().head(37)
    assert_read_matches(lib, q, df.head(37))


QUERY_CASES = [
    case_filter_numeric,
    case_filter_string_isin,
    case_filter_none_strings,
    case_filter_compound,
    case_filter_matching_whole_slices,
    case_project,
    case_filter_then_project,
    case_groupby_string_key,
    case_filter_then_groupby,
    case_resample_aligned,
    case_resample_straddling,
    case_resample_single_bucket,
    case_date_range_clause,
    case_head,
]

ADMISSION_CONFIGS = [
    pytest.param({}, id="defaults"),
    pytest.param({RESIDENCY_KEY: 1}, id="k1"),
    pytest.param({READ_WINDOW_KEY: 1}, id="window1"),
    pytest.param({RESIDENCY_KEY: 1, READ_WINDOW_KEY: 1}, id="k1_window1"),
    pytest.param({RESIDENCY_KEY: 0}, id="killswitch"),
]


# A budget of one processing unit and a read window of one segment is where any failure to advance admission
# deadlocks rather than merely running slowly, so the timeout is what makes that a test failure.
@pytest.mark.timeout(120)
@pytest.mark.parametrize("case", QUERY_CASES, ids=lambda case: case.__name__[len("case_") :])
@pytest.mark.parametrize("admission_config", ADMISSION_CONFIGS)
def test_query_correct_under_admission_config(residency_lib, case, admission_config):
    lib, df = residency_lib
    with config_context_multi(admission_config):
        case(lib, df)


@pytest.mark.timeout(120)
def test_query_correct_with_single_thread_pools(residency_lib, tiny_thread_pool):
    lib, df = residency_lib
    with config_context_multi({RESIDENCY_KEY: 1, READ_WINDOW_KEY: 1}):
        case_filter_numeric(lib, df)
        case_resample_straddling(lib, df)


@pytest.fixture
def sliced_library(lmdb_library_factory):
    return lmdb_library_factory(LibraryOptions(rows_per_segment=SEGMENT_ROW_SIZE))


# Multi-symbol reads build one admission handler per symbol, so several are live at once.
@pytest.mark.timeout(120)
def test_symbol_concat_under_admission_config(sliced_library):
    df = sample_df()[["int_col", "float_col"]]
    chunk = NUM_ROWS // 2
    sliced_library.write("concat_0", df.iloc[:chunk])
    sliced_library.write("concat_1", df.iloc[chunk:])

    with config_context_multi({RESIDENCY_KEY: 1, READ_WINDOW_KEY: 1}):
        with segment_residency_tracking() as residency:
            received = concat(sliced_library.read_batch(["concat_0", "concat_1"], lazy=True)).collect().data
            # This test is only worth anything if the read went through the admission handler at all
            assert residency.high_water > 0, "read did not go through the admission handler"
    assert_frame_equal(df, received, check_dtype=False)


@pytest.mark.timeout(120)
def test_sort_and_finalize_staged_data_under_admission_config(sliced_library):
    df = sample_df()[["int_col", "float_col"]]
    for start in range(0, NUM_ROWS, SEGMENT_ROW_SIZE):
        sliced_library.stage(SYM, df.iloc[start : start + SEGMENT_ROW_SIZE])

    with config_context_multi({RESIDENCY_KEY: 1, READ_WINDOW_KEY: 1}):
        with segment_residency_tracking() as residency:
            sliced_library.sort_and_finalize_staged_data(SYM)
            assert residency.high_water == NUM_ROW_SLICES

    assert_frame_equal(df, sliced_library.read(SYM).data, check_dtype=False)
