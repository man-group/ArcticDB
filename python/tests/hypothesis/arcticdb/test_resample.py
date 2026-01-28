import pandas as pd
import numpy as np
import pytest
from hypothesis import given, settings, assume, HealthCheck
import hypothesis.strategies as st
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked, date, dataframe
from arcticdb.util.test import (
    generic_resample_test,
    compute_common_type_for_columns_in_df_list,
    expected_aggregation_type,
    compute_common_type_for_columns,
)
from arcticdb.util._versions import IS_PANDAS_TWO

from arcticdb.util.logger import get_logger


COLUMN_DTYPE = ["float", "int", "uint"]
ALL_AGGREGATIONS = ["sum", "mean", "min", "max", "first", "last", "count"]
# Make sure the start date is pre-epoch so that we can test pre-epoch dates. Not all C++ libraries handle pre-epoch well.
MIN_DATE = np.datetime64("1960-01-01")
MAX_DATE = np.datetime64("1980-01-01")

pytestmark = pytest.mark.pipeline


def dense_row_count_in_resampled_dataframe(df_list, rule):
    """
    The number of rows Arctic's resampling will produce after appending all dataframes in `df_list` and then resampling
    with `rule`.  Assumes df_list is sorted by start date and the indexes are not overlapping.
    """
    return (df_list[-1].index[-1] - df_list[0].index[0]).value // pd.Timedelta(rule).value


@st.composite
def origin(draw):
    selected_origin = draw(st.sampled_from(["start", "end", "start_day", "end_day", "epoch", "timestamp"]))
    # Hypothesis may generate dates for year > 2200 and some of the arithmetic operation will overflow.
    if selected_origin == "timestamp":
        min_date = MIN_DATE - np.timedelta64(5, "D")
        max_date = MAX_DATE + np.timedelta64(5, "D")
        return pd.Timestamp(draw(date(min_date=min_date, max_date=max_date)))
    else:
        return selected_origin


def freq_fits_in_64_bits(count, unit):
    """
    This is used to check if a frequency is usable by Arctic. ArcticDB converts the frequency to signed 64-bit integer.
    """
    billion = 1_000_000_000
    mult = {"h": 3600 * billion, "min": 60 * billion, "s": billion, "ms": billion // 1000, "us": 1000, "ns": 1}
    return (mult[unit] * count).bit_length() <= 63


@st.composite
def rule(draw):
    count = draw(st.integers(min_value=1, max_value=10_000))
    unit = draw(st.sampled_from(["min", "h", "s"]))
    result = f"{count}{unit}"
    assume(freq_fits_in_64_bits(count=count, unit=unit))
    return result


@st.composite
def offset(draw):
    unit = draw(st.sampled_from(["s", "min", "h", "ms", "us", "ns", None]))
    if unit is None:
        return None
    count = draw(st.integers(min_value=1, max_value=100))
    result = f"{count}{unit}"
    assume(freq_fits_in_64_bits(count=count, unit=unit))
    return result


@st.composite
def dynamic_schema_column_list(draw):
    all_column_names = [f"col_{i}" for i in range(5)]
    segment_count = draw(st.integers(min_value=1, max_value=10))
    segment_ranges = sorted(
        draw(
            st.lists(
                date(min_date=MIN_DATE, max_date=MAX_DATE, unit="s"),
                unique=True,
                min_size=segment_count + 1,
                max_size=segment_count + 1,
            )
        )
    )
    segments = []
    dtypes = [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64, np.float32, np.float64]
    columns_per_segment = [
        draw(st.lists(st.sampled_from(all_column_names), min_size=1, max_size=3, unique=True))
        for _ in range(segment_count)
    ]
    dtypes_per_segment = [
        draw(st.lists(st.sampled_from(dtypes), min_size=len(cols), max_size=len(cols))) for cols in columns_per_segment
    ]
    column_dtype_per_segment = [
        {name: dtype for name, dtype in zip(columns_per_segment[i], dtypes_per_segment[i])}
        for i in range(segment_count)
    ]
    assume(all(col_type is not None for col_type in compute_common_type_for_columns(column_dtype_per_segment).values()))
    for segment_index in range(segment_count):
        segment_column_names = columns_per_segment[segment_index]
        column_dtypes = dtypes_per_segment[segment_index]
        segment_start_date = segment_ranges[segment_index]
        segment_end_date = segment_ranges[segment_index + 1]
        segments.append(draw(dataframe(segment_column_names, column_dtypes, segment_start_date, segment_end_date)))
    return segments


@pytest.mark.skipif(not IS_PANDAS_TWO, reason="Some resampling parameters don't exist in Pandas < 2")
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe([f"col_{dtype}" for dtype in COLUMN_DTYPE], COLUMN_DTYPE, MIN_DATE, MAX_DATE),
    rule=rule(),
    origin=origin(),
    offset=offset(),
)
def test_resample(lmdb_version_store_v1, any_output_format, df, rule, origin, offset):
    # The assumption below is to avoid OOM-ing the GitHub runners.
    assume(dense_row_count_in_resampled_dataframe([df], rule) < 150000)

    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "sym"
    logger = get_logger()
    logger.info(f"Data frame generated has {df.shape[0]} rows")
    lib.write(sym, df)
    for closed in ["left", "right"]:
        for label in ["left", "right"]:
            columns = list(df.columns)
            agg = {f"{name}_{op}": (name, op) for name in columns for op in ALL_AGGREGATIONS}
            logger.debug(
                f"Exercise test with: rule={rule} closed=[{closed}], label={label}, origin={origin}, offset={offset}"
            )
            logger.debug(f"Aggregations: {agg}")
            try:
                generic_resample_test(
                    lib,
                    sym,
                    rule,
                    agg,
                    df,
                    origin=origin,
                    offset=offset,
                    closed=closed,
                    label=label,
                    # Must be int or uint column otherwise dropping of empty buckets will not work
                    drop_empty_buckets_for="col_uint",
                )
            except ValueError as pandas_error:
                # This is to avoid a bug in pandas related to how end an end_day work. It's possible that when end/end_day is used,
                # the first value of the data frame to be outside the computed resampling range. In the arctic, this is not a problem
                # as we allow this by design.
                if str(pandas_error) != "Values falls before first bin":
                    raise
                else:
                    return
            except RuntimeError as pandas_error:
                # This is a bug in pandas one that should be fixed in Pandas 2
                if str(pandas_error) == "empty group with uint64_t" and not IS_PANDAS_TWO:
                    return
                else:
                    raise


@use_of_function_scoped_fixtures_in_hypothesis_checked
@given(df_list=dynamic_schema_column_list(), rule=rule(), origin=origin(), offset=offset())
@settings(deadline=None, suppress_health_check=[HealthCheck.data_too_large])
def test_resample_dynamic_schema(
    lmdb_version_store_dynamic_schema_v1, any_output_format, df_list, rule, origin, offset
):
    # The assumption below is to avoid OOM-ing the GitHub runners.
    assume(dense_row_count_in_resampled_dataframe(df_list, rule) < 150000)

    common_column_types = compute_common_type_for_columns_in_df_list(df_list)
    lib = lmdb_version_store_dynamic_schema_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    lib.version_store.clear()
    sym = "sym"
    agg = {f"{name}_{op}": (name, op) for name in common_column_types for op in ALL_AGGREGATIONS}
    expected_types = {
        f"{name}_{op}": expected_aggregation_type(op, df_list, name)
        for name in common_column_types
        for op in ALL_AGGREGATIONS
    }
    for df in df_list:
        # This column will be used to keep track of empty buckets.
        df["_empty_bucket_tracker_"] = np.zeros(df.shape[0], dtype=int)
        lib.append(sym, df)

    for closed in ["left", "right"]:
        for label in ["left", "right"]:
            try:
                generic_resample_test(
                    lib,
                    sym,
                    rule,
                    agg,
                    pd.concat(df_list),
                    origin=origin,
                    offset=offset,
                    closed=closed,
                    label=label,
                    # Must be int or uint column otherwise dropping of empty buckets will not work
                    drop_empty_buckets_for="_empty_bucket_tracker_",
                    expected_types=expected_types,
                )
            except ValueError as pandas_error:
                # This is to avoid a bug in pandas related to how end an end_day work. It's possible that when end/end_day are used
                # the first value of the data frame to be outside the computed resampling range. In arctic this is not a problem
                # as we allow this by design.
                if str(pandas_error) != "Values falls before first bin":
                    raise
                else:
                    return
            except RuntimeError as pandas_error:
                # This is a bug in pandas one that should be fixed in Pandas 2
                if str(pandas_error) == "empty group with uint64_t" and not IS_PANDAS_TWO:
                    return
                else:
                    raise
