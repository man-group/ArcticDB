import pandas as pd
import numpy as np
import pytest
from hypothesis import given, settings, assume, HealthCheck
import hypothesis.extra.pandas as hs_pd
import hypothesis.strategies as st
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked
from arcticdb.util.test import generic_resample_test, larget_common_type, largest_numeric_type
from arcticdb.util._versions import IS_PANDAS_TWO


COLUMN_DTYPE = ["float", "int", "uint"]
ALL_AGGREGATIONS = ["sum", "mean", "min", "max", "first", "last", "count"]
MIN_DATE = np.datetime64('1969-01-01')
MAX_DATE = np.datetime64('2000-01-01')

@st.composite
def date(draw, min_date, max_date, unit="ns"):
    """
    Return a date between `min_date` and `max_date` using resolution `unit`.

    Getting a diff in nanoseconds and then using it get an offset in that range is faster than using the numpy date
    strategy. Using numpy's date strategy will also issue "too many failed filter operations" with a narrow date range

    Note
    --------
    This way of generation will not generate np.NaT
    """

    delta = (max_date - min_date).astype(f'timedelta64[{unit}]').astype(np.int64)
    unit_resolution = np.timedelta64(1, unit)
    if delta < unit_resolution:
        raise ValueError(f"Error when generating date in range {min_date} {max_date}. Time delta in {unit}={delta} is less than the resolution of {unit}={unit_resolution}.")
    offset_from_start_in_ns = draw(st.integers(min_value=0, max_value=delta))
    return min_date + np.timedelta64(offset_from_start_in_ns, unit)


@st.composite
def dataframe(draw, column_names, column_dtypes, min_date, max_date):
    index = hs_pd.indexes(elements=date(min_date=min_date, max_date=max_date), min_size=1)
    columns = [hs_pd.column(name=name, dtype=dtype) for (name, dtype) in zip(column_names, column_dtypes)]
    result = draw(hs_pd.data_frames(columns, index=index))
    result.sort_index(inplace=True)
    return result

@st.composite
def origin(draw):
    selected_origin = draw(st.sampled_from(["start", "end", "start_day", "end_day", "epoch", "timestamp"]))
    # Hypothesis may generate dates for year > 2200 and some of the arithmetic operation will overflow.
    if selected_origin == "timestamp":
        min_date = MIN_DATE - np.timedelta64(365, 'D')
        max_date = MAX_DATE + np.timedelta64(365, 'D')
        return pd.Timestamp(draw(date(min_date=min_date, max_date=max_date)))
    else:
        return selected_origin

def freq_fits_in_64_bits(count, unit):
    """
    This is used to check if a frequency is usable by Arctic. ArcticDB converts the frequency to signed 64-bit integer.
    """
    billion = 1_000_000_000
    mult = {'h': 3600 * billion, 'min': 60 * billion, 's': billion}
    return (mult[unit] * count).bit_length() <= 63

@st.composite
def rule(draw):
    count = draw(st.integers(min_value=1, max_value=10_000))
    unit = draw(st.sampled_from(['min', 'h', 's']))
    result = f"{count}{unit}"
    assume(freq_fits_in_64_bits(count=count, unit=unit))
    return result

@st.composite
def offset(draw):
    unit = draw(st.sampled_from(['s', 'min', 'h', None]))
    if unit is None:
        return None
    count = draw(st.integers(min_value=1, max_value=10_000))
    result = f"{count}{unit}"
    assume(freq_fits_in_64_bits(count=count, unit=unit))
    return result


@pytest.mark.skipif(not IS_PANDAS_TWO, reason="Some resampling parameters don't exist in Pandas < 2")
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe([f"col_{dtype}" for dtype in COLUMN_DTYPE], COLUMN_DTYPE, MIN_DATE, MAX_DATE),
    rule=rule(),
    origin=origin(),
    offset=offset()
)
def test_resample(lmdb_version_store_v1, df, rule, origin, offset):
    lib = lmdb_version_store_v1
    sym = "sym"
    lib.write(sym, df)
    for closed in ["left", "right"]:
        for label in ["left", "right"]:
            columns = list(df.columns)
            agg = {f"{name}_{op}": (name, op) for name in columns for op in ALL_AGGREGATIONS}
            try:
                generic_resample_test(
                    lib,
                    sym,
                    rule,
                    agg,
                    origin=origin,
                    offset=offset,
                    closed=closed,
                    label=label,
                    # Must be int or uint column otherwise dropping of empty buckets will not work
                    drop_empty_buckets_for="col_uint")
            except ValueError as pandas_error:
                # This is to avoid a bug in pandas related to how end an end_day work. It's possible that when end/end_day are used
                # the first value of the data frame to be outside the computed resampling range. In arctic this is not a problem
                # as we allow this by design.
                if str(pandas_error) != "Values falls before first bin":
                    raise pandas_error
                else:
                    return

@st.composite
def dynamic_schema_column_list(draw):
    all_column_names = [f"col_{i}" for i in range(10)]
    segment_count = draw(st.integers(min_value=1, max_value=100))
    segment_ranges = sorted(draw(st.lists(date(min_date=MIN_DATE, max_date=MAX_DATE, unit="s"), unique=True, min_size=segment_count+1, max_size=segment_count+1)))
    segments = []
    for segment_index in range(segment_count):
        segment_column_names = draw(st.lists(st.sampled_from(all_column_names), min_size=1, max_size=3, unique=True))
        column_count = len(segment_column_names)
        column_dtypes = draw(st.lists(st.sampled_from([np.int32, np.float32, np.int8]), min_size=column_count, max_size=column_count))
        segment_start_date = segment_ranges[segment_index]
        segment_end_date = segment_ranges[segment_index + 1]
        segments.append(draw(dataframe(segment_column_names, column_dtypes, segment_start_date, segment_end_date)))
    return segments

@use_of_function_scoped_fixtures_in_hypothesis_checked
@given(
    df_list=dynamic_schema_column_list(),
    rule=rule(),
    origin=origin(),
    offset=offset()
)
@settings(deadline=None, suppress_health_check=[HealthCheck.data_too_large])
def test_resample_dynamic_schema(lmdb_version_store_dynamic_schema_v1, df_list, rule, origin, offset):
    lib = lmdb_version_store_dynamic_schema_v1
    lib.version_store.clear()
    sym = "sym"
    columns = set()
    for df in df_list:
        columns |= set(df.columns)
        # This column will be used to keep track of empty buckets.
        df["_empty_bucket_tracker_"] = np.zeros(df.shape[0], dtype=int)
        lib.append(sym, df)

    for closed in ["left"]:
        for label in ["left"]:
            agg = {f"{name}_{op}": (name, op) for name in columns for op in ALL_AGGREGATIONS}
            try:
                generic_resample_test(
                    lib,
                    sym,
                    rule,
                    agg,
                    origin=origin,
                    offset=offset,
                    closed=closed,
                    label=label,
                    # Must be int or uint column otherwise dropping of empty buckets will not work
                    drop_empty_buckets_for="_empty_bucket_tracker_")
            except ValueError as pandas_error:
                # This is to avoid a bug in pandas related to how end an end_day work. It's possible that when end/end_day are used
                # the first value of the data frame to be outside the computed resampling range. In arctic this is not a problem
                # as we allow this by design.
                if str(pandas_error) != "Values falls before first bin":
                    raise pandas_error
                else:
                    return


@pytest.mark.parametrize("label", ["left", "right"])
@pytest.mark.parametrize("closed", ["left", "right"])
@pytest.mark.parametrize("dtype", [np.int32, np.float32, np.uint16])
def test_test(lmdb_version_store_dynamic_schema_v1, label, closed, dtype):
    rule='10ns'
    df1 = pd.DataFrame({'col_0': np.array([1], dtype)}, index=pd.DatetimeIndex([pd.Timestamp(0)]))
    df2 = pd.DataFrame({'col_1': np.array([2, 3], dtype)}, index=pd.to_datetime([pd.Timestamp(10), pd.Timestamp(20)]))
    df3 = pd.DataFrame({'col_0': np.array([4, 5], dtype)}, index=pd.to_datetime([pd.Timestamp(21), pd.Timestamp(30)]))
    df_list = [df1, df2, df3]

    lib = lmdb_version_store_dynamic_schema_v1
    lib.version_store.clear()
    sym = "sym"
    for df in df_list:
        lib.append(sym, df)

    agg = {f"{name}_{op}": (name, op) for name in ["col_0"] for op in ALL_AGGREGATIONS}
    expected_types = {
        "col_0_min": dtype,
        "col_0_max": dtype,
        "col_0_sum": largest_numeric_type(dtype),
        "col_0_mean": np.float64,
        "col_0_first": dtype,
        "col_0_last": dtype,
        "col_0_count": np.uint64,
    }
    generic_resample_test(
        lib,
        sym,
        rule,
        agg,
        pd.concat(df_list),
        expected_types,
        label=label,
        closed=closed,
        # Must be int or uint column otherwise dropping of empty buckets will not work
        drop_empty_buckets_for=None)

