import pandas as pd
import numpy as np
import pytest
from hypothesis import given, settings, assume
import hypothesis.extra.pandas as hs_pd
import hypothesis.extra.numpy as hs_np
import hypothesis.strategies as st
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked
from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal
from arcticdb.util._versions import IS_PANDAS_TWO


COLUMN_DTYPE = ["float", "int", "uint"]
ALL_AGGREGATIONS = ["sum", "mean", "min", "max", "first", "last", "count"]
MIN_DATE = np.datetime64('1969-01-01')
MAX_DATE = np.datetime64('2000-01-01')

@st.composite
def date(draw, min_date, max_date):
    # Bound the start and end date so that we don't end up with too many buckets eating all RAM
    # Use some pre-epoch dates.
    # hs_np.from_dtype's min_value and max_value do not work with dates
    res = draw(hs_np.from_dtype(np.dtype("datetime64[ns]")))
    assume(min_date <= res and res <= max_date)
    return res

@st.composite
def dataframe(draw):
    index = hs_pd.indexes(elements=date(min_date=MIN_DATE, max_date=MAX_DATE), min_size=1).filter(lambda idx: pd.NaT not in idx)
    columns = [hs_pd.column(name=f"col_{dtype}", dtype=dtype) for dtype in COLUMN_DTYPE]
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
    This is used to check if a frequency is usable by Arctic. ArcticDB converts the frequency to signed 64 bit integer. 
    """
    billion = 1_000_000_000
    mult = {'h': 3600 * billion, 'min': 60 * billion, 's': billion}
    return (mult[unit] * count).bit_length() <= 63

@st.composite
def rule(draw):
    count = draw(st.integers(min_value=1))
    unit = draw(st.sampled_from(['min', 'h']))
    result = f"{count}{unit}"
    assume(freq_fits_in_64_bits(count=count, unit=unit))
    return result

@st.composite
def offset(draw):
    unit = draw(st.sampled_from(['s', 'min', 'h', None]))
    if unit is None:
        return None
    count = draw(st.integers(min_value=1))
    result = f"{count}{unit}"
    assume(freq_fits_in_64_bits(count=count, unit=unit))
    return result

def drop_inf_and_nan(df):
    return df[~df.isin([np.nan, np.inf, -np.inf]).any(axis=1)]

def pd_resample_and_drop_empty_buckets(df, rule, offset, closed, label, origin, agg):
    # Arctic drops empty buckets but Pandas does not. Add a column used only to track the number elements in the bucket.
    # If its count is 0 the row is dropped.
    count_col = '_count_col_'
    count_df = pd.DataFrame({count_col: np.zeros(len(df.index), dtype="int")}, index=df.index)
    to_resample = pd.concat([df, count_df], axis=1)
    resampled = to_resample.resample(rule, closed=closed, label=label, offset=offset, origin=origin).agg(None, **agg, _count_col_=(count_col, "count"))
    resampled = resampled[resampled[count_col] > 0]
    resampled.drop(columns=[count_col], inplace=True)
    return resampled

def assert_resampled_dfs_approximate(left, right):
    """
    Checks if integer columns are exactly the same. For float columns checks if they are approximately the same.
    We can't guarantee the same order of operations for the floats thus numerical errors might appear.
    """
    assert left.shape == right.shape
    assert left.columns.equals(right.columns)
    # To avoid checking the freq member of the index as arctic does not fill it in
    assert list(left.index) == list(right.index)

    # Drop NaN an inf values because. Pandas uses Kahan summation algorithm to improve numerical stability.
    # Thus they don't consistently overflow to infinity. Discussion: https://github.com/pandas-dev/pandas/issues/60303
    left_no_inf_and_nan = drop_inf_and_nan(left)
    right_no_inf_and_nan = drop_inf_and_nan(right)

    for col in left_no_inf_and_nan.columns:
        if pd.api.types.is_integer_dtype(left_no_inf_and_nan[col].dtype) and pd.api.types.is_integer_dtype(right_no_inf_and_nan[col].dtype):
            pd.testing.assert_series_equal(left_no_inf_and_nan[col], right_no_inf_and_nan[col], check_freq=False, check_flags=False, check_dtype=False)
        else:
            pd.testing.assert_series_equal(left_no_inf_and_nan[col], right_no_inf_and_nan[col], atol=1e-8, check_freq=False, check_flags=False, check_dtype=False)

@pytest.mark.skipif(not IS_PANDAS_TWO, reason="Some resampling parameters don't exist in Pandas < 2")
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe(),
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
            q = QueryBuilder()
            q = q.resample(rule, closed=closed, label=label, offset=offset, origin=origin).agg(agg)
            try:
                pd_resampled = pd_resample_and_drop_empty_buckets(df=df, rule=rule, offset=offset, closed=closed, label=label, origin=origin, agg=agg)
            except ValueError as pandas_error:
                # This is to avoid a bug in pandas related to how end an end_day work. It's possible that when end/end_day are used
                # the first value of the data frame to be outside of the computed resampling range. In arctic this is not a problem
                # as we allow this by design.
                if str(pandas_error) != "Values falls before first bin":
                    raise pandas_error
                else:
                    return
            # Column reordering is needed due to https://github.com/man-group/ArcticDB/issues/1996
            arctic_resample = lib.read(sym, query_builder=q).data
            arctic_resample = arctic_resample[agg.keys()]
            assert_resampled_dfs_approximate(pd_resampled, arctic_resample)