import pandas as pd
import numpy as np
import pytest
from hypothesis import given, reproduce_failure, settings, assume
import hypothesis.extra.pandas as hs_pd
import hypothesis.extra.numpy as hs_np
import hypothesis.strategies as st
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked
from arcticdb.util.test import generic_resample_test
from arcticdb.util._versions import IS_PANDAS_TWO
from hypothesis.extra.pandas import column

from arcticdb.util.utils import get_logger


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
    '''
    The dataframe returned will contain 3 64 bit columns with float64, int64, uint64 values.
    However the range of values in them will be limited due to the fact that we will run sum, mean operations which have internal 
    maximum of float64. Thus if we generate values to max 64 bits we would achieve overflow and this
    could lead to failures in comparison to Pandas. Ints are limited to 52 bit and floats to 32 bits 
    '''
    index = hs_pd.indexes(elements=date(min_date=MIN_DATE, max_date=MAX_DATE)
                          .filter(lambda d: d is not pd.NaT), min_size=1)
    columns = [column(name="col_float", elements=st.floats(width=32, 
                                                           allow_nan=True, allow_infinity=True), dtype=np.float64),
               column(name="col_int", elements=st.integers(-(2**52), 2**52 - 1), dtype=np.int64),
               column(name="col_uint", elements=st.integers(0, 2**52 - 1), dtype=np.uint64)]
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
    count = draw(st.integers(min_value=1, max_value=10_000))
    unit = draw(st.sampled_from(['min', 'h']))
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
    df=dataframe(),
    rule=rule(),
    origin=origin(),
    offset=offset()
)
def test_resample(lmdb_version_store_v1, df, rule, origin, offset):
    lib = lmdb_version_store_v1
    sym = "sym"
    logger = get_logger()
    logger.info(f"Data frame generated has {df.shape[0]} rows")
    lib.write(sym, df)
    for closed in ["left", "right"]:
        for label in ["left", "right"]:
            columns = list(df.columns)
            agg = {f"{name}_{op}": (name, op) for name in columns for op in ALL_AGGREGATIONS}
            logger.debug(f"Exercise test with: rule={rule} closed=[{closed}], label={label}, origin={origin}, offset={offset}")
            logger.debug(f"Aggregations: {agg}")
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
                # the first value of the data frame to be outside of the computed resampling range. In arctic this is not a problem
                # as we allow this by design.
                if str(pandas_error) != "Values falls before first bin":
                    raise pandas_error
                else:
                    return
