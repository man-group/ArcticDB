from sqlite3 import Timestamp
from networkx import minimum_cut_value
import pandas as pd
import numpy as np
import pytest
from hypothesis import given, settings, HealthCheck
import hypothesis.extra.pandas as hs_pd
import hypothesis.extra.numpy as hs_np
import hypothesis.strategies as st
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked

COLUMN_DTYPE = [ "int16", "int32", "int64", "uint16", "uint32", "uint64", "float32", "float64"]

@st.composite
def dataframe(draw):
    column_count = draw(st.integers(min_value=1, max_value=10))
    column_names = [f"col_{i}" for i in range(column_count)]
    column_types = [draw(st.sampled_from(COLUMN_DTYPE)) for _ in range(column_count)]
    index_row_count = draw(st.integers(min_value=5000, max_value=200000))
    index = draw(hs_np.arrays(np.dtype("datetime64[ns]"), index_row_count).map(pd.DatetimeIndex).filter(lambda idx: pd.NaT not in idx))
    data = {name: draw(hs_np.arrays(np.dtype(dtype), len(index))) for name, dtype in zip(column_names, column_types)}
    res = pd.DataFrame(data, index=index)
    res.sort_index(inplace=True)
    return res

@st.composite
def origin(draw):
    selected_origin = draw(st.list(["start", "end", "start_day", "end_day", "epoch", "timestamp"]))
    return pd.Timestamp(draw(st.integers(minimum_cut_value=0))) if selected_origin == "timestamp" else selected_origin

@st.composite
def rule(draw):
    size = draw(st.integers(min_value=1))
    unit = draw(st.lists(['s', 'm', 'd', 'w', 'h']))
    return f"{size}{unit}"

@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None, suppress_health_check=[HealthCheck.data_too_large, HealthCheck.too_slow])
@given(df=dataframe(), rule=rule(), origin=origin(), offset=rule())
def test_sort_merge_static_schema_write(df):
    for closed in ["left", "right"]:
        for label in ["left", "right"]:
            resampled = df.resample(rule=rule, origin=origin, offset=offset, closed=closed, label=label)
