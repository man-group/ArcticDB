"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import NamedTuple, Callable, Any, Optional
from enum import Enum

import numpy as np
import pandas as pd

from arcticc.pb2.descriptors_pb2 import IndexDescriptor
from arcticdb.version_store._common import TimeFrame

from math import inf

from hypothesis import settings, HealthCheck, strategies as st
from hypothesis.extra.numpy import unsigned_integer_dtypes, integer_dtypes, floating_dtypes, from_dtype
import hypothesis.extra.pandas as hs_pd


_function_scoped_fixture = getattr(HealthCheck, "function_scoped_fixture", None)


def use_of_function_scoped_fixtures_in_hypothesis_checked(fun):
    """
    A decorator to declare that the use of function-scoped fixtures in said scope is correct,
    i.e. the state in the fixture from previous hypothesis tries will not affect future tries.
    See https://hypothesis.readthedocs.io/en/latest/healthchecks.html#hypothesis.HealthCheck.function_scoped_fixture
    Examples
    ========
    ```
    @use_of_function_scoped_fixtures_in_hypothesis_checked
    @given(...)
    def test_something(function_scope_fixture):
        ...
    ```
    """

    if not _function_scoped_fixture:
        return fun

    existing = getattr(fun, "_hypothesis_internal_use_settings", None)
    assert existing or getattr(fun, "is_hypothesis_test", False), "Must be used before @given/@settings"

    combined = {_function_scoped_fixture, *(existing or settings.default).suppress_health_check}
    new_settings = settings(parent=existing, suppress_health_check=combined)
    setattr(fun, "_hypothesis_internal_use_settings", new_settings)
    return fun


def non_infinite(x):
    return -inf < x < inf


def non_zero_and_non_infinite(x):
    return non_infinite(x) and x != 0


@st.composite
def integral_type_strategies(draw):
    return draw(from_dtype(draw(st.one_of([unsigned_integer_dtypes(), integer_dtypes()]))).filter(non_infinite))


@st.composite
def signed_integral_type_strategies(draw):
    return draw(from_dtype(draw(st.one_of([integer_dtypes()]))).filter(non_infinite))


@st.composite
def unsigned_integral_type_strategies(draw):
    return draw(from_dtype(draw(st.one_of([unsigned_integer_dtypes()]))).filter(non_infinite))


@st.composite
def dataframes_with_names_and_dtypes(draw, names, dtype_strategy):
    cols = [hs_pd.column(name, dtype=draw(dtype_strategy)) for name in names]
    return draw(hs_pd.data_frames(cols, index=hs_pd.range_indexes()))


@st.composite
def numeric_type_strategies(draw):
    return draw(
        from_dtype(draw(st.one_of([unsigned_integer_dtypes(), integer_dtypes(), floating_dtypes()]))).filter(
            non_infinite
        )
    )


@st.composite
def non_zero_numeric_type_strategies(draw):
    return draw(
        from_dtype(draw(st.one_of([unsigned_integer_dtypes(), integer_dtypes(), floating_dtypes()]))).filter(
            non_zero_and_non_infinite
        )
    )


string_strategy = st.text(
    min_size=1, alphabet=st.characters(blacklist_characters="'\\", min_codepoint=32, max_codepoint=126)
)


class FactoryReturn(NamedTuple):
    """
    Attributes
    ----------
    next_start_signed
        If positive, use it as the `start` parameter in a subsequent call to `InputFactories.make` for more data that
            immediately follows this `data` (for gap-less `append`-ing).
        If negative, appending without gap is not possible.
        In both cases, the absolute value of this is a valid `start` to `make()`.
    """

    data: Any
    next_start_signed: int


class _InputFactoryValues(NamedTuple):
    make: Callable[[int, int], FactoryReturn]
    index_kind: type(IndexDescriptor.Type.ROWCOUNT)
    input_type: str
    pandas_attr: Optional[str] = None
    compat_category: Optional[str] = None


_ROWCOUNT = IndexDescriptor.Type.ROWCOUNT
_TIMESTAMP = IndexDescriptor.Type.TIMESTAMP
_DAY = 86400 * 1000 * 1000 * 1000


def _day(d):
    return np.datetime64(d, "D")


def _dt_range(s, l):
    # NOTE: When using Pandas < 2.0, `datetime64` _always_ has the nanosecond resolution,
    # i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
    # Yet, this has changed in Pandas 2.0 and other resolution can be used,
    # i.e. Pandas 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
    # See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution  # noqa: E501
    # TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do not
    # rely uniquely on the resolution-less 'M' specifier if it this doable.
    return np.arange(_day(s), _day(s + l)).astype("datetime64[ns]")


class InputFactories(_InputFactoryValues, Enum):
    """
    Ways to make inputs to AN write/append/update methods that triggers different code paths in normalization plus some
    metadata about how it would be normalized.

    Attributes
    ----------
    make:
        The factory function
    index_kind
        The stored index type of the generated input
    input_type
        The Protobuf `NormalizationMetadata.input_type` field the generated output is expected to use
    pandas_attr
        An attribute in the `NormalizationMetadata.Pandas.index` that should be set for this kind of input.
    compat_category
        Factories with different categories produce input that are not append-able to each other.
        Falls back to input_type + pandas_attr

    DF_RC, DF_RC_STEP, DF_RC_NON_RANGE
        Produce DataFrames that are stored using row count indexes
    DF_DTI
        Produce DataFrames that use various datetime index types

    Factory Parameters
    ------------------
    start
        Relative value for the start of the index if the factory supports indexes.
        Interpreted by the factory as it sees fits, e.g. can be the row num of the first row or days since epoch in
        a datetime index.
    length
        The number of rows if the factory can produce multi-row results.
    """

    DF_RC = (lambda s, l: FactoryReturn(pd.DataFrame({"col": [s] * l}), s + l), _ROWCOUNT, "df")
    DF_RC_STEP = (
        lambda s, l: FactoryReturn(
            pd.DataFrame({"col": [s] * l}, index=pd.RangeIndex(s, s + l * 2, step=2)), s + l * 2
        ),
        _ROWCOUNT,
        "df",
        "step",
    )
    DF_RC_NON_RANGE = (
        # Create uneven steps, so cannot be converted to RangeIndex by mistake
        lambda s, l: FactoryReturn(
            pd.DataFrame({"col": [s] * l}, index=pd.Index(s * 1000 + i * 2 + (i & 1) for i in range(l))), -(s + l)
        ),
        _ROWCOUNT,
        "df",
        "is_not_range_index",
    )

    DF_DTI = (
        lambda s, l: FactoryReturn(pd.DataFrame({"col": [s] * l}, index=pd.DatetimeIndex(_dt_range(s, l))), s + l),
        _TIMESTAMP,
        "df",
    )
    # DF_PERIOD = (lambda s, l: pd.DataFrame({"col": [s] * l}, index=pd.period_range(_day(s), periods=l)), _TIMESTAMP, "df")

    DF_MULTI_RC = (
        lambda s, l: FactoryReturn(
            pd.DataFrame({"col": [s] * (l * 2)}, index=pd.MultiIndex.from_product([range(s, s + l), (1, -1)])), s + l
        ),
        _ROWCOUNT,
        "df",
        None,
        "multi",
    )

    # FUTURE: multi-index, categoricals. (Does anyone use IntervalIndex and TimedeltaIndex?)

    SERIES = (lambda s, _: FactoryReturn(pd.Series([s]), s + 1), _ROWCOUNT, "series")
    SERIES_W_NAME = (
        lambda s, _: FactoryReturn(pd.Series([s], name="col"), s + 1),
        _ROWCOUNT,
        "series",
        None,
        "series-col-name",
    )

    TIME_FRAME = (
        lambda s, l: FactoryReturn(
            TimeFrame(times=_dt_range(s, l), columns_names=["col"], columns_values=[np.full(l, fill_value=s)]), s + l
        ),
        _TIMESTAMP,
        "ts",
    )
    MSG_PACK = (lambda s, _: FactoryReturn(str(s), -1), _ROWCOUNT, "msg_pack_frame")
    # The normalizer converts the ndarray into a linear array, so creates two kinds to test shape handling:
    ND_ARRAY_1D = (lambda s, l: FactoryReturn(np.full(l, fill_value=s), s + 1), _ROWCOUNT, "np")
    ND_ARRAY_2D = (lambda s, l: FactoryReturn(np.full((l, 2), fill_value=s), s + 1), _ROWCOUNT, "np", None, "np-2d")

    @property
    def compat_category(self):
        return super().compat_category or f"{self.input_type}{self.index_kind}-{self.pandas_attr}"

    def __repr__(self):
        return f"{InputFactories.__name__}.{self.name}"


_ASSERTIONS_BY_INPUT_TYPE = dict(
    df=pd.testing.assert_frame_equal,
    series=pd.testing.assert_series_equal,
    np=np.testing.assert_equal,
    ts="normal",
    msg_pack_frame="normal",
)


def assert_equals_by_input_type(input_type: str, a, b):
    method = _ASSERTIONS_BY_INPUT_TYPE.get(input_type, None)
    assert method, "Unknown input_type " + input_type
    if method == "normal":
        assert a == b
    else:
        method(a, b)
