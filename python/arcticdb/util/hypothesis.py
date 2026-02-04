"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import string
from dataclasses import dataclass
from typing import NamedTuple, Callable, Any, Optional
from enum import Enum

import numpy as np
import pandas as pd

from arcticc.pb2.descriptors_pb2 import IndexDescriptor
from arcticdb.version_store._common import TimeFrame
from arcticdb.util.test import unicode_symbols

from hypothesis import settings, HealthCheck, strategies as st
from hypothesis.extra.numpy import unsigned_integer_dtypes, integer_dtypes, floating_dtypes, from_dtype
from hypothesis.extra.pandas import column, data_frames, range_indexes
import hypothesis.extra.pandas as hs_pd

_function_scoped_fixture = getattr(HealthCheck, "function_scoped_fixture", None)


@dataclass
class column_strategy:
    name: str
    dtype_strategy: Any = None
    restrict_range: bool = False


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


def restricted_numeric_range(dtype):
    # Stick within the size of an int32 so that multiplication still fits inside an int64
    min_value = max(np.finfo(dtype).min if np.issubdtype(dtype, np.floating) else np.iinfo(dtype).min, -(2**31))
    max_value = min(np.finfo(dtype).max if np.issubdtype(dtype, np.floating) else np.iinfo(dtype).max, 2**31)
    return min_value, max_value


# Use the platform endianness everywhere
ENDIANNESS = "="


@st.composite
def signed_integral_type_strategies(draw):
    return draw(from_dtype(draw(st.one_of([integer_dtypes(endianness=ENDIANNESS)]))))


@st.composite
def unsigned_integral_type_strategies(draw):
    return draw(from_dtype(draw(st.one_of([unsigned_integer_dtypes(endianness=ENDIANNESS)]))))


@st.composite
def supported_integer_dtypes(draw):
    return draw(st.one_of(unsigned_integer_dtypes(endianness=ENDIANNESS), integer_dtypes(endianness=ENDIANNESS)))


@st.composite
def supported_floating_dtypes(draw):
    # Pandas comparison of float32 series to float64 values is buggy.
    # Change float_dtypes sizes to include 32 if this is fixed https://github.com/pandas-dev/pandas/issues/59524
    return draw(st.one_of(floating_dtypes(endianness=ENDIANNESS, sizes=[64])))


@st.composite
def supported_numeric_dtypes(draw):
    # Pandas comparison of float32 series to float64 values is buggy.
    # Change float_dtypes sizes to include 32 if this is fixed https://github.com/pandas-dev/pandas/issues/59524
    return draw(
        st.one_of(
            unsigned_integer_dtypes(endianness=ENDIANNESS),
            integer_dtypes(endianness=ENDIANNESS),
            floating_dtypes(endianness=ENDIANNESS, sizes=[64]),
        )
    )


@st.composite
def supported_string_dtypes(draw):
    return draw(st.just("object"))


string_strategy = st.text(
    min_size=1, alphabet=st.characters(blacklist_characters="'\\", min_codepoint=32, max_codepoint=126)
)


@st.composite
def dataframe_strategy(draw, column_strategies, min_size=0):
    cols = []
    for column_strat in column_strategies:
        dtype = draw(column_strat.dtype_strategy)
        if dtype == "object":
            elements = string_strategy
        else:
            min_value, max_value = restricted_numeric_range(dtype) if column_strat.restrict_range else (None, None)
            elements = from_dtype(
                dtype,
                allow_nan=False,
                allow_infinity=False,
                min_value=min_value,
                max_value=max_value,
            )
        cols.append(column(column_strat.name, dtype=dtype, elements=elements))
    return draw(data_frames(cols, index=range_indexes(min_size=min_size)))


@st.composite
def numeric_type_strategies(draw):
    dtype = draw(supported_numeric_dtypes())
    min_value, max_value = restricted_numeric_range(dtype)
    return draw(
        from_dtype(
            dtype,
            allow_nan=False,
            allow_infinity=False,
            min_value=min_value,
            max_value=max_value,
        )
    )


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
    if min_date == max_date:
        return min_date

    if isinstance(max_date, pd.Timestamp):
        delta = int((max_date - min_date) / pd.Timedelta(1, unit=unit))
    else:
        delta = (max_date - min_date).astype(f"timedelta64[{unit}]").astype(np.int64)

    unit_resolution = np.timedelta64(1, unit)
    if delta < unit_resolution:
        raise ValueError(
            f"Error when generating date in range {min_date} {max_date}. Time delta in {unit}={delta} is less than the resolution of {unit}={unit_resolution}."
        )
    offset_from_start_in_ns = draw(st.integers(min_value=0, max_value=delta))
    return min_date + np.timedelta64(offset_from_start_in_ns, unit)


@st.composite
def dataframe(draw, column_names, column_dtypes, min_date, max_date, index_name="index"):
    assert index_name not in column_names, f"Column name '{index_name}' conflicts with index name"
    index = hs_pd.indexes(elements=date(min_date=min_date, max_date=max_date), min_size=1)
    columns = []
    for name, dtype in zip(column_names, column_dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            # Cap the int size to be in the range of either (u)int32 or the range of the given dtype if it's smaller.
            # There are two reasons to cap int size:
            # 1. To avoid overflows (happens usually when the dtype is 64bit and sum aggregator is used)
            # 2. When dynamic schema is used. If we use pd.concat([df1, df2]) on two dataframes which are using dynamic
            #    schema segments, the columns which are missing in either df will become float64 in the result (so that
            #    the missing values can be NaN), however, if int64 is used some values won't be represented correctly,
            #    and the test will fail.
            current_byte_size = np.dtype(dtype).itemsize
            if current_byte_size <= 4:
                type_info = np.iinfo(dtype)
            else:
                is_signed = pd.api.types.is_signed_integer_dtype(np.dtype(dtype))
                capping_dtype = np.dtype("int32") if is_signed else np.dtype("uint32")
                type_info = np.iinfo(capping_dtype)
            min_value = type_info.min
            max_value = type_info.max
            columns.append(
                hs_pd.column(name=name, elements=st.integers(min_value=min_value, max_value=max_value), dtype=dtype)
            )
        elif pd.api.types.is_float_dtype(dtype):
            # The column will still be of the specified dtype (float32 or float64), but by asking hypothesis to generate
            # 16-bit floats, we reduce overflows. Pandas use Kahan summation, which can sometimes yield a different
            # result for overflows. Passing min_value and max_value will disable generation of NaN, -inf and
            # inf, which are supposed to work and have to be tested.
            columns.append(hs_pd.column(name=name, elements=st.floats(width=16), dtype=dtype))
        elif pd.api.types.is_object_dtype(dtype):
            # Object dtypes denote strings
            columns.append(
                hs_pd.column(
                    name=name,
                    dtype=dtype,
                    elements=st.one_of(
                        st.text(min_size=1, max_size=20, alphabet=string.ascii_letters + string.digits),  # ASCII
                        st.text(min_size=1, max_size=20, alphabet=unicode_symbols()),  # Unicode
                        st.just(np.nan),  # Missing value
                        st.just(None),  # Missing value
                    ),
                )
            )
        else:
            columns.append(hs_pd.column(name=name, dtype=dtype))
    result = draw(hs_pd.data_frames(columns, index=index))
    result.sort_index(inplace=True)
    result.index.name = index_name
    return result


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
        "is_physically_stored",
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
