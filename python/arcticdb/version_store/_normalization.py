"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import copy
import datetime
from datetime import timedelta
import math

import numpy as np
import os
import sys
import pandas as pd
import pickle
from abc import ABCMeta, abstractmethod

from pandas.api.types import is_integer_dtype
from arcticc.pb2.descriptors_pb2 import UserDefinedMetadata, NormalizationMetadata, MsgPackSerialization
from arcticc.pb2.storage_pb2 import VersionStoreConfig
from collections import Counter
from arcticdb.exceptions import ArcticNativeException, ArcticDbNotYetImplemented, NormalizationException, SortingException
from arcticdb.supported_types import DateRangeInput, time_types as supported_time_types
from arcticdb.util._versions import IS_PANDAS_TWO, IS_PANDAS_ZERO
from arcticdb.version_store.read_result import ReadResult
from arcticdb_ext.version_store import SortedValue as _SortedValue
from pandas.core.internals import make_block

from pandas import DataFrame, MultiIndex, Series, DatetimeIndex, Index, RangeIndex
from typing import Dict, NamedTuple, List, Union, Mapping, Any, TypeVar, Tuple

from arcticdb._msgpack_compat import packb, padded_packb, unpackb, ExtType
from arcticdb.log import version as log
from arcticdb.version_store._common import _column_name_to_strings, TimeFrame

PICKLE_PROTOCOL = 4

from pandas._libs.tslib import Timestamp
from pandas._libs.tslibs.timezones import get_timezone


try:
    from pandas._libs.tslibs.timezones import is_utc as check_is_utc_if_newer_pandas
except ImportError:
    # not present in pandas==0.22. Safe to remove when all clients upgrade.
    assert pd.__version__.startswith(
        "0"
    ), "is_utc not present in this Pandas - has it been changed in latest Pandas release?"

    def check_is_utc_if_newer_pandas(*args, **kwargs):
        return False  # the UTC specific issue is not present in old Pandas so no need to go down special case


IS_WINDOWS = sys.platform == "win32"


NPDDataFrame = NamedTuple(
    "NPDDataFrame",
    [
        # DO NOT REORDER, positional access used in c++
        ("index_names", List[str]),
        ("column_names", List[str]),
        ("index_values", List[np.ndarray]),
        ("columns_values", List[np.ndarray]),
        ("sorted", _SortedValue),
    ],
)

NormalizedInput = NamedTuple("NormalizedInput", [("item", NPDDataFrame), ("metadata", NormalizationMetadata)])


# To simplify unit testing of serialization logic. This maps the cpp _FrameData exposed object
class FrameData(
    NamedTuple("FrameData", [("data", List[np.ndarray]), ("names", List[str]), ("index_columns", List[str])])
):
    @staticmethod
    def from_npd_df(df):
        # type: (NPDDataFrame)->FrameData
        return FrameData(df.index_values + df.columns_values, names=df.column_names, index_columns=df.index_names)

    @staticmethod
    def from_cpp(fd):
        # type: (Any)->FrameData

        if isinstance(fd, FrameData):
            return fd
        else:
            return FrameData(fd.value.data, fd.names, fd.index_columns)


# NOTE: When using Pandas < 2.0, `datetime64` _always_ uses nanosecond resolution,
# i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
# Yet, this has changed in Pandas 2.0 and other resolution can be used,
# i.e. Pandas >= 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
# See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution  # noqa: E501
# TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do not
# rely uniquely on the resolution-less 'M' specifier if it this doable.
DTN64_DTYPE = "datetime64[ns]"

# All possible value of the "object" dtype for pandas.Series.
OBJECT_TOKENS = (object, "object", "O")

_SUPPORTED_TYPES = Union[DataFrame]  # , Series]
_SUPPORTED_NATIVE_RETURN_TYPES = Union[FrameData]


def _accept_array_string(v):
    # TODO remove this once arctic keeps the string type under the hood
    # and does not transform string into bytes
    return type(v) in (str, bytes)


def _is_nan(element):
    return (isinstance(element, np.floating) or isinstance(element, float)) and math.isnan(element)


def _is_nat(element):
    return isinstance(element, type(pd.NaT)) and pd.isna(element)


def get_sample_from_non_empty_arr(arr, arr_name):
    for element in arr:
        if element is None:
            continue

        if _is_nan(element):
            continue

        if _is_nat(element):
            continue

        return element

    log.info("Column {} does not have non null elements.", arr_name)
    return None


def coerce_string_column_to_fixed_length_array(arr, to_type, string_max_len):
    # in python3 all text will be treated as unicode
    if to_type == str:
        if sys.platform == "win32":
            # See https://sourceforge.net/p/numpy/mailman/numpy-discussion/thread/1139250278.7538.52.camel%40localhost.localdomain/#msg11998404
            # Different wchar size on Windows is not compatible with our current internal representation of Numpy strings
            raise ValueError("Numpy strings are not supported on Windows - use Python strings instead")
        casted_arr = arr.astype("<U" if string_max_len is None else "<U{:d}".format(string_max_len))
    else:
        casted_arr = arr.astype("S" if string_max_len is None else "S{:d}".format(string_max_len))
        log.debug("converted {} to {}".format(arr.dtype, casted_arr.dtype))

    return casted_arr


def get_timezone_from_metadata(norm_meta):
    if len(norm_meta.index.tz):
        return norm_meta.index.tz

    if len(norm_meta.multi_index.tz):
        return norm_meta.multi_index.tz

    return None


def _to_primitive(arr, arr_name, dynamic_strings, string_max_len=None, coerce_column_type=None, norm_meta=None):
    arr_dtype_as_str = str(arr.dtype)
    if "pyarrow" in arr_dtype_as_str:
        raise ArcticDbNotYetImplemented(
            "PyArrow-backed pandas DataFrame and Series are not currently supported by ArcticDB. \n"
            "Please convert your pandas DataFrame and Series to use NumPy array before using them with ArcticDB. \n"
            "If you are interested in the support of PyArrow-backed pandas DataFrame and Series, please upvote this \n"
            "GitHub issue and participate to its discussions: https://github.com/man-group/ArcticDB/issues/881"
        )

    if isinstance(arr.dtype, pd.core.dtypes.dtypes.CategoricalDtype):
        if is_integer_dtype(arr.categories.dtype):
            norm_meta.common.int_categories[arr_name].category.extend(arr.categories)
        else:
            norm_meta.common.categories[arr_name].category.extend(arr.categories)
        return arr.codes

    # This check has to come after the categorical check above, as Categoricals are a Pandas concept, not numpy, which
    # causes issubdtype to throw if arr.dtype == CategoricalDtype
    if np.issubdtype(arr.dtype, np.timedelta64):
        raise ArcticDbNotYetImplemented(f"Failed to normalize column '{arr_name}' with unsupported dtype '{arr.dtype}'")

    if np.issubdtype(arr.dtype, np.datetime64):
        # ArcticDB only operates at nanosecond resolution (i.e. `datetime64[ns]`) type because so did Pandas < 2.
        # In Pandas >= 2.0, other resolution are supported (namely `ms`, `s`, and `us`).
        # See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution  # noqa: E501
        # We want to maintain consistent behaviour, so we convert any other resolution
        # to `datetime64[ns]`.
        arr = arr.astype(DTN64_DTYPE, copy=False)

    # TODO(jjerphan): Remove once pandas < 2 is not supported anymore.
    if not IS_PANDAS_TWO and len(arr) == 0 and arr.dtype == "float":
        # In Pandas < 2, empty series dtype is `"float"`, but as of Pandas 2.0, empty series dtype is `"object"`
        # We cast its array to `"object"` so that the EMPTY type can be used, and the type can be promoted correctly
        # then.
        return arr.astype("object")

    if arr.dtype.hasobject is False and not (
        dynamic_strings and arr.dtype == "float" and coerce_column_type in OBJECT_TOKENS
    ):
        # not an object type numpy column and not going to later be
        # coerced to an object type column - does not require conversion to a primitive type.
        return arr

    if len(arr) == 0:
        if coerce_column_type is not None:
            # Casting an empty Series using None returns a Series with of "float64" dtype in any version of pandas…
            return arr.astype(coerce_column_type)
        else:
            return arr

    # Coerce column allows us to force a column to the given type, which means we can skip expensive iterations in
    # Python with the caveat that if the user gave an invalid type it's going to blow up in the core.
    if coerce_column_type and (coerce_column_type == float or isinstance(coerce_column_type(), np.floating)):
        log.debug("Coercing column: {} to type: {}", arr_name, coerce_column_type)
        """
        This is useful in the cases where we had an object column like: pd.DataFrame({'col1': ['1', np.nan]})
        and the string element was reset to np.nan which doesn't fix the dtype and would force pickling.
        """
        return arr.astype("f")
    elif coerce_column_type and coerce_column_type in OBJECT_TOKENS and dynamic_strings:
        return arr.astype("object")
    elif coerce_column_type and _accept_array_string(coerce_column_type()):
        # Save the time for iteration if the user tells us explicitly it's a string column.
        log.debug("Coercing column: {} to type: {}", arr_name, coerce_column_type)
        if dynamic_strings:
            return arr
        log.info("Coercing to string/unicode column type is only supported for dynamic_strings param.")

    # This is an expensive loop in python if you have highly sparse data with concrete values coming quite late.
    sample = get_sample_from_non_empty_arr(arr, arr_name)

    if isinstance(sample, Timestamp):
        # If we have pd.Timestamp as the sample, then:
        # - 1: check they all have the same timezone
        tz_matches = np.vectorize(lambda element: pd.isna(element) or (isinstance(element, pd.Timestamp) and element.tz == sample.tz))
        if not (tz_matches(arr)).all():
            raise NormalizationException(f"Failed to normalize column {arr_name}: first non-null element found is a "
                                         f"Timestamp with timezone '{sample.tz}', but one or more subsequent elements "
                                         f"are either not Timestamps or have differing timezones, neither of which is "
                                         f"supported.")
        # - 2: try and clean up all NaNs inside it.
        log.debug("Removing all NaNs from column: {} of type datetime64", arr_name)
        return arr.astype(DTN64_DTYPE)
    elif _accept_array_string(sample):
        if dynamic_strings:
            return arr
        else:
            log.debug("Converting  array with dtype=object to native string. This might be a costly operation")
            casted_arr = coerce_string_column_to_fixed_length_array(arr, type(sample), string_max_len)
    elif dynamic_strings and sample is None:  # arr is entirely empty
        return arr
    else:
        raise ArcticDbNotYetImplemented(
            f"Failed to normalize column '{arr_name}' with dtype '{arr.dtype}'. Found first non-null value of type "
            f"'{type(sample)}', but only strings, unicode, and Timestamps are supported. "
            f"Do you have mixed dtypes in your column?"
        )
    

    # Pick any unwanted data conversions (e.g. np.nan to 'nan') or None to the string 'None'
    if np.array_equal(arr, casted_arr):
        return casted_arr
    else:
        if None in arr:
            raise ArcticDbNotYetImplemented(
                "You have a None object in the numpy array at positions={} Column type={} for column={} "
                "which cannot be normalized.".format(np.where(arr == None), arr.dtype, arr_name)
            )
        else:
            raise ArcticDbNotYetImplemented(
                "Could not convert this column={} of type 'O' to a primitive type. ".format(arr_name)
            )


# Roundtrip through pd.Timestamp object to avoid possible issues with
# python's native datetime and pytz timezone
def _to_tz_timestamp(dt):
    # type: (datetime.datetime)->(int, str)
    ts = pd.Timestamp(
        year=dt.year,
        month=dt.month,
        day=dt.day,
        hour=dt.hour,
        minute=dt.minute,
        second=dt.second,
        microsecond=dt.microsecond,
    ).value
    tz = dt.tzinfo.zone if dt.tzinfo is not None else None
    return [ts, tz]


def _from_tz_timestamp(ts, tz):
    # type: (int, Optional[str])->(datetime.datetime)
    return pd.Timestamp(ts).tz_localize(tz).to_pydatetime(warn=False)


_range_index_props_are_public = hasattr(RangeIndex, "start")


def _normalize_single_index(index, index_names, index_norm, dynamic_strings=None, string_max_len=None, empty_types=False):
    # index: pd.Index or np.ndarray -> np.ndarray
    index_tz = None
    is_empty = len(index) == 0
    if empty_types and is_empty and not index_norm.is_physically_stored:
        return [], []
    elif isinstance(index, RangeIndex):
        if index.name:
            if not isinstance(index.name, int) and not isinstance(index.name, str):
                raise NormalizationException(
                    f"Index name must be a string or an int, received {index.name} of type {type(index.name)}"
                )
            if isinstance(index.name, int):
                index_norm.is_int = True
            index_norm.name = str(index.name)
        index_norm.start = index.start if _range_index_props_are_public else index._start
        index_norm.step = index.step if _range_index_props_are_public else index._step
        return [], []
    else:
        coerce_type = DTN64_DTYPE if len(index) == 0 else None
        index_vals = index
        if not isinstance(index, np.ndarray):
            index_vals = index.values
        ix_vals = [
            _to_primitive(
                index_vals, index_names, dynamic_strings, coerce_column_type=coerce_type, string_max_len=string_max_len
            )
        ]
        if index_names[0] is None:
            index_names = ["index"]
            if isinstance(index_norm, NormalizationMetadata.PandasIndex):
                index_norm.fake_name = True
            else:
                index_norm.fake_field_pos.append(0)
            log.debug("Index has no name, defaulting to 'index'")
        if isinstance(index, DatetimeIndex) and index.tz is not None:
            index_tz = get_timezone(index.tz)
        elif (
            len(index) > 0
            and (isinstance(index[0], datetime.datetime) or isinstance(index[0], pd.Timestamp))
            and index[0].tzinfo is not None
        ):
            index_tz = get_timezone(index[0].tzinfo)

        if index_tz is not None:
            index_norm.tz = _ensure_str_timezone(index_tz)

        if not isinstance(index_names[0], int) and not isinstance(index_names[0], str):
            raise NormalizationException(
                f"Index name must be a string or an int, received {index_names[0]} of type {type(index_names[0])}"
            )
        # Currently, we only support a single index column
        # when we support multi-index, we will need to implement a similar logic to the one in _normalize_columns_names
        # in the mean time, we will cast all other index names to string, so we don't crash in the cpp layer
        if isinstance(index_names[0], int):
            index_norm.is_int = True
        index_norm.name = str(index_names[0])
        return [str(name) for name in index_names], ix_vals


def _ensure_str_timezone(index_tz):
    if isinstance(index_tz, datetime.tzinfo) and check_is_utc_if_newer_pandas(index_tz):
        # Pandas started to treat UTC as a special case and give back the tzinfo object for it. We coerce it back to
        # a str to avoid special cases for it further along our pipeline. The breaking change was:
        # https://github.com/jbrockmendel/pandas/commit/94ce05d1bcc3c99e992c48cc99d0fd2726f43102#diff-3dba9e959e6ad7c394f0662a0e6477593fca446a6924437701ecff82b0b20b55
        return "UTC"
    else:
        return index_tz


def _denormalize_single_index(item, norm_meta):
    # item: np.ndarray -> pd.Index()
    rtn = Index([])
    if len(item.index_columns) == 0:
        # when then initial index was a RangeIndex
        if norm_meta.WhichOneof("index_type") == "index" and not norm_meta.index.is_physically_stored:
            if len(item.data) > 0:
                if hasattr(norm_meta.index, "step") and norm_meta.index.step != 0:
                    stop = norm_meta.index.start + norm_meta.index.step * len(item.data[0])
                    name = norm_meta.index.name if norm_meta.index.name else None
                    return RangeIndex(start=norm_meta.index.start, stop=stop, step=norm_meta.index.step, name=name)
                else:
                    return DatetimeIndex([])
            else:
                return RangeIndex(start=0, stop=0, step=1)
        # this means that the index is not a datetime index and it's been represented as a regular field in the stream
        item.index_columns.append(item.names.pop(0))

    if len(item.index_columns) == 1:
        name = int(item.index_columns[0]) if norm_meta.index.is_int else item.index_columns[0]
        rtn = Index(item.data[0] if len(item.data) > 0 else [], name=name)

        tz = get_timezone_from_metadata(norm_meta)
        if isinstance(rtn, DatetimeIndex) and tz:
            rtn = rtn.tz_localize("UTC").tz_convert(tz)
    return rtn


def _denormalize_columns_names(columns_names, norm_meta):
    if columns_names is None:
        return None
    for idx in range(len(columns_names)):
        col = columns_names[idx]
        if col in norm_meta.common.col_names:
            col_data = norm_meta.common.col_names[col]
            if col_data.is_none:
                columns_names[idx] = None
            elif col_data.is_empty:
                columns_names[idx] = ""
            elif col_data.is_int:
                columns_names[idx] = int(col_data.original_name)
            else:
                columns_names[idx] = col_data.original_name

    return columns_names


def _denormalize_columns(item, norm_meta, idx_type, n_indexes):
    columns = None
    data = None
    denormed_columns = None
    if len(item.names) > 0:
        if norm_meta.has_synthetic_columns and idx_type != "multi_index":
            columns = RangeIndex(0, len(item.names))
        else:
            columns = item.names
            if len(norm_meta.common.col_names) > 0:
                denormed_columns = _denormalize_columns_names(copy.deepcopy(columns), norm_meta)
            else:
                denormed_columns = columns
        if len(item.data) == 0:
            data = None
        else:
            data = {n: item.data[i + n_indexes] if i < len(item.data) else [] for i, n in enumerate(columns)}
    return columns, denormed_columns, data


def _normalize_columns_names(columns_names, index_names, norm_meta, dynamic_schema=False):
    counter = Counter(columns_names)
    for idx in range(len(columns_names)):
        col = columns_names[idx]
        if col is None:
            if dynamic_schema and counter[col] > 1:
                raise ArcticNativeException("Multiple None columns not allowed in dynamic_schema")
            new_name = "__none__{}".format(0 if dynamic_schema else idx)
            norm_meta.common.col_names[new_name].is_none = True
            columns_names[idx] = new_name
            continue

        if not isinstance(col, str) and not isinstance(col, int):
            raise NormalizationException(
                f"Column names must be of type str or int, received {col} of type {type(col)} on column number {idx}"
            )

        col_str = str(col)
        columns_names[idx] = col_str
        if len(col_str) == 0:
            if dynamic_schema and counter[col] > 1:
                raise ArcticNativeException("Multiple '' columns not allowed in dynamic_schema")
            new_name = "__empty__{}".format(0 if dynamic_schema else idx)
            norm_meta.common.col_names[new_name].is_empty = True
            columns_names[idx] = new_name
        else:
            if dynamic_schema and (counter[col] > 1):
                raise ArcticNativeException("Same column names not allowed in dynamic_schema")
            new_name = col_str
            if counter[col] > 1 or col in index_names:
                new_name = "__col_{}__{}".format(col, 0 if dynamic_schema else idx)
            if isinstance(col, int):
                norm_meta.common.col_names[new_name].is_int = True
            norm_meta.common.col_names[new_name].original_name = col_str
            columns_names[idx] = new_name

    return columns_names


def _normalize_columns(
    columns_names,
    columns_vals,
    norm_meta,
    coerce_columns=None,
    dynamic_strings=None,
    string_max_len=None,
    dynamic_schema=False,
    index_names=[],
):
    # TODO optimize this away when RangeIndex for columns and gen in c++
    columns_names_norm = list(map(str, columns_names))
    if not isinstance(columns_names, RangeIndex):
        if coerce_columns is not None and (set(columns_names_norm) != set(coerce_columns.keys())):
            raise ArcticNativeException("Keys in coerce column dictionary must match columns in dataframes")
        columns_names_norm = _normalize_columns_names(list(columns_names), index_names, norm_meta, dynamic_schema)

        if columns_names_norm != list(columns_names):
            log.debug("Dataframe column names normalized")

    if len(columns_names_norm) != len(columns_vals):
        raise ArcticNativeException(
            "mismatch in columns_name and vals size in _normalize_columns {} != {}".format(
                len(columns_names_norm), len(columns_vals)
            )
        )
    column_vals = [
        _to_primitive(
            columns_vals[idx],
            columns_names_norm[idx],
            string_max_len=string_max_len,
            dynamic_strings=dynamic_strings,
            coerce_column_type=coerce_columns[str(columns_names[idx])] if coerce_columns else None,
            norm_meta=norm_meta,
        )
        for idx in range(len(columns_names_norm))
    ]
    return columns_names_norm, column_vals


class Normalizer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def normalize(self, item, **kwargs):
        # type: (Any)->NormalizedInput
        pass

    @abstractmethod
    def denormalize(self, item, norm_meta):
        # type: (Any, NormalizationMetadata)->Any
        pass


_IDX_PREFIX = "__idx__"
_IDX_PREFIX_LEN = len(_IDX_PREFIX)


class _PandasNormalizer(Normalizer):
    def _index_to_records(self, df, pd_norm, dynamic_strings, string_max_len, empty_types):
        index = df.index
        empty_df = len(index) == 0
        if empty_df and empty_types:
            index_norm = pd_norm.index
            index_norm.is_physically_stored = False
            index = DatetimeIndex([])
        elif isinstance(index, MultiIndex):
            # This is suboptimal and only a first implementation since it reduplicates the data
            index_norm = pd_norm.multi_index
            index_norm.field_count = len(index.levels) - 1
            fields = list(range(1, len(index.levels)))
            names = [_column_name_to_strings(index.names[0])]
            for f in fields:
                current_index = index.levels[f]
                if isinstance(current_index, DatetimeIndex) and current_index.tz is not None:
                    index_norm.timezone[f] = _ensure_str_timezone(get_timezone(current_index.tz))
                else:
                    index_norm.timezone[f] = ""
                if index.names[f] is None:
                    index_norm.fake_field_pos.append(f)
                    names.append("__fkidx__{}".format(f))
                else:
                    names.append("{}{}".format(_IDX_PREFIX, index.names[f]))
            df.index.names = names
            df.reset_index(fields, inplace=True)
            index = df.index
        else:
            index_norm = pd_norm.index
            index_norm.is_physically_stored = not isinstance(index, RangeIndex) and not empty_df
            is_categorical = len(df.select_dtypes(include="category").columns) > 0
            index = DatetimeIndex([]) if IS_PANDAS_TWO and empty_df and not is_categorical else index

        return _normalize_single_index(index, list(index.names), index_norm, dynamic_strings, string_max_len, empty_types=empty_types)

    def _index_from_records(self, item, norm_meta):
        # type: (NormalizationMetadata.Pandas, _SUPPORTED_NATIVE_RETURN_TYPES, Bool)->Union[Index, DatetimeIndex, MultiIndex]

        return _denormalize_single_index(item, norm_meta)

    def normalize(self, item, string_max_len=None, **kwargs):
        raise NotImplementedError

    def denormalize(self, item, norm_meta):
        raise NotImplementedError


def corrected_index_name(index, norm_meta):
    if index is None and norm_meta.common.index.name:
        return norm_meta.common.index.name
    elif norm_meta.common.index.fake_name:
        return None
    elif index is not None:
        return index.name


class SeriesNormalizer(_PandasNormalizer):
    # Placeholder as it is currently unused
    TYPE = "series"

    def __init__(self):
        self._df_norm = DataFrameNormalizer()

    def normalize(self, item, string_max_len=None, dynamic_strings=False, coerce_columns=None, empty_types=False, **kwargs):
        df, norm = self._df_norm.normalize(
            item.to_frame(),
            dynamic_strings=dynamic_strings,
            string_max_len=string_max_len,
            coerce_columns=coerce_columns,
            empty_types=empty_types
        )
        norm.series.CopyFrom(norm.df)
        if item.name is not None:
            norm.series.common.name = _column_name_to_strings(item.name)
            norm.series.common.has_name = True
        # else protobuf bools default to False

        return NormalizedInput(item=df, metadata=norm)

    def denormalize(self, item, norm_meta):
        # type: (_FrameData, NormalizationMetadata.PandaDataFrame)->DataFrame

        df = self._df_norm.denormalize(item, norm_meta)

        series = pd.Series() if df.columns.empty else df.iloc[:, 0]

        if len(norm_meta.common.name) or norm_meta.common.has_name:
            series.name = norm_meta.common.name
        else:
            # Either the Series was written with a new client that understands the has_name field, and it was None, or
            # the Series was written by an older client as either an empty string or None, we cannot tell, so maintain
            # behaviour as it was before the has_name field was added
            series.name = None

        return series


class NdArrayNormalizer(Normalizer):
    TYPE = "ndarray"

    def normalize(self, item, **kwargs):
        if IS_WINDOWS and item.dtype.char == "U":
            raise ArcticDbNotYetImplemented("Numpy strings are not yet implemented on Windows")  # SKIP_WIN
        norm_meta = NormalizationMetadata()
        norm_meta.np.shape.extend(item.shape)

        # Currently we reshape and store any dimensional ndarray in a single column and store the shape in
        # the protobuf which is used during denorm. This can be problematic as this might lead to a lot of
        # (MAX_ROWS x 1) segments instead of an even distribution for now.
        return NormalizedInput(
            item=NPDDataFrame(
                index_names=[],
                index_values=[],
                column_names=["ndarray"],
                columns_values=[item.reshape(np.prod(item.shape))],
                sorted=_SortedValue.UNKNOWN,
            ),
            metadata=norm_meta,
        )

    def denormalize(self, item, norm_meta):
        original_shape = tuple(norm_meta.shape)
        data = item.data[0]
        return data.reshape(original_shape)


from pandas.core.internals import BlockManager


class BlockManagerUnconsolidated(BlockManager):
    def __init__(self, *args, **kwargs):
        BlockManager.__init__(self, *args, **kwargs)
        self._is_consolidated = False
        self._known_consolidated = False

    def _consolidate_inplace(self):
        pass

    def _consolidate(self):
        return self.blocks


class DataFrameNormalizer(_PandasNormalizer):
    TYPE = "df"

    def __init__(self, *args, **kwargs):
        super(DataFrameNormalizer, self).__init__(*args, **kwargs)
        self._skip_df_consolidation = os.getenv("SKIP_DF_CONSOLIDATION") is not None

    def df_without_consolidation(self, columns, index, item, n_indexes, data):
        """
        This is a hack that allows us to monkey-patch the DataFrame Block Manager so it doesn't do any
        consolidation and instead just creates a separate block for each column thus skipping an extra copy
        during the consolidation for similarly typed columns.

        :return: Dataframe with all columns without any consolidation.
        """

        def df_from_arrays(arrays, cols, ind, n_ind):
            def gen_blocks():
                _len = len(index)
                column_placement_in_block = 0
                for idx, a in enumerate(arrays):
                    if idx < n_ind:
                        continue
                    # In Pandas 1 the dtype param of make_block is ignored for empty blocks and the dtype is always object
                    # Pre-empty type Arctic has a default dtype of float64 for empty columns. Thus a casting to float64
                    # is needed.
                    # Note: empty datetime cannot be cast to float64
                    # TODO: Remove the casting after empty types become the only option
                    # Issue: https://github.com/man-group/ArcticDB/issues/1562
                    block = make_block(values=a.reshape((1, _len)), placement=(column_placement_in_block,))
                    yield block.astype(np.float64) if _len == 0 and block.dtype == np.dtype('object') and not IS_PANDAS_TWO else block
                    column_placement_in_block += 1

            if cols is None or len(cols) == 0:
                return pd.DataFrame(data, index=ind, columns=cols)

            blocks = tuple(gen_blocks())
            if not isinstance(cols, Index):
                cols = Index(cols)

            mgr = BlockManagerUnconsolidated(blocks=blocks, axes=[cols, ind])
            return pd.DataFrame(mgr, copy=False)

        return df_from_arrays(item.data, columns, index, n_indexes)

    # @profile
    def denormalize(self, item, norm_meta):
        # type: (_FrameData, NormalizationMetadata.PandaDataFrame)->DataFrame

        if norm_meta.HasField("multi_columns"):
            raise ArcticDbNotYetImplemented(
                "MultiColumns are not implemented. Normalization meta: {}".format(str(norm_meta))
            )

        index = self._index_from_records(item, norm_meta.common)
        n_indexes = len(item.index_columns)
        idx_type = norm_meta.common.WhichOneof("index_type")

        columns, denormed_columns, data = _denormalize_columns(item, norm_meta, idx_type, n_indexes)

        if not self._skip_df_consolidation:
            df = DataFrame(data, index=index, columns=columns)
            # Setting the columns' dtype manually, since pandas might just convert the dtype of some
            # (empty) columns to another one and since the `dtype` keyword for `pd.DataFrame` constructor
            # does not accept a mapping such as `columns_dtype`.
            # For instance the following code has been tried but returns a pandas.DataFrame full of NaNs:
            #
            #       columns_mapping = {} if data is None else {
            #           name: pd.Series(np_array, index=index, dtype=np_array.dtype)
            #           for name, np_array in data.items()
            #       }
            #       df = DataFrame(index=index, columns=columns_mapping, copy=False)
            #
            if not IS_PANDAS_TWO:
                # TODO(jjerphan): Remove once pandas < 2 is not supported anymore.
                # Before Pandas 2.0, empty series' dtype was float, but as of Pandas 2.0. empty series' dtype became object.
                # See: https://github.com/pandas-dev/pandas/issues/17261
                # EMPTY type column are returned as pandas.Series with "object" dtype to match Pandas 2.0 default.
                # We cast it back to "float" so that it matches Pandas 1.0 default for empty series.
                # Moreover, we explicitly provide the index otherwise Pandas 0.X overrides it for a RangeIndex
                empty_columns_names = (
                    [] if data is None else
                    [
                        name for name, np_array in data.items()
                        if np_array.dtype in OBJECT_TOKENS and len(df[name]) == 0
                    ]
                )
                for column_name in empty_columns_names:
                    df[column_name] = pd.Series([], index=index, dtype='float64')
        else:
            if index is not None:
                df = self.df_without_consolidation(columns, index, item, n_indexes, data)
            else:
                df = self.df_without_consolidation(columns, item.data[0], item, n_indexes, data)

        if denormed_columns is not None:
            df.columns = denormed_columns
        if norm_meta.common.columns.fake_name is False and len(norm_meta.common.columns.name) > 0:
            df.columns.name = norm_meta.common.columns.name
        for key in norm_meta.common.categories:
            if key in data:
                category_info = list(norm_meta.common.categories[key].category)
                codes = data[key]
                if IS_PANDAS_ZERO:
                    # `pd.Categorical.from_codes` from `pandas~=0.25.x` (pandas' supported version for python 3.6)
                    # does not support `codes` of `dtype=object`: it has to have an integral dtype.
                    # See: https://github.com/pandas-dev/pandas/blob/0.25.x/pandas/core/arrays/categorical.py#L688-L704
                    codes = np.asarray(codes, dtype=int)
                df[key] = pd.Categorical.from_codes(codes=codes, categories=category_info)
        for key in norm_meta.common.int_categories:
            if key in data:
                category_info = list(norm_meta.common.int_categories[key].category)
                res = pd.Categorical.from_codes(codes=data[key], categories=category_info)
                df[key] = res

        if idx_type == "index":
            df.index.name = corrected_index_name(index, norm_meta)
        elif idx_type == "multi_index":
            df = self._denormalize_multi_index(df=df, norm_meta=norm_meta)

        return df

    @staticmethod
    def _denormalize_multi_index(df: pd.DataFrame, norm_meta: NormalizationMetadata.PandasDataFrame) -> pd.DataFrame:
        midx = norm_meta.common.multi_index

        # Reconstruct the index level names
        ffp = set(midx.fake_field_pos)

        if 0 in ffp:
            level_0_name = None
        else:
            level_0_name = df.index.name

        index_names = [level_0_name]

        for index_level_num, name in enumerate(df.columns[: midx.field_count], start=1):
            if index_level_num in ffp:
                name = None
            else:
                name = name[_IDX_PREFIX_LEN:]

            index_names.append(name)

        if df.empty:
            # Multi-indexing operations don't behave well for empty dataframes, resulting in loss of
            # level names / dtypes. Prevent this by explicitly creating the empty multi-index.
            levels = [df.index]

            for index_level_num in range(1, midx.field_count + 1):
                index_col_idx = index_level_num - 1
                index_col = df.iloc[:, index_col_idx]

                # Restore the timezone on the series used to construct the index level
                tz = midx.timezone.get(index_level_num, "")
                if tz != "":
                    index_col = index_col.dt.tz_localize(tz)

                if not IS_PANDAS_TWO and index_col.dtype == "float":
                    # In Pandas < 2, empty series dtype is `"float"`, but as of Pandas 2.0, empty series dtype
                    # is `"object"`. We cast it back to "float" so that it matches Pandas 1.0 default for empty series.
                    # See: https://github.com/man-group/ArcticDB/pull/1049
                    # Yet for index columns, we need to cast it to "object" to preserve the index level dtype.
                    index_col = index_col.astype("object")

                levels.append(index_col)
            if pd.__version__.startswith("0"):
                index = pd.MultiIndex(levels=levels, labels=[[]] * len(levels), names=index_names)
            else:
                index = pd.MultiIndex(levels=levels, codes=[[]] * len(levels), names=index_names)
            df = df.iloc[:, midx.field_count :]
            df.index = index
        else:
            df.set_index(list(df.columns[: midx.field_count]), append=True, inplace=True)

            # Restore the timezones in all but the first index which is fixed in _index_from_records.
            for key in midx.timezone:
                tz = midx.timezone[key]
                if tz != "":
                    df = df.tz_localize("UTC", level=key).tz_convert(tz, level=key)

            df.index.names = index_names

        if norm_meta.has_synthetic_columns:
            df.columns = RangeIndex(0, len(df.columns))

        return df

    def normalize(self, item, string_max_len=None, dynamic_strings=False, coerce_columns=None, empty_types=False, **kwargs):
        # type: (DataFrame, Optional[int])->NormalizedInput
        norm_meta = NormalizationMetadata()
        norm_meta.df.common.mark = True
        if isinstance(item.columns, RangeIndex):
            norm_meta.df.has_synthetic_columns = True

        if isinstance(item.index, MultiIndex):
            # need to copy otherwise we are altering input which might surprise too many users
            # TODO provide a better impl of MultiIndex
            item = item.copy()

        if isinstance(item.columns, MultiIndex):
            raise ArcticDbNotYetImplemented("MultiIndex column are not supported yet")

        index_names, ix_vals = self._index_to_records(
            item, norm_meta.df.common, dynamic_strings, string_max_len=string_max_len, empty_types=empty_types
        )
        # The first branch of this if is faster, but does not work with null/duplicated column names
        if item.columns.is_unique and not item.columns.hasnans:
            columns_vals = [item[col].values for col in item.columns]
        else:
            columns_vals = [item.iloc[:, idx].values for idx in range(len(item.columns))]
        columns, column_vals = _normalize_columns(
            item.columns,
            columns_vals,
            norm_meta.df,
            coerce_columns=coerce_columns,
            dynamic_strings=dynamic_strings,
            string_max_len=string_max_len,
            dynamic_schema=kwargs.get("dynamic_schema", False),
            index_names=index_names,
        )
        if item.columns.name is not None:
            norm_meta.df.common.columns.name = item.columns.name
        else:
            norm_meta.df.common.columns.fake_name = True

        sort_status = _SortedValue.UNKNOWN
        index = item.index
        # Treat empty indexes as ascending so that all operations are valid
        if index.empty:
            sort_status = _SortedValue.ASCENDING
        elif isinstance(index, (pd.DatetimeIndex, pd.PeriodIndex)):
            if index.is_monotonic_increasing:
                sort_status = _SortedValue.ASCENDING
            elif index.is_monotonic_decreasing:
                sort_status = _SortedValue.DESCENDING
            else:
                sort_status = _SortedValue.UNSORTED

        return NormalizedInput(
            item=NPDDataFrame(
                index_names=index_names,
                index_values=ix_vals,
                column_names=columns,
                columns_values=column_vals,
                sorted=sort_status,
            ),
            metadata=norm_meta,
        )


class MsgPackNormalizer(Normalizer):
    """
    Fall back plan for the time being to store arbitrary data
    """
    def __init__(self, cfg=None):
        self.strict_mode = cfg.strict_mode if cfg is not None else False

    def normalize(self, obj, **kwargs):
        packed, nbytes = self._msgpack_padded_packb(obj)

        norm_meta = NormalizationMetadata()
        norm_meta.msg_pack_frame.version = 1
        norm_meta.msg_pack_frame.size_bytes = nbytes

        # FUTURE(#263): do we need to care about byte ordering?
        column_val = np.array(memoryview(packed), np.uint8).view(np.uint64)

        return NormalizedInput(
            item=NPDDataFrame(
                index_names=[],
                index_values=[],
                column_names=["bytes"],
                columns_values=[column_val],
                sorted=_SortedValue.UNKNOWN,
            ),
            metadata=norm_meta,
        )

    def denormalize(self, obj, meta):
        input_type = meta.WhichOneof("input_type")
        if input_type != "msg_pack_frame":
            raise ArcticNativeException("Expected msg_pack_frame input, actual {}".format(meta))
        sb = meta.msg_pack_frame.size_bytes
        col_data = obj.data[0].view(np.uint8)[:sb]
        return self._msgpack_unpackb(memoryview(col_data))

    def _custom_pack(self, obj):
        if isinstance(obj, pd.Timestamp):
            tz = obj.tz.zone if obj.tz is not None else None
            return ExtType(MsgPackSerialization.PD_TIMESTAMP, packb([obj.value, tz]))

        if isinstance(obj, datetime.datetime):
            return ExtType(MsgPackSerialization.PY_DATETIME, packb(_to_tz_timestamp(obj)))

        if isinstance(obj, datetime.timedelta):
            return ExtType(MsgPackSerialization.PY_TIMEDELTA, packb(pd.Timedelta(obj).value))

        if self.strict_mode:
            raise TypeError("Normalisation is running in strict mode, writing pickled data is disabled.")
        else:
            return ExtType(MsgPackSerialization.PY_PICKLE_3, packb(Pickler.write(obj)))

    def _ext_hook(self, code, data):
        if code == MsgPackSerialization.PD_TIMESTAMP:
            data = unpackb(data, raw=False)
            return pd.Timestamp(data[0], tz=data[1]) if data[1] is not None else pd.Timestamp(data[0])

        if code == MsgPackSerialization.PY_DATETIME:
            data = unpackb(data, raw=False)
            return _from_tz_timestamp(data[0], data[1])

        if code == MsgPackSerialization.PY_TIMEDELTA:
            data = unpackb(data, raw=False)
            return pd.Timedelta(data).to_pytimedelta()

        if code == MsgPackSerialization.PY_PICKLE_2:
            # If stored in Python2 we want to use raw while unpacking.
            # https://github.com/msgpack/msgpack-python/blob/master/msgpack/_unpacker.pyx#L230
            data = unpackb(data, raw=True)
            return Pickler.read(data, pickled_in_python2=True)

        if code == MsgPackSerialization.PY_PICKLE_3:
            data = unpackb(data, raw=False)
            return Pickler.read(data, pickled_in_python2=False)

        return ExtType(code, data)

    def _msgpack_packb(self, obj):
        return packb(obj, default=self._custom_pack)

    def _msgpack_padded_packb(self, obj):
        return padded_packb(obj, default=self._custom_pack)

    def _msgpack_unpackb(self, buff, raw=False):
        return unpackb(buff, raw=raw, ext_hook=self._ext_hook)


class Pickler(object):
    @staticmethod
    def read(data, pickled_in_python2=False):
        if isinstance(data, str):
            return pickle.loads(data.encode("ascii"), encoding="bytes")
        elif isinstance(data, str):
            if not pickled_in_python2:
                # Use the default encoding for python2 pickled objects similar to what's being done for PY2.
                return pickle.loads(data, encoding="bytes")

        try:
            return pickle.loads(data)
        except UnicodeDecodeError as exc:
            log.debug("Failed decoding with ascii, using latin-1.")
            return pickle.loads(data, encoding="latin-1")

    @staticmethod
    def write(obj):
        return pickle.dumps(obj, protocol=PICKLE_PROTOCOL)


class TimeFrameNormalizer(Normalizer):
    def normalize(self, item, string_max_len=None, dynamic_strings=False, coerce_columns=None, empty_types=False, **kwargs):
        norm_meta = NormalizationMetadata()
        norm_meta.ts.mark = True
        index_norm = norm_meta.ts.common.index
        index_norm.is_physically_stored = len(item.times) > 0 and not isinstance(item.times, RangeIndex)
        index_names, ix_vals = _normalize_single_index(
            item.times, ["times"], index_norm, dynamic_strings, string_max_len, empty_types=empty_types
        )
        columns_names, columns_vals = _normalize_columns(
            item.columns_names,
            item.columns_values,
            norm_meta.ts,
            coerce_columns=coerce_columns,
            dynamic_strings=dynamic_strings,
            string_max_len=string_max_len,
            index_names=index_names,
            dynamic_schema=kwargs.get("dynamic_schema", False),
        )

        return NormalizedInput(
            item=NPDDataFrame(
                index_names=index_names,
                index_values=ix_vals,
                column_names=columns_names,
                columns_values=columns_vals,
                sorted=_SortedValue.ASCENDING if item.issorted else _SortedValue.UNSORTED,
            ),
            metadata=norm_meta,
        )

    def denormalize(self, item, norm_meta):
        idx = _denormalize_single_index(item, norm_meta.common)
        columns, denormed_columns, data = _denormalize_columns(item, norm_meta, "index", 1)
        if columns is None:
            columns = []
            denormed_columns = []
            data = {}
        return TimeFrame(
            times=idx.values,
            columns_names=denormed_columns if denormed_columns is not None else columns,
            columns_values=[data[col_name] for col_name in columns],
        )


class KnownTypeFallbackOnError(Normalizer):
    def __init__(self, delegate, nfh):
        # type: (Normalizer, Normalizer)->None
        self._delegate = delegate
        self._failure_handler = nfh

    def normalize(self, item, **kwargs):
        try:
            return self._delegate.normalize(item, **kwargs)
        except:
            log.error("First class type({}) normalization failed, falling back to generic serialization.", type(item))
            log.debug("item {}:", item)
            return self._failure_handler.normalize(item, **kwargs)

    def denormalize(self, item, norm_meta):
        return self._delegate.denormalize(item, norm_meta)


class CompositeNormalizer(Normalizer):
    def __init__(self, fallback_normalizer=None, use_norm_failure_handler_known_types=False):
        self.df = DataFrameNormalizer()
        self.series = SeriesNormalizer()
        self.tf = TimeFrameNormalizer()
        self.np = NdArrayNormalizer()

        if use_norm_failure_handler_known_types and fallback_normalizer is not None:
            self.df = KnownTypeFallbackOnError(self.df, fallback_normalizer)
            self.series = KnownTypeFallbackOnError(self.series, fallback_normalizer)
            self.tf = KnownTypeFallbackOnError(self.tf, fallback_normalizer)
            self.np = KnownTypeFallbackOnError(self.np, fallback_normalizer)

        self.msg_pack_denorm = MsgPackNormalizer()  # must exist for deserialization
        self.fallback_normalizer = fallback_normalizer

    def _normalize(self, item, string_max_len=None, dynamic_strings=False, coerce_columns=None, empty_types=False, **kwargs):
        normalizer = self.get_normalizer_for_type(item)

        if not normalizer:
            return item, None

        log.debug("Normalizer used: {}".format(normalizer))
        return normalizer(
            item,
            string_max_len=string_max_len,
            dynamic_strings=dynamic_strings,
            coerce_columns=coerce_columns,
            empty_types=empty_types,
            **kwargs,
        )

    def get_normalizer_for_type(self, item):
        # TODO: this should use customcompositenormalizer as well.
        if isinstance(item, DataFrame):
            if (
                item.empty
                and not isinstance(self.df, KnownTypeFallbackOnError)
                and self.fallback_normalizer is not None
            ):
                return KnownTypeFallbackOnError(self.df, self.fallback_normalizer).normalize
            return self.df.normalize

        if isinstance(item, Series):
            if (
                item.empty
                and not isinstance(self.series, KnownTypeFallbackOnError)
                and self.fallback_normalizer is not None
            ):
                return KnownTypeFallbackOnError(self.series, self.fallback_normalizer).normalize

            return self.series.normalize

        if isinstance(item, TimeFrame):
            return self.tf.normalize

        if isinstance(item, np.ndarray):
            return self.np.normalize

        if self.fallback_normalizer is not None:
            # Msgpack normalize if everything else fails.
            return self.fallback_normalizer.normalize

        return None

    def normalize(
        self, item, string_max_len=None, pickle_on_failure=False, dynamic_strings=False, coerce_columns=None, empty_types=False, **kwargs
    ):
        """
        :param item: Item to be normalized to something Arctic Native understands.
        :param string_max_len: This is used for dataframe with string columns as we convert the column to a fixed
        width string column which relies on the largest string in the object.
        :param pickle_on_failure: This will fallback to pickling the Supported objects (DataFrame, Series, TimeFrame)
         even if use_norm_failure_handler_known_types was not configured at the library level.
        """
        try:
            return self._normalize(
                item,
                string_max_len=string_max_len,
                dynamic_strings=dynamic_strings,
                coerce_columns=coerce_columns,
                empty_types=empty_types,
                **kwargs,
            )
        except Exception as ex:
            log.debug("Could not normalize item of type: {} with the default normalizer due to {}", type(item), ex)
            if pickle_on_failure:
                log.debug("pickle_on_failure flag set, normalizing the item with MsgPackNormalizer", type(item), ex)
                return self.fallback_normalizer.normalize(item)
            else:
                raise

    def denormalize(self, item, norm_meta):
        # type: (_FrameData, NormalizationMetadata)->_SUPPORTED_TYPES
        if isinstance(item, FrameData):
            input_type = norm_meta.WhichOneof("input_type")
            if input_type == "df":
                return self.df.denormalize(item, norm_meta.df)
            elif input_type == "series":
                return self.series.denormalize(item, norm_meta.series)
            elif input_type == "ts":
                return self.tf.denormalize(item, norm_meta.ts)
            elif input_type == "np":
                return self.np.denormalize(item, norm_meta.np)
            elif input_type == "msg_pack":
                return self.msg_pack_denorm.denormalize(item, norm_meta)

        if self.fallback_normalizer is None:
            raise ArcticNativeException("Cannot denormalize item with metadata {}".format(norm_meta))
        return self.fallback_normalizer.denormalize(item, norm_meta)


_NORMALIZER = CompositeNormalizer()

normalize = _NORMALIZER.normalize
denormalize = _NORMALIZER.denormalize

_MAX_USER_DEFINED_META = 16 << 20 # 16MB
_WARN_USER_DEFINED_META = 8 << 20 # 8MB

_MAX_RECURSIVE_METASTRUCT = 16 << 20 # 16MB
_WARN_RECURSIVE_METASTRUCT = 8 << 20 # 8MB


def _init_msgpack_metadata():
    cfg = VersionStoreConfig.MsgPack()
    cfg.max_blob_size = _MAX_USER_DEFINED_META
    return MsgPackNormalizer(cfg)


_msgpack_metadata = _init_msgpack_metadata()


def normalize_metadata(metadata: Any) -> UserDefinedMetadata:
    if metadata is None:
        return None
    # Prevent arbitrary large object serialization
    # as it will slow down the indexing read side
    # which is not a good idea.
    # A subsequent improvement could remove that limitation
    # using an extra indirection and point to the blob key
    # However, this is also a probable sign of poor data modelling
    # and understanding the need should be a priority before
    # removing this protection.
    packed = _msgpack_metadata._msgpack_packb(metadata)
    size = len(packed)
    if size > _MAX_USER_DEFINED_META:
        raise ArcticDbNotYetImplemented(f'User defined metadata cannot exceed {_MAX_USER_DEFINED_META}B')
    if size > _WARN_USER_DEFINED_META:
        log.warn(f'User defined metadata is above warning size ({_WARN_USER_DEFINED_META}B), metadata cannot exceed {_MAX_USER_DEFINED_META}B.  Current size: {size}B.')

    udm = UserDefinedMetadata()
    udm.inline_payload = packed
    return udm


def normalize_recursive_metastruct(metastruct: Dict[Any, Any]) -> UserDefinedMetadata:
    # Prevent arbitrary large object serialization, as it is indicative of a poor data layout
    packed = _msgpack_metadata._msgpack_packb(metastruct)
    size = len(packed)
    if size > _MAX_RECURSIVE_METASTRUCT:
        raise ArcticDbNotYetImplemented(f'Recursively normalized data normalization metadata cannot exceed {_MAX_RECURSIVE_METASTRUCT}B')
    if size > _WARN_RECURSIVE_METASTRUCT:
        log.warn(f'Recursively normalized data normalization metadata is above warning size ({_WARN_RECURSIVE_METASTRUCT}B), cannot exceed {_MAX_RECURSIVE_METASTRUCT}B. Current size: {size}B.')

    udm = UserDefinedMetadata()
    udm.inline_payload = packed
    return udm


def denormalize_user_metadata(udm, ext_obj=None):
    # type: (NormalizationMetadata.UserDefinedMetadata, Optional[buffer])->Mapping[string,Any]
    storage_type = udm.WhichOneof("storage_type")
    if storage_type == "inline_payload":
        return _msgpack_metadata._msgpack_unpackb(udm.inline_payload)
    elif storage_type is None:
        return None
    else:
        raise ArcticDbNotYetImplemented("Extra object reference is not supported yet")


def denormalize_dataframe(ret):
    read_result = ReadResult(*ret)
    frame_data = FrameData.from_cpp(read_result.frame_data)
    if len(frame_data.names) == 0:
        return None

    return DataFrameNormalizer().denormalize(frame_data, read_result.norm.df)

def normalize_dataframe(df, **kwargs):
    return DataFrameNormalizer().normalize(df, **kwargs)


T = TypeVar("T", bound=Union[pd.DataFrame, pd.Series])


def restrict_data_to_date_range_only(data: T, *, start: Timestamp, end: Timestamp) -> T:
    """Return a copy of `data` filtered so that its contents lie between `start` and `end` (inclusive).

    `data` must be time-indexed.
    """

    def _strip_tz(s, e):
        return s.tz_localize(None), e.tz_localize(None)

    if hasattr(data, "loc"):
        if not data.index.get_level_values(0).tz:
            start, end = _strip_tz(start, end)
        if not data.index.is_monotonic_increasing:
            # data.loc[...] scans forward through the index until hitting a value >= pd.to_datetime(end)
            # If the input data is unsorted this produces non-intuitive results
            # The copy below in data.loc[...] will recalculate is_monotonic_<in|de>creasing
            # Therefore if data.loc[...] is sorted, but data is not the update will be allowed with unexpected results
            # See https://github.com/man-group/ArcticDB/issues/1173 for more details
            # We could set data.is_monotonic_<in|de>creasing to the values on input to this function after calling
            # data.loc[...] and let version_core.cpp::sorted_data_check_update handle this, but that will be confusing
            # as the frame input to sorted_data_check_update WILL be sorted. Instead, we fail early here, at the cost
            # of duplicating exception messages.
            raise SortingException("E_UNSORTED_DATA When calling update, the input data must be sorted.")
        data = data.loc[pd.to_datetime(start) : pd.to_datetime(end)]
    else:  # non-Pandas, try to slice it anyway
        if not getattr(data, "timezone", None):
            start, end = _strip_tz(start, end)
        data = data[
            start.to_pydatetime(warn=False)
            - timedelta(microseconds=1) : end.to_pydatetime(warn=False)
            + timedelta(microseconds=1)
        ]
    return data


def normalize_dt_range_to_ts(dtr: DateRangeInput) -> Tuple[Timestamp, Timestamp]:
    def _to_utc_ts(v: "ExplicitlySupportedDates", bound_name: str) -> Timestamp:
        if not isinstance(v, supported_time_types):
            raise TypeError(
                "DateRange bounds must be datetime, date or Timestamps, DateRange.{}={}({})".format(
                    bound_name, type(v), v
                )
            )

        v = Timestamp(v)

        if v.tzinfo is None:
            log.debug(
                f"DateRange bounds do not have timestamps, will default to UTC for the query,DateRange.{bound_name}={v}"
            )
            v = v.tz_localize("UTC")

        return v

    if getattr(dtr, "startopen", False) or getattr(dtr, "endopen", False):
        raise ValueError("Only supports closed/closed date range. Actual:{}".format(dtr))

    def _get_name_or_pos(name, pos):
        if hasattr(dtr, name):
            return getattr(dtr, name)
        return dtr[pos]

    start_val = _get_name_or_pos("start", 0)
    end_val = _get_name_or_pos("end", -1)
    s = _to_utc_ts(start_val, "start") if start_val else Timestamp.min.tz_localize("UTC")
    e = _to_utc_ts(end_val, "end") if end_val else Timestamp.max.tz_localize("UTC")
    return s, e
