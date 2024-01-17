"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import collections
from datetime import datetime

import numpy as np
from numpy import datetime64
from pandas import Timestamp
from typing import NamedTuple, List, AnyStr


def _stringify(v):
    return str(v, "utf-8")


# NOTE: When using Pandas < 2.0, `datetime64` _always_ has the nanosecond resolution,
# i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
# Yet, this has changed in Pandas 2.0 and other resolution can be used,
# i.e. Pandas 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
# See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution  # noqa: E501
# TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do not
# rely uniquely on the resolution-less 'M' specifier if it this doable.
_NS_DTYPE = np.dtype("datetime64[ns]")


class TimeFrame(
    NamedTuple(
        "TimeFrame", [("times", np.ndarray), ("columns_names", List[AnyStr]), ("columns_values", List[np.ndarray])]
    )
):
    def __new__(cls, times, columns_names, columns_values):
        if not isinstance(times, np.ndarray) or times.dtype != _NS_DTYPE:
            raise TypeError("times must be a np.ndarray, actual {}({})".format(type(times), times))

        if len(columns_names) != len(columns_values):
            raise ValueError(
                "Columns names and columns values must have the same size, actual {} != {}".format(
                    len(columns_names), len(columns_values)
                )
            )
        if not all(times.shape[0] == cv.shape[0] for cv in columns_values):
            s = np.array([cv.shape[0] for cv in columns_values])
            error_message = (
                "Inconsistent size of column values. times.shape[0]={} must match cv.shape[0] for all column values."
                " actual={}"
            ).format(times.shape[0], s)
            raise ValueError(error_message)
        return tuple.__new__(cls, (times, columns_names, columns_values))

    class _IlocProxy(object):
        def __init__(self, tf):
            self._tf = tf

        def __getitem__(self, item):
            return self._tf._iloc(item)

    @property
    def iloc(self):
        return TimeFrame._IlocProxy(self)

    def _iloc(self, item, resolve_slice=None):
        if isinstance(item, tuple):
            if len(item) != 2:
                raise ValueError(
                    "Only support 2 dimensional indexing where dimension 0 is the row indexing and dimension 1 is"
                    " column one"
                )
            col_filter = item[1]
            item = item[0]
            if isinstance(col_filter, slice):
                dst = TimeFrame(self.times, self.columns_names[col_filter], self.columns_values[col_filter])
            elif isinstance(col_filter, int):
                dst = TimeFrame(self.times, [self.columns_names[col_filter]], [self.columns_values[col_filter]])
            elif isinstance(col_filter, str):
                idx = self.columns_names.index(col_filter)
                if idx == -1:
                    raise KeyError("Cannot find column {} in {}".format(col_filter, self.columns_names))
                dst = TimeFrame(self.times, [self.columns_names[idx]], [self.columns_values[idx]])
            else:
                raise TypeError(
                    "Column filtering only support slice and integer, actual {}({})".format(
                        type(col_filter), col_filter
                    )
                )
        else:
            dst = self
        if not isinstance(item, (slice, int)):
            raise TypeError("Only support indexing by slice or int")
        if isinstance(dst, int):
            return TimeFrame(
                dst.times[item : item + 1], dst.columns_names, [cv[item : item + 1] for cv in dst.columns_values]
            )

        if resolve_slice is not None:
            item = resolve_slice(item)
        return TimeFrame(dst.times[item], dst.columns_names, [cv[item] for cv in dst.columns_values])

    class _TsLocProxy(object):
        def __init__(self, tf):
            self._tf = tf

        def _resolve_slice(self, item):
            def _normalize(v):
                if isinstance(v, str) or isinstance(v, datetime):
                    return datetime64(Timestamp(v).value, "ns")
                return datetime64(item, "ns")

            s = np.searchsorted(self._tf.times, _normalize(item.start), side="left") if item.start is not None else None
            e = np.searchsorted(self._tf.times, _normalize(item.stop), side="right") if item.stop is not None else None
            return slice(s, e, item.step)

        def __getitem__(self, item):
            return self._tf._iloc(item, self._resolve_slice)

    @property
    def tsloc(self):
        return TimeFrame._TsLocProxy(self)

    @property
    def issorted(self):
        return np.all(self.times[:-1] <= self.times[1:])

    def __eq__(self, other):
        if other is None:
            return False
        if id(other) == id(self):
            return True
        if self.times.shape != other.times.shape:
            return False
        if len(self.columns_values) != len(other.columns_values):
            return False
        return (
            np.array_equal(self.times, other.times)
            and self.columns_names == other.columns_names
            and all(np.array_equal(l, r) for l, r in zip(self.columns_values, other.columns_values))
        )


def _column_name_to_strings(name):
    if isinstance(name, str):
        return name
    elif isinstance(name, bytes):
        # XXX: should we assume that bytes in Python 3 are UTF-8?
        return name.decode("utf8")
    elif isinstance(name, tuple):
        return str(tuple(map(_column_name_to_strings, name)))
    elif isinstance(name, collections.abc.Sequence):
        raise TypeError("Unsupported type for MultiIndex level")
    elif name is None:
        return None
    return _stringify(name)


def _index_level_name(index, i, column_names):
    if index.name is not None and index.name not in column_names:
        return index.name
    else:
        return "__index_level_{:d}__".format(i)


def _get_columns_to_convert(df, preserve_index=True):
    columns = df.columns

    if not columns.is_unique:
        raise ValueError("Duplicate column names found: {}".format(list(df.columns)))

    column_names = []
    index_columns = []
    index_column_names = []
    data_type = None

    if preserve_index:
        n = len(getattr(df.index, "levels", [df.index]))
        for i in range(n):
            index_column = df.index.get_level_values(i)
            index_columns.extend([index_column._data])
            index_column_names.append(index_column.name)

    columns_to_convert = []
    convert_types = []

    for name in columns:
        col = df[name]
        name = _column_name_to_strings(name)

        # columns_to_convert.append(col.values)
        columns_to_convert.append(col)
        convert_types.append(data_type)
        column_names.append(name)

    names = index_column_names + column_names

    return names, index_column_names, column_names, index_columns, columns_to_convert, convert_types
