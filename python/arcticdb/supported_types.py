"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import datetime
from typing import Sequence, Union, TYPE_CHECKING

import numpy as np
import pandas as pd
from arcticdb_ext.types import DataType, TypeDescriptor, FieldDescriptor

sint_types = [np.int8, np.int16, np.int32, np.int64]
uint_types = [np.uint8, np.uint16, np.uint32, np.uint64]
int_types = sint_types + uint_types
float_types = [np.float32, np.float64]
numeric_types = int_types + float_types
time_types = (pd.Timestamp, datetime.datetime, datetime.date)
Timestamp = Union[time_types]

_ExtDateRangeTypes = pd.core.indexes.datetimelike.DatetimeIndexOpsMixin
if TYPE_CHECKING:
    try:
        import arctic.date

        _ExtDateRangeTypes = Union[_ExtDateRangeTypes, arctic.date.DateRange]
    except ModuleNotFoundError:
        pass

ExplicitlySupportedDates = Union[time_types]
DateRangeInput = Union[Sequence[ExplicitlySupportedDates], _ExtDateRangeTypes]

Shape = np.uint64

_np_by_dt = {
    DataType.UINT8: np.uint8,
    DataType.UINT16: np.uint16,
    DataType.UINT32: np.uint32,
    DataType.UINT64: np.uint64,
    DataType.INT8: np.int8,
    DataType.INT16: np.int16,
    DataType.INT32: np.int32,
    DataType.INT64: np.int64,
    DataType.FLOAT32: np.float32,
    DataType.FLOAT64: np.float64,
}

_dt_by_np = {v: k for k, v in _np_by_dt.items()}


def get_numpy_dtype(dt):
    # type: (DataType)->np.dtype
    return _np_by_dt[dt]


def get_data_type(d):
    # type: (np.dtype)->DataType
    return _dt_by_np[d]


def field_desc(data_type, dimension, name=None):
    td = TypeDescriptor(data_type, dimension)
    return FieldDescriptor(td) if name is None else FieldDescriptor(td, name)
