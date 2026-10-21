"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import List

import numpy as np
import pandas as pd

from arcticdb.dependencies import _PYARROW_AVAILABLE, pyarrow as pa
from arcticdb.util._versions import IS_AT_LEAST_PANDAS_TWO_THREE, IS_AT_LEAST_PYARROW_TEN_ZERO_ONE
from pandas._libs.arrays import NDArrayBacked
from arcticdb_ext.version_store import RecordBatchData

_ARROW_BACKED_STR_DTYPE_SUPPORTED = (
    _PYARROW_AVAILABLE and IS_AT_LEAST_PANDAS_TWO_THREE and IS_AT_LEAST_PYARROW_TEN_ZERO_ONE
)


def _use_pyarrow_strings_in_pandas():
    # get_option raises on pandas < 2.1 where future.infer_string does not exist, which the gate above excludes.
    return _ARROW_BACKED_STR_DTYPE_SUPPORTED and bool(pd.get_option("future.infer_string"))


def _use_python_strings_in_pandas():
    return (
        IS_AT_LEAST_PANDAS_TWO_THREE
        and not _use_pyarrow_strings_in_pandas()
        and bool(pd.get_option("future.infer_string"))
    )


def _object_strings_to_python_str_array(value):
    dtype = pd.StringDtype(storage="python", na_value=np.nan)
    if value.dtype != np.dtype("object"):
        return pd.array(value, dtype=dtype)
    array_type = dtype.construct_array_type()
    result = array_type.__new__(array_type)
    NDArrayBacked.__init__(result, value, dtype)
    return result


def _is_arrow_column(value):
    return isinstance(value, list) and (len(value) == 0 or isinstance(value[0], RecordBatchData))


def _arrow_string_arrays_to_pd_array(arrays):
    dtype = pd.StringDtype(storage="pyarrow", na_value=np.nan)
    if not arrays:
        return pd.array([], dtype=dtype)
    imported = [pa.RecordBatch._import_from_c(a.array(), a.schema()).column(0) for a in arrays]
    return pd.array(pa.chunked_array(imported), dtype=dtype)


def _adapt_string_column(column):
    if _is_arrow_column(column):
        return _arrow_string_arrays_to_pd_array(column)
    if (
        isinstance(column, np.ndarray)
        # Object dtype or fixed-width numpy strings
        and (column.dtype == np.dtype("object") or column.dtype.kind == "U")
        and _use_python_strings_in_pandas()
    ):
        return _object_strings_to_python_str_array(column)
    return column


def _pandas_str_column_to_record_batches(chunked, arr_name) -> List[RecordBatchData]:
    # chunked: pa.ChunkedArray backing a pyarrow-backed pandas string column (arr._pa_array)
    batches = []
    for chunk in chunked.chunks:  # each chunk is a pa.Array (large_string)
        record_batch = pa.RecordBatch.from_arrays([chunk], names=[str(arr_name)])
        rbd = RecordBatchData()
        record_batch._export_to_c(rbd.array(), rbd.schema())
        batches.append(rbd)
    return batches
