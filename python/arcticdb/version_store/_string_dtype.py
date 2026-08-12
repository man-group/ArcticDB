"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import List

import numpy as np
import pandas as pd

from arcticdb.dependencies import _PYARROW_AVAILABLE, pyarrow as pa
from arcticdb.util._versions import IS_PANDAS_TWO_THREE
from arcticdb_ext.version_store import RecordBatchData

_ARROW_BACKED_STR_DTYPE_SUPPORTED = _PYARROW_AVAILABLE and IS_PANDAS_TWO_THREE


def _use_pyarrow_strings_in_pandas():
    # get_option raises on pandas < 2.1 where future.infer_string does not exist, which the gate above excludes.
    return _ARROW_BACKED_STR_DTYPE_SUPPORTED and bool(pd.get_option("future.infer_string"))


def _is_arrow_string_column(value):
    return isinstance(value, list) and (len(value) == 0 or isinstance(value[0], RecordBatchData))


def _arrow_string_arrays_to_pd_array(arrays):
    dtype = pd.StringDtype(storage="pyarrow", na_value=np.nan)
    if not arrays:
        return pd.array([], dtype=dtype)
    imported = [pa.RecordBatch._import_from_c(a.array(), a.schema()).column(0) for a in arrays]
    return pd.array(pa.chunked_array(imported), dtype=dtype)


def _adopt_arrow_strings(column):
    return _arrow_string_arrays_to_pd_array(column) if _is_arrow_string_column(column) else column


def _pandas_str_column_to_record_batches(chunked, arr_name) -> List[RecordBatchData]:
    # chunked: pa.ChunkedArray backing a pyarrow-backed pandas string column (arr._pa_array)
    batches = []
    for chunk in chunked.chunks:  # each chunk is a pa.Array (large_string)
        record_batch = pa.RecordBatch.from_arrays([chunk], names=[str(arr_name)])
        rbd = RecordBatchData()
        record_batch._export_to_c(rbd.array(), rbd.schema())
        batches.append(rbd)
    return batches


__all__ = [
    "_ARROW_BACKED_STR_DTYPE_SUPPORTED",
    "_use_pyarrow_strings_in_pandas",
    "_is_arrow_string_column",
    "_arrow_string_arrays_to_pd_array",
    "_adopt_arrow_strings",
    "_pandas_str_column_to_record_batches",
]
