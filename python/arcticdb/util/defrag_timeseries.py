"""
Copyright 2025 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import Union

from arcticdb.version_store.library import Library
from arcticdb.version_store._store import NativeVersionStore


def _defrag_timeseries(lib: Union[Library, NativeVersionStore], sym: str):
    if isinstance(lib, Library):
        lib = lib._nvs
    target_rows_per_slice = lib.lib_cfg().lib_desc.version.write_options.segment_row_size
    if target_rows_per_slice == 0:
        target_rows_per_slice = 100_000
    index = lib.read_index(sym)
    row_counts = index["end_row"] - index["start_row"]
    idx = 0
    for row_count in row_counts:
        if row_count == target_rows_per_slice:
            idx += 1
        else:
            break
    if idx < len(index) and index["start_row"][idx] != index["start_row"][-1]:
        df = lib.read(sym, row_range=(index["start_row"][idx], index["end_row"][-1])).data
        lib.update(sym, df)
