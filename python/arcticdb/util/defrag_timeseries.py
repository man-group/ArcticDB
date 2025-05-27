"""
Copyright 2025 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import Union

from arcticdb.version_store.library import Library
from arcticdb.version_store._store import NativeVersionStore


# This is incredibly basic, but should work well on append-only libraries
# Limitations:
# - Will throw an exception during the call to update if the symbol is not a timeseries
# - Requires all the fragmented data to fit into memory at once
# - Assumes that if the data is fragmented at a given timestamp, that all subsequent data is also fragmented
# - Has no tolerance, so a row-slice with 99,999 rows will be considered fragmented and compacted into a 100k row
#   segment
# - Cannot specify the number of rows per slice, just uses the library default
# - Has no concept of changing the column slicing policy
# - Creates a new version
# - Can inefficiently traverse the entire index key when data is column sliced but not fragmented
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
