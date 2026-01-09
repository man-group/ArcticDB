"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import List, Optional, Tuple, Union

import pandas as pd

from arcticdb.exceptions import NoSuchVersionException
from arcticdb.version_store.library import Library
from arcticdb.version_store._store import NativeVersionStore


class SymbolInfo:
    def __init__(self, symbol: str, append_df: pd.DataFrame, lib: NativeVersionStore):
        self.symbol = symbol
        self.append_df = append_df
        try:
            self.index = lib.read_index(self.symbol)
            self.exists = True
        except NoSuchVersionException:
            self.exists = False


def _generate_levels(target_rows_per_slice: int, factor: int) -> List[int]:
    levels = []
    while target_rows_per_slice > 1:
        levels.append(target_rows_per_slice)
        target_rows_per_slice //= factor
    return levels


def _generate_row_to_read_from(
    lib: NativeVersionStore, symbol: str, df: pd.DataFrame, levels: List[int]
) -> Optional[int]:
    try:
        lib.read_index(symbol)
    except NoSuchVersionException:
        # This symbol is new, so nothing to read
        return None


def _update_and_defrag(lib: Union[Library, NativeVersionStore], items: List[Tuple[str, pd.DataFrame]], factor: int):
    if isinstance(lib, Library):
        lib = lib._nvs
    target_rows_per_slice = lib.lib_cfg().lib_desc.version.write_options.segment_row_size
    if target_rows_per_slice == 0:
        target_rows_per_slice = 100_000
    levels = _generate_levels(target_rows_per_slice, factor)
    symbol_infos = [SymbolInfo(item[0], item[1]) for item in items]
