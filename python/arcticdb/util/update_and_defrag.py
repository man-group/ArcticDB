"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import List, Optional, Tuple

import pandas as pd

from arcticdb import ReadRequest, UpdatePayload, VersionedItem
from arcticdb.exceptions import ArcticNativeException, NoSuchVersionException
from arcticdb.preconditions import check
from arcticdb.version_store.library import Library
from arcticdb.version_store._store import NativeVersionStore
from arcticdb_ext.storage import NoDataFoundException


def _generate_levels(target_rows_per_slice: int, factor: int) -> List[int]:
    levels = []
    while target_rows_per_slice > 1:
        levels.append(target_rows_per_slice)
        target_rows_per_slice //= factor
    return levels


def _generate_date_to_read_from(
    start_indexes: List[pd.Timestamp],
    start_rows: List[int],
    end_rows: List[int],
    new_df_row_count: int,
    levels: List[int],
    threshold: float,
) -> Optional[pd.Timestamp]:
    check(len(start_rows) == len(end_rows), f"len(start_rows) != len(end_rows): {len(start_rows)} != {len(end_rows)}")
    check(
        len(start_rows) == len(start_indexes),
        f"len(start_rows) != len(start_indexes): {len(start_rows)} != {len(start_indexes)}",
    )
    if not len(levels) or not len(start_rows):
        return None
    row_counts = list((item[1] - item[0] for item in zip(start_rows, end_rows)))
    for idx, row_count in enumerate(row_counts):
        if row_count < threshold * levels[0]:
            # end_rows[-1] - start_rows[idx] == sum(row_counts[idx:])
            if (end_rows[-1] - start_rows[idx]) + new_df_row_count >= levels[0]:
                return start_indexes[idx]
            else:
                return _generate_date_to_read_from(
                    start_indexes[idx:], start_rows[idx:], end_rows[idx:], new_df_row_count, levels[1:], threshold
                )


def _update_and_defrag(
    lib: Library,
    items: List[Tuple[str, pd.DataFrame]],
    factor: int,
    threshold: float = 0.9,
):
    class SymbolInfo:
        def __init__(self, symbol: str, append_df: pd.DataFrame, lib: Library):
            self.append_df = append_df
            self.existing_data_tail = None
            self.start_indexes = []
            self.start_rows = []
            self.end_rows = []
            try:
                index = lib._nvs.read_index(symbol)
                # To ensure idempotency, we must not append the same data twice
                start_index_of_new_df = append_df.index[0]
                end_index_of_existing_df = index["end_index"][-1] - pd.Timedelta(1, unit="ns")
                if start_index_of_new_df <= end_index_of_existing_df:
                    self.append_df = None
                else:
                    # Lazy way to get unique values when there is column slicing (although main use case has dynamic schema enabled anyway)
                    # Index keys will not be very long so not worried about efficiency here
                    self.start_indexes = index.index.sort_values().drop_duplicates().to_list()
                    self.start_rows = index["start_row"].sort_values().drop_duplicates().to_list()
                    self.end_rows = index["end_row"].sort_values().drop_duplicates().to_list()
            except NoDataFoundException:
                # Symbol does not exist
                pass

        def rows_to_append(self):
            return len(self.append_df) if self.append_df is not None else 0

        def update_df(self) -> Optional[pd.DataFrame]:
            if self.append_df is not None and self.existing_data_tail is not None:
                res = pd.concat([self.existing_data_tail, self.append_df])
                check(res.index.is_monotonic_increasing, "Expected monotonically increasing index")
                del self.append_df
                del self.existing_data_tail
                return res
            elif self.append_df is not None:
                return self.append_df
            elif self.existing_data_tail is not None:
                raise ArcticNativeException("Operation already applied so tail should not have been read")
            else:
                return None

    check(0 < threshold <= 1, f"threshold must be in (0, 1], received {threshold}")
    target_rows_per_slice = lib._nvs.lib_cfg().lib_desc.version.write_options.segment_row_size
    if target_rows_per_slice == 0:
        target_rows_per_slice = 100_000
    levels = _generate_levels(target_rows_per_slice, factor)
    symbol_infos = {item[0]: SymbolInfo(item[0], item[1], lib) for item in items}
    read_requests = []
    for symbol, symbol_info in symbol_infos.items():
        ts_to_read_from = _generate_date_to_read_from(
            symbol_info.start_indexes,
            symbol_info.start_rows,
            symbol_info.end_rows,
            symbol_info.rows_to_append(),
            levels,
            threshold,
        )
        if ts_to_read_from is not None:
            read_requests.append(ReadRequest(symbol, date_range=(ts_to_read_from, None)))
    vits = lib.read_batch(read_requests)
    for vit in vits:
        check(isinstance(vit, VersionedItem), f"Unexpected DataError when tailing existing data: {vit}")
        symbol_infos[vit.symbol].existing_data_tail = vit.data
    update_payloads = []
    for symbol, symbol_info in symbol_infos.items():
        update_df = symbol_info.update_df()
        if update_df is not None:
            update_payloads.append(UpdatePayload(symbol, update_df))
    lib.update_batch(update_payloads, upsert=True)
