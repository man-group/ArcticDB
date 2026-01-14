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
    if not len(levels) or not len(start_rows) or new_df_row_count == 0:
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


def _append_and_defrag_idempotent(
    lib: Library,
    items: List[Tuple[str, pd.DataFrame]],
    factor: int,
    threshold: float = 0.9,
) -> None:
    """
    Appends provided dataframes to the specified symbols idempotently, and performs an exponential defragmentation of
    the row slices at the same time based on the rows_per_segment library option and the provided factor. Upserts the
    data if the symbol does not exist.
    Caveats:
    * Data must be a timeseries
    * Symbols must be append-only
    * Expects all modifications to the symbols to be using this function
    * Must not be used from two processes at once against the same symbols
    * Must use the same `factor` argument on every call to a given symbol
    * Expects repeated calls with the same index values for a given symbol's dataframe to have the same values in the
      columns as well

    Parameters
    ----------
    lib : `Library`
        The library containing the symbols to append this data to
    items : `List[Tuple[str, pd.DataFrame]]`
        A list of symbol-dataframe pairs to append.
    factor : `int`
        The exponent to reduce the fragmentation by at each level. Must be >1.
        e.g. If the rows_per_segment = 100,000 (the default) and factor is 10, then the row-slice sizes that will
        trigger defragmentation will be [100,000, 10,000, 1,000, 100, 10]
    threshold : `float`
        The threshold at which a row-slice is considered to be close enough to the target row slice count. Must be
        between 0 and 1.
        e.g. with a threshold of 0.9 and rows_per_segment = 100,000, any segment with >= 90,000 rows will not be
        defragmented any further.
        Warning - setting this parameter to exactly 1 can result in repeated defragmentation of all row-slices due to
        the implementation of `update`. It is recommended to leave this at the default value.

    Returns
    -------
    None

    Examples
    --------
    ac = Arctic(URI)
    lib = ac.create_library("test", LibraryOptions(rows_per_segment=64))
    sym = "test"
    factor = 4  # defrag thresholds will be [64, 16, 4]
    rows_per_df = 4  # append 4 rows in each call
    ts = pd.Timestamp("2026-01-01")
    df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
    # Create the symbol with a single row slice 0-4
    _append_and_defrag_idempotent(lib, [(sym, df)], factor)
    # Idempotent, repeating this operation has no effect, sym will still only have 1 version
    _append_and_defrag_idempotent(lib, [(sym, df)], factor)
    for _ in range(15):
        ts += pd.Timedelta(1, unit="days")
        df = pd.DataFrame({"col": np.arange(rows_per_df)}, index=rows_per_df * [ts])
        _append_and_defrag_idempotent(lib, [(sym, df)], factor)
    The row-slice structure after each iteration will be as follows:
    0:  0-4, 4-8 no compaction happens this iteration
    1:  0-4, 4-8, 8-12 no compaction happens this iteration
    2:  0-16 defrag triggered as we have hit the next defrag threshold of 16
    3:  0-16, 16-20 no compaction happens this iteration
    4:  0-16, 16-20, 20-24 no compaction happens this iteration
    5:  0-16, 16-20, 20-24, 24-28 no compaction happens this iteration
    6:  0-16, 16-32 same reasoning as iteration 2
    7:  0-16, 16-32, 32-36 no compaction happens this iteration
    8:  0-16, 16-32, 32-36, 36-40 no compaction happens this iteration
    9:  0-16, 16-32, 32-36, 36-40, 40-44 no compaction happens this iteration
    10: 0-16, 16-32, 32-48 same reasoning as iteration 2
    11: 0-16, 16-32, 32-48, 48-52 no compaction happens this iteration
    12: 0-16, 16-32, 32-48, 48-52, 52-56 no compaction happens this iteration
    13: 0-16, 16-32, 32-48, 48-52, 52-56, 56-60 no compaction happens this iteration
    14: 0-64 defrag triggered as we have hit the next defrag threshold of 64
    """

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
                # read_index returns timezone-naive timestamps representing UTC in the start_index and end_index columns
                end_index_of_existing_df = index["end_index"][-1].tz_localize("UTC").tz_localize(
                    start_index_of_new_df.tz
                ) - pd.Timedelta(1, unit="ns")
                if start_index_of_new_df <= end_index_of_existing_df:
                    self.append_df = None
                else:
                    # Drop columns we don't need as we're going to do a filter
                    index = index[["start_row", "end_row", "start_col"]]
                    # Drop everything except the first column slice
                    first_start_col = index["start_col"][0]
                    index = index[index["start_col"] == first_start_col]
                    self.start_indexes = index.index.to_list()
                    self.start_rows = index["start_row"].to_list()
                    self.end_rows = index["end_row"].to_list()
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

    check(1 < factor, f"factor must be >1, received {factor}")
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
