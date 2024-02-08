"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from typing import Any, Optional, Union, List, Sequence, Tuple, Dict, Set
from datetime import datetime
from pandas import Timestamp

from arcticdb_ext.version_store import PythonVersionStoreReadQuery as _PythonVersionStoreReadQuery
from arcticdb_ext.version_store import PythonVersionStoreUpdateQuery as _PythonVersionStoreUpdateQuery
from arcticdb_ext.version_store import PythonVersionStoreVersionQuery as _PythonVersionStoreVersionQuery
from arcticdb.supported_types import DateRangeInput
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.preconditions import check
from arcticdb.exception import ArcticNativeException
from arcticdb._preprocess import normalize_dt_range
from arcticdb._optional import assume_true, assume_false
from arcticdb_ext.version_store import IndexRange as _IndexRange

VersionQueryInput = Union[int, str, ExplicitlySupportedDates, None]


def get_read_query(
        date_range: Optional[DateRangeInput],
        row_range: Tuple[int, int],
        columns: Optional[List[str]],
        query_builder: QueryBuilder,
):
    check(date_range is None or row_range is None, "Date range and row range both specified")

    read_query = _PythonVersionStoreReadQuery()

    if query_builder:
        read_query.add_clauses(query_builder.clauses)

    if row_range is not None:
        read_query.row_range = _SignedRowRange(row_range[0], row_range[1])

    if date_range is not None:
        read_query.row_filter = normalize_dt_range(date_range)

    if columns is not None:
        read_query.columns = list(columns)

    return read_query

def get_read_queries(
            self,
            num_symbols: int,
            date_ranges: Optional[List[Optional[DateRangeInput]]],
            row_ranges: Optional[List[Optional[Tuple[int, int]]]],
            columns: Optional[List[List[str]]],
            query_builder: Optional[Union[QueryBuilder, List[QueryBuilder]]],
    ):
        read_queries = []

        if date_ranges is not None:
            check(
                len(date_ranges) == num_symbols,
                "Mismatched number of symbols ({}) and date ranges ({}) supplied to batch read",
                num_symbols,
                len(date_ranges),
                )
        if row_ranges is not None:
            check(
                len(row_ranges) == num_symbols,
                "Mismatched number of symbols ({}) and row ranges ({}) supplied to batch read",
                num_symbols,
                len(row_ranges),
                )
        if query_builder is not None and not isinstance(query_builder, QueryBuilder):
            check(
                len(query_builder) == num_symbols,
                "Mismatched number of symbols ({}) and query builders ({}) supplied to batch read",
                num_symbols,
                len(query_builder),
                )
        if columns is not None:
            check(
                len(columns) == num_symbols,
                "Mismatched number of symbols ({}) and columns ({}) supplied to batch read",
                num_symbols,
                len(columns),
                )

        for idx in range(num_symbols):
            date_range = None
            row_range = None
            these_columns = None
            query = None
            if date_ranges is not None:
                date_range = date_ranges[idx]
            if row_ranges is not None:
                row_range = row_ranges[idx]
            if columns is not None:
                these_columns = columns[idx]
            if query_builder is not None:
                query = query_builder if isinstance(query_builder, QueryBuilder) else query_builder[idx]

            read_query = get_read_query(
                date_range=date_range,
                row_range=row_range,
                columns=these_columns,
                query_builder=query,
            )

            read_queries.append(read_query)

        return read_queries
def get_version_query(as_of: VersionQueryInput, **kwargs):
    version_query = _PythonVersionStoreVersionQuery()
    version_query.set_skip_compat(assume_true("skip_compat", kwargs))
    version_query.set_iterate_on_failure(assume_false("iterate_on_failure", kwargs))

    if isinstance(as_of, str):
        version_query.set_snap_name(as_of)
    elif isinstance(as_of, int):
        version_query.set_version(as_of)
    elif isinstance(as_of, (datetime, Timestamp)):
        version_query.set_timestamp(Timestamp(as_of).value)
    elif as_of is not None:
        raise ArcticNativeException("Unexpected combination of read parameters")

    return version_query

def get_version_queries(num_symbols: int, as_ofs: Optional[List[VersionQueryInput]], **kwargs):
    if as_ofs is not None:
        check(
            len(as_ofs) == num_symbols,
            "Mismatched number of symbols ({}) and as_ofs ({}) supplied to batch read",
            num_symbols,
            len(as_ofs),
            )
        return [get_version_query(as_of, **kwargs) for as_of in as_ofs]
    else:
        return [get_version_query(None, **kwargs) for _ in range(num_symbols)]


def get_update_query(date_range: Optional[DateRangeInput] = None):
    update_query = _PythonVersionStoreUpdateQuery()
    if date_range is not None:
        update_query.row_filter = _IndexRange(start.value, end.value)