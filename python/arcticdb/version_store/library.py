"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import copy
import datetime
import os

import pytz
from enum import Enum, auto
from typing import Optional, Any, Tuple, Dict, Union, List, Iterable, NamedTuple

from arcticdb.dependencies import _PYARROW_AVAILABLE, pyarrow as pa
from arcticdb.exceptions import ArcticDbNotYetImplemented, MissingKeysInStageResultsError
from numpy import datetime64

from arcticdb.options import LibraryOptions, EnterpriseLibraryOptions, OutputFormat, ArrowOutputStringFormat
from arcticdb.preconditions import check
from arcticdb.supported_types import Timestamp
from arcticdb.util._versions import IS_PANDAS_TWO

from arcticdb.version_store.processing import ExpressionNode, QueryBuilder
from arcticdb.version_store._store import (
    NativeVersionStore,
    VersionedItem,
    VersionedItemWithJoin,
    VersionQueryInput,
    MergeStrategy,
    MergeAction,
)
from arcticdb_ext.exceptions import ArcticException
from arcticdb_ext.version_store import DataError, StageResult, KeyNotFoundInStageResultInfo

import pandas as pd
import numpy as np
import logging
from arcticdb.version_store._normalization import normalize_metadata
from arcticdb.version_store.admin_tools import AdminTools
import arcticdb_ext as _ae

logger = logging.getLogger(__name__)


AsOf = Union[int, str, datetime.datetime]


NORMALIZABLE_TYPES = (pd.DataFrame, pd.Series, np.ndarray)


NormalizableType = Union[NORMALIZABLE_TYPES]
"""Types that can be normalised into Arctic's internal storage structure.

See Also
--------

Library.write: for more documentation on normalisation.
"""


class ArcticInvalidApiUsageException(ArcticException):
    """Exception indicating an invalid call made to the Arctic API."""


class ArcticDuplicateSymbolsInBatchException(ArcticInvalidApiUsageException):
    """Exception indicating that duplicate symbols were passed to a batch method of this module."""


class ArcticUnsupportedDataTypeException(ArcticInvalidApiUsageException):
    """Exception indicating that a method does not support the type of data provided."""


class SymbolVersion(NamedTuple):
    """A named tuple. A symbol name - version pair.

    Attributes
    ----------
    symbol : str
        Symbol name.
    version : int
        Version of the symbol.
    """

    symbol: str
    version: int

    def __repr__(self):
        return f"{self.symbol}_v{self.version}"


class VersionInfo(NamedTuple):
    """A named tuple. Descriptive information about a particular version of a symbol.

    Attributes
    ----------
    date: datetime.datetime
        Time that the version was written in UTC.
    deleted: bool
        True if the version has been deleted and is only being kept alive via a snapshot.
    snapshots: List[str]
        Snapshots that refer to this version.
    """

    date: datetime.datetime
    deleted: bool
    snapshots: List[str]

    def __repr__(self):
        result = f"(date={self.date}"
        if self.deleted:
            result += ", deleted"
        if self.snapshots:
            result += f", snapshots={self.snapshots}"
        result += ")"
        return result


class NameWithDType(NamedTuple):
    """A named tuple. A name and dtype description pair."""

    name: str
    dtype: str


class SymbolDescription(NamedTuple):
    """A named tuple. Descriptive information about the data stored under a particular symbol.

    Attributes
    ----------
    columns: Tuple[NameWithDType]
        Columns stored under the symbol.
    index : Tuple[NameWithDType]
        Index of the symbol.
    index_type : str {"NA", "index", "multi_index"}
        Whether the index is a simple index or a multi_index. ``NA`` indicates that the stored data does not have an index.
    row_count : Optional[int]
        Number of rows, or None if the symbol is pickled.
    last_update_time : datetime.datetime
        The time of the last update to the symbol, in UTC.
    date_range : Tuple[Union[pandas.Timestamp], Union[pandas.Timestamp]]
        The values of the index column in the first and last row of this symbol. Both values will be NaT if:
        - the symbol is not timestamp indexed
        - the symbol is timestamp indexed, but the sorted field of this class is UNSORTED (see below)
    sorted : str
        One of "ASCENDING", "DESCENDING", "UNSORTED", or "UNKNOWN":
        ASCENDING - The data has a timestamp index, and is sorted in ascending order. Guarantees that operations such as
                    append, update, and read with date_range work as expected.
        DESCENDING - The data has a timestamp index, and is sorted in descending order. Update and read with date_range
                     will not work.
        UNSORTED - The data has a timestamp index, and is not sorted. Can only be created by calling write, write_batch,
                   append, or append_batch with validate_index set to False. Update and read with date_range will not
                   work.
        UNKNOWN - Either the data does not have a timestamp index, or the data does have a timestamp index, but was
                  written by a client that predates this information being stored.
    """

    columns: Tuple[NameWithDType]
    index: Tuple[NameWithDType]
    index_type: str
    row_count: int
    last_update_time: datetime.datetime
    date_range: Tuple[Union[datetime.datetime, datetime64], Union[datetime.datetime, datetime64]]
    sorted: str

    def __eq__(self, other):
        # Needed as NaT != NaT
        date_range_fields_equal = (
            self.date_range[0] == other.date_range[0]
            or (np.isnat(self.date_range[0]) and np.isnat(other.date_range[0]))
        ) and (
            self.date_range[1] == other.date_range[1]
            or (np.isnat(self.date_range[1]) and np.isnat(other.date_range[1]))
        )
        if date_range_fields_equal:
            non_date_range_fields = [field for field in self._fields if field != "date_range"]
            return all(getattr(self, field) == getattr(other, field) for field in non_date_range_fields)
        else:
            return False


class WritePayload:
    """
    WritePayload is designed to enable batching of multiple operations with an API that mirrors the singular
    ``write`` API.

    Construction of ``WritePayload`` objects is only required for batch write operations.

    One instance of ``WritePayload`` refers to one unit that can be written through to ArcticDB.
    """

    def __init__(
        self, symbol: str, data: Union[Any, NormalizableType], metadata: Any = None, index_column: Optional[str] = None
    ):
        """
        Constructor.

        Parameters
        ----------
        symbol : str
            Symbol name. Limited to 255 characters. The following characters are not supported in symbols:
            ``"*", "&", "<", ">"``
        data : Any
            Data to be written. If data is not of NormalizableType then it will be pickled.
        metadata : Any, default=None
            Optional metadata to persist along with the symbol.
        index_column: Optional[str], default=None
            Optional specification of timeseries index column if data is an Arrow table. Ignored if data is not an Arrow
            table.

        See Also
        --------
        Library.write_pickle: For information on the implications of providing data that needs to be pickled.
        """
        self.symbol = symbol
        self.data = data
        self.metadata = metadata
        self.index_column = index_column

    def __repr__(self):
        res = f"WritePayload(symbol={self.symbol}, data_id={id(self.data)}"
        res += f", metadata={self.metadata}" if self.metadata is not None else ""
        res += f", index_column={self.index_column}" if self.index_column is not None else ""
        res += ")"
        return res

    def __iter__(self):
        yield self.symbol
        yield self.data
        if self.metadata is not None:
            yield self.metadata
        if self.index_column is not None:
            yield self.index_column


class WriteMetadataPayload:
    """
    WriteMetadataPayload is designed to enable batching of multiple operations with an API that mirrors the singular
    ``write_metadata`` API.

    Construction of ``WriteMetadataPayload`` objects is only required for batch write metadata operations.

    One instance of ``WriteMetadataPayload`` refers to one unit that can be written through to ArcticDB.
    """

    def __init__(self, symbol: str, metadata: Any):
        """
        Constructor.

        Parameters
        ----------
        symbol : str
            Symbol name. Limited to 255 characters. The following characters are not supported in symbols:
            ``"*", "&", "<", ">"``
        metadata : Any
            metadata to persist along with the symbol.
        """
        self.symbol = symbol
        self.metadata = metadata

    def __repr__(self):
        return f"WriteMetadataPayload(symbol={self.symbol}, metadata={self.metadata})"

    def __iter__(self):
        yield self.symbol
        yield self.metadata


class ReadRequest(NamedTuple):
    """ReadRequest is designed to enable batching of read operations with an API that mirrors the singular ``read`` API.
    Therefore, construction of this object is only required for batch read operations.

    Attributes
    ----------
    symbol: str
        See `read` method.
    as_of: Optional[AsOf], default=none
        See `read` method.
    date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]], default=none
        See `read`method.
    row_range: Optional[Tuple[Optional[int], Optional[int]]], default=none
        See `read` method.
    columns: Optional[List[str]], default=none
        See `read` method.
    query_builder: Optional[Querybuilder], default=none
        See `read` method.
    arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]], default=None
        See `read` method.
    arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]], default=None
        See `read` method.

    See Also
    --------
    Library.read: For documentation on the parameters.
    """

    symbol: str
    as_of: Optional[AsOf] = None
    date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None
    row_range: Optional[Tuple[Optional[int], Optional[int]]] = None
    columns: Optional[List[str]] = None
    query_builder: Optional[QueryBuilder] = None
    output_format: Optional[Union[OutputFormat, str]] = None
    arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None
    arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]] = None

    def __repr__(self):
        res = f"ReadRequest(symbol={self.symbol}"
        res += f", as_of={self.as_of}" if self.as_of is not None else ""
        res += f", date_range={self.date_range}" if self.date_range is not None else ""
        res += f", row_range={self.row_range}" if self.row_range is not None else ""
        res += f", columns={self.columns}" if self.columns is not None else ""
        res += f", query_builder={self.query_builder}" if self.query_builder is not None else ""
        res += f", output_format={self.output_format}" if self.output_format is not None else ""
        res += (
            f", arrow_string_format_default={self.arrow_string_format_default}"
            if self.arrow_string_format_default is not None
            else ""
        )
        res += (
            f", arrow_string_format_per_column={self.arrow_string_format_per_column}"
            if self.arrow_string_format_per_column is not None
            else ""
        )
        res += ")"
        return res


class ReadInfoRequest(NamedTuple):
    """ReadInfoRequest is useful for batch methods like read_metadata_batch and get_description_batch, where we
    only need to specify the symbol and the version information. Therefore, construction of this object is
    only required for these batch operations.

    Attributes
    ----------
    symbol : str
        See `read_metadata` method.
    as_of: Optional[AsOf], default=none
        See `read_metadata` method.

    See Also
    --------
    Library.read: For documentation on the parameters.
    """

    symbol: str
    as_of: Optional[AsOf] = None

    def __repr__(self):
        res = f"ReadInfoRequest(symbol={self.symbol}"
        res += f", as_of={self.as_of}" if self.as_of is not None else ""
        res += ")"
        return res


class DeleteRequest(NamedTuple):
    """DeleteRequest is designed to enable batching of delete operations with an API that mirrors the singular ``delete`` API.
    Therefore, construction of this object is only required for batch delete operations.

    Attributes
    ----------
    symbol: str
        See `delete` and `delete_batch` methods.
    version_ids: List[int]
        See `delete` and `delete_batch` methods.

    Raises
    ------
    ValueError
        If version_ids is empty.
    """

    symbol: str
    version_ids: List[int]

    def __repr__(self):
        return f"DeleteRequest(symbol={self.symbol}, version_ids={self.version_ids})"


class UpdatePayload:
    """
    UpdatePayload is designed to enable batching of multiple operations with an API that mirrors the singular
    ``update`` API.

    Construction of ``UpdatePayload`` objects is only required for batch update operations.

    One instance of ``UpdatePayload`` refers to one unit that can be written through to ArcticDB.

    """

    def __init__(
        self,
        symbol: str,
        data: NormalizableType,
        metadata: Any = None,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None,
        index_column: Optional[str] = None,
    ):
        """
        Constructor.

        Parameters
        ----------
        symbol : str
            Symbol name. Limited to 255 characters. The following characters are not supported in symbols:
            ``"*", "&", "<", ">"``
        data : NormalizableType
            Time-indexed data to use for the update.
        metadata : Any, default=None
            Optional metadata to persist along with the new symbol version.
        date_range : Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]], default=None
            Restricts the update to the specified range in the stored data. Leaving either bound as ``None`` leaves that
            side of the range open-ended.
        index_column: Optional[str], default=None
            Optional specification of timeseries index column if data is an Arrow table. Ignored if data is not an Arrow
            table.
        """
        self.symbol = symbol
        self.data = data
        self.metadata = metadata
        self.date_range = date_range
        self.index_column = index_column

    def __repr__(self):
        res = f"UpdatePayload(symbol={self.symbol}, data_id={id(self.data)}"
        res += f", metadata={self.metadata}" if self.metadata is not None else ""
        res += f", date_range={self.date_range}" if self.date_range is not None else ""
        res += f", index_column={self.index_column}" if self.index_column is not None else ""
        res += ")"
        return res


class LazyDataFrame(QueryBuilder):
    """
    Lazy dataframe implementation, allowing chains of queries to be added before the read is actually executed.
    Returned by `Library.read`, `Library.head`, and `Library.tail` calls when `lazy=True`.

    See Also
    --------
    QueryBuilder for supported querying operations.

    Examples
    --------

    >>>
    # Specify that we want version 0 of "test" symbol, and to only return the "new_column" column in the output
    >>> lazy_df = lib.read("test", as_of=0, columns=["new_column"], lazy=True)
    # Perform a filtering operation
    >>> lazy_df = lazy_df[lazy_df["col1"].isin(0, 3, 6, 9)]
    # Create a new column through a projection operation
    >>> lazy_df["new_col"] = lazy_df["col1"] + lazy_df["col2"]
    # Actual read and processing happens here
    >>> df = lazy_df.collect().data
    """

    def __init__(
        self,
        lib: "Library",
        read_request: ReadRequest,
    ):
        if read_request.query_builder is None:
            super().__init__()
        else:
            self.clauses = read_request.query_builder.clauses
            self._python_clauses = read_request.query_builder._python_clauses
            self._optimisation = read_request.query_builder._optimisation
        self.lib = lib
        self.read_request = read_request._replace(query_builder=None)

    def _to_read_request(self) -> ReadRequest:
        """
        Convert this object into a ReadRequest, including any queries applied to this object since the read call.

        Returns
        -------
        ReadRequest
            Object with all the parameters necessary to completely specify the data to be read.
        """
        q = QueryBuilder().prepend(self)
        q._optimisation = self._optimisation
        return ReadRequest(
            symbol=self.read_request.symbol,
            as_of=self.read_request.as_of,
            date_range=self.read_request.date_range,
            row_range=self.read_request.row_range,
            columns=self.read_request.columns,
            output_format=self.read_request.output_format,
            arrow_string_format_default=self.read_request.arrow_string_format_default,
            arrow_string_format_per_column=self.read_request.arrow_string_format_per_column,
            query_builder=q,
        )

    def collect(self) -> VersionedItem:
        """
        Read the data and execute any queries applied to this object since the read call.

        Returns
        -------
        VersionedItem
            Object that contains a .data and .metadata element.
        """
        return self.lib.read(**self._to_read_request()._asdict())

    def __str__(self) -> str:
        query_builder_repr = super().__str__()
        return f"LazyDataFrame({self.read_request.__repr__()}{' | ' if query_builder_repr else ''}{query_builder_repr})"

    # Needs to be explicitly defined for lists of these objects in LazyDataFrameCollection to render correctly
    def __repr__(self) -> str:
        return self.__str__()


class LazyDataFrameCollection(QueryBuilder):
    """
    Lazy dataframe implementation for batch operations. Allows the application of chains of queries to be added before
    the actual reads are performed. Queries applied to this object will be applied to all  the symbols being read.
    If per-symbol queries are required, split can be used to break this class into a list of `LazyDataFrame` objects.
    Returned by `Library.read_batch` calls when `lazy=True`.

    See Also
    --------
    QueryBuilder for supported querying operations.

    Examples
    --------

    >>>
    # Specify that we want the latest version of "test_0" symbol, and version 0 of "test_1" symbol
    >>> lazy_dfs = lib.read_batch(["test_0", ReadRequest("test_1", as_of=0)], lazy=True)
    # Perform a filtering operation on both the "test_0" and "test_1" symbols
    >>> lazy_dfs = lazy_dfs[lazy_dfs["col1"].isin(0, 3, 6, 9)]
    # Perform a different projection operation on each symbol
    >>> lazy_dfs = lazy_dfs.split()
    >>> lazy_dfs[0].apply("new_col", lazy_dfs[0]["col1"] + 1)
    >>> lazy_dfs[1].apply("new_col", lazy_dfs[1]["col1"] + 2)
    # Bring together again and perform the same filter on both symbols
    >>> lazy_dfs = LazyDataFrameCollection(lazy_dfs)
    >>> lazy_dfs = lazy_dfs[lazy_dfs["new_col"] > 0]
    # Actual read and processing happens here
    >>> res = lazy_dfs.collect()
    """

    def __init__(
        self,
        lazy_dataframes: List[LazyDataFrame],
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]] = None,
    ):
        """
        Gather a list of `LazyDataFrame`s into a single object that can be collected together.

        Parameters
        ----------
        lazy_dataframes : List[LazyDataFrame]
            Collection of `LazyDataFrames`s to gather together.
        """
        lib_set = {lazy_dataframe.lib for lazy_dataframe in lazy_dataframes}
        check(
            len(lib_set) in [0, 1],
            f"LazyDataFrameCollection init requires all provided lazy dataframes to be referring to the same library, but received: {[lib for lib in lib_set]}",
        )
        output_format_set = {
            lazy_dataframe.read_request.output_format
            for lazy_dataframe in lazy_dataframes
            if lazy_dataframe.read_request.output_format is not None
        }
        check(
            len(output_format_set) in [0, 1],
            f"LazyDataFrameCollection init requires all provided lazy dataframes to have the same output_format, but received: {output_format_set}",
        )
        super().__init__()
        self._lazy_dataframes = lazy_dataframes
        self._arrow_string_format_default = arrow_string_format_default
        self._arrow_string_format_per_column = arrow_string_format_per_column
        if len(self._lazy_dataframes):
            self._lib = self._lazy_dataframes[0].lib
            self._output_format = self._lazy_dataframes[0].read_request.output_format

    def split(self) -> List[LazyDataFrame]:
        """
        Separate the collection into a list of LazyDataFrames, including any queries already applied to this object.

        Returns
        -------
        List[LazyDataFrame]
        """
        return [LazyDataFrame(self._lib, read_request) for read_request in self._read_requests()]

    def collect(self) -> List[Union[VersionedItem, DataError]]:
        """
        Read the data and execute any queries applied to this object since the read_batch call.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            See documentation on `Library.read_batch`.
        """
        if not len(self._lazy_dataframes):
            return []
        return self._lib.read_batch(
            self._read_requests(),
            output_format=self._output_format,
            arrow_string_format_default=self._arrow_string_format_default,
            arrow_string_format_per_column=self._arrow_string_format_per_column,
        )

    def _read_requests(self) -> List[ReadRequest]:
        # Combines queries for individual LazyDataFrames with the global query associated with this
        # LazyDataFrameCollection and returns a list of corresponding read requests
        read_requests = [lazy_dataframe._to_read_request() for lazy_dataframe in self._lazy_dataframes]
        if len(self.clauses):
            for read_request in read_requests:
                if read_request.query_builder is None:
                    read_request.query_builder = QueryBuilder()
                read_request.query_builder.then(self)
        return read_requests

    def __str__(self) -> str:
        query_builder_repr = super().__str__()
        return (
            "LazyDataFrameCollection("
            + str(self._lazy_dataframes)
            + (" | " if len(query_builder_repr) else "")
            + query_builder_repr
            + ")"
        )

    def __repr__(self) -> str:
        return self.__str__()


class LazyDataFrameAfterJoin(QueryBuilder):
    """
    Lazy dataframe implementation, allowing chains of queries to be added before the reads and join are actually
    executed.
    Returned by joining methods such as `adb.concat`.

    See Also
    --------
    QueryBuilder for supported querying operations.

    Examples
    --------

    >>>
    # Specify that we want symbols "test0" and "test1"
    >>> lazy_dfs = lib.read_batch(["test0", "test1"], lazy=True)
    # Perform a joining operation
    >>> lazy_df_after_join = adb.concat(lazy_dfs)
    # Create a new column through a projection operation on the joined data
    >>> lazy_df_after_join["new_col"] = lazy_df_after_join["col1"] + lazy_df_after_join["col2"]
    # Actual batch read, join, and subsequent processing happens here
    >>> df = lazy_df_after_join.collect().data
    """

    def __init__(
        self,
        lazy_dataframes: LazyDataFrameCollection,
        join: QueryBuilder,
    ):
        super().__init__()
        self._lazy_dataframes = lazy_dataframes
        self.then(join)

    def collect(self) -> VersionedItemWithJoin:
        """
        Read the data and execute any queries applied to this object since the join method.

        Returns
        -------
        VersionedItemWithJoin
            Contains a .data field with the joined together data, and a list of VersionedItem objects describing the
            version number, metadata, etc., of the symbols that were joined together.
        """
        if not len(self._lazy_dataframes._lazy_dataframes):
            return []
        else:
            lib = self._lazy_dataframes._lib
            return lib.read_batch_and_join(
                self._lazy_dataframes._read_requests(),
                self,
                output_format=self._lazy_dataframes._output_format,
                arrow_string_format_default=self._lazy_dataframes._arrow_string_format_default,
                arrow_string_format_per_column=self._lazy_dataframes._arrow_string_format_per_column,
            )

    def __str__(self) -> str:
        query_builder_repr = super().__str__()
        return f"LazyDataFrameAfterJoin({self._lazy_dataframes._lazy_dataframes} | {query_builder_repr})"

    def __repr__(self) -> str:
        return self.__str__()


def concat(
    lazy_dataframes: Union[List[LazyDataFrame], LazyDataFrameCollection],
    join: str = "outer",
) -> LazyDataFrameAfterJoin:
    """
    Concatenate a list of symbols together.

    Parameters
    ----------
    join : str, default="outer"
        Whether the columns of the input symbols should be inner or outer joined. Supported inputs are "inner" and
        "outer".

        * inner - Only columns present in ALL the input symbols will be present in the returned DataFrame.
        * outer - Columns present in ANY of the input symbols will be present in the returned DataFrame. Columns
          that are present in some input symbols but not in others will be backfilled according to their type using
          the same rules as with dynamic schema.

    Returns
    -------
    LazyDataFrameAfterJoin
        Lazy DataFrame representing the joined data, to which further processing operations can be chained.

    Raises
    -------
    ArcticNativeException
        The join argument is not one of "inner" or "outer"

    Examples
    --------
    Join 2 symbols together without any pre or post processing.

    >>> df0 = pd.DataFrame(
        {
            "col1": [0.5],
            "col2": [1],
        },
        index=[pd.Timestamp("2025-01-01")],
    )
    >>> df1 = pd.DataFrame(
        {
            "col3": ["hello"],
            "col2": [2],
        },
        index=[pd.Timestamp("2025-01-02")],
    )
    >>> lib.write("symbol0", df0)
    >>> lib.write("symbol1", df1)
    >>> lazy_dfs = lib.read_batch(["symbol0", "symbol1"], lazy=True)
    >>> adb.concat(lazy_dfs, join="outer").collect().data

                               col1     col2     col3
        2025-01-01 00:00:00     0.5        1     None
        2025-01-02 00:00:00     NaN        2  "hello"

    Join 2 symbols together with both some per-symbol processing prior to the join, and some further processing after
    the join.

    >>> df0 = pd.DataFrame(
        {
            "col": [0, 1, 2, 3, 4],
        },
        index=pd.date_range("2025-01-01", freq="min", periods=5),
    )
    >>> df1 = pd.DataFrame(
        {
            "col": [5, 6, 7, 8, 9],
        },
        index=pd.date_range("2025-01-01T00:05:00", freq="min" periods=5),
    )
    >>> lib.write("symbol0", df0)
    >>> lib.write("symbol1", df1)
    >>> lazy_df0, lazy_df1 = lib.read_batch(["symbol0", "symbol1"], lazy=True).split()
    >>> lazy_df0 = lazy_df0[lazy_df0["col"] <= 2]
    >>> lazy_df1 = lazy_df1[lazy_df1["col"] <= 6]
    >>> lazy_df = adb.concat([lazy_df0, lazy_df1])
    >>> lazy_df = lazy_df.resample("10min").agg({"col": "sum"})
    >>> lazy_df.collect().data

                                col
        2025-01-01 00:00:00      14
    """
    if not isinstance(lazy_dataframes, LazyDataFrameCollection):
        lazy_dataframes = LazyDataFrameCollection(lazy_dataframes)
    return LazyDataFrameAfterJoin(lazy_dataframes, QueryBuilder().concat(join))


def col(name: str) -> ExpressionNode:
    """
    Placeholder for referencing columns by name in lazy dataframe operations before the underlying object has been
    initialised.

    Parameters
    ----------
    name : str
        Column name.

    Returns
    -------
    ExpressionNode
        Reference to named column for use in querying operations.

    Examples
    --------

    >>> lazy_df = lib.read("test", lazy=True).apply("new_col", col("col1") + col("col2"))
    >>> df = lazy_df.collect().data
    """
    return ExpressionNode.column_ref(name)


class StagedDataFinalizeMethod(Enum):
    WRITE = auto()
    APPEND = auto()


class DevTools:
    def __init__(self, nvs):
        self._nvs = nvs

    def library_tool(self):
        return self._nvs.library_tool()

    def remove_incompletes(self, symbols: List[str]):
        """
        Removes staged data for several symbols.

        Does not raise if a symbol has no staged data.

        In the worst case this can list over all the staged data in your library, so if you are only touching a small
        subset of the staged data in your library, it may be better to use the remove_incomplete method above.

        This is a private function for now and its API is not stable.

        Parameters
        ----------
        symbols : List[str]
            Symbols to remove staged data for.
        """
        if self._nvs.get_backing_store() == "mongo_storage":
            # Issue ref: 8784267430
            raise ArcticDbNotYetImplemented("remove_incompletes is not yet implemented on MongoDB")
        symbols_set = set(symbols)
        common_prefix = os.path.commonprefix(symbols)
        self._nvs.version_store.remove_incompletes(symbols_set, common_prefix)


class Library:
    """
    The main interface exposing read/write functionality within a given Arctic instance.

    Arctic libraries contain named symbols which are the atomic unit of data storage within Arctic. Symbols
    contain data that in most cases resembles a DataFrame and are versioned such that all modifying
    operations can be tracked and reverted.

    Instances of this class provide a number of primitives to write, modify and remove symbols, as well as
    also providing methods to manage library snapshots. For more information on snapshots please see the `snapshot`
    method.

    Arctic libraries support concurrent writes and reads to multiple symbols as well as concurrent reads to a single
    symbol. However, concurrent writers to a single symbol are not supported other than for primitives that
    explicitly state support for single-symbol concurrent writes.
    """

    def __init__(self, arctic_instance_description: str, nvs: NativeVersionStore):
        """
        Parameters
        ----------
        arctic_instance_description
            Human readable description of the Arctic instance to which this library belongs. Used for informational
            purposes only.
        nvs
            The native version store that backs this library.
        """
        self.arctic_instance_desc = arctic_instance_description
        self._nvs = nvs
        self._nvs._normalizer.df.set_skip_df_consolidation()
        self._dev_tools = DevTools(nvs)

    def __repr__(self) -> str:
        return "Library(%s, path=%s, storage=%s)" % (
            self.arctic_instance_desc,
            self._nvs._lib_cfg.lib_desc.name,
            self._nvs.get_backing_store(),
        )

    def __getitem__(self, symbol: str) -> VersionedItem:
        return self.read(symbol)

    def __contains__(self, symbol: str) -> bool:
        return self.has_symbol(symbol)

    def _allowed_input_type(self, data) -> bool:
        if isinstance(data, NORMALIZABLE_TYPES) or (
            _PYARROW_AVAILABLE and isinstance(data, pa.Table) and self._nvs._allow_arrow_input
        ):
            return True
        else:
            return False

    def options(self) -> LibraryOptions:
        """Library options set on this library. See also `enterprise_options`."""
        write_options = self._nvs.lib_cfg().lib_desc.version.write_options
        return LibraryOptions(
            dynamic_schema=write_options.dynamic_schema,
            dedup=write_options.de_duplication,
            rows_per_segment=write_options.segment_row_size,
            columns_per_segment=write_options.column_group_size,
            encoding_version=self._nvs.lib_cfg().lib_desc.version.encoding_version,
        )

    def enterprise_options(self) -> EnterpriseLibraryOptions:
        """Enterprise library options set on this library. See also `options` for non-enterprise options."""
        write_options = self._nvs.lib_cfg().lib_desc.version.write_options
        return EnterpriseLibraryOptions(
            replication=write_options.sync_passive.enabled, background_deletion=write_options.delayed_deletes
        )

    def stage(
        self,
        symbol: str,
        data: NormalizableType,
        validate_index=True,
        sort_on_index=False,
        sort_columns: List[str] = None,
        index_column: Optional[str] = None,
    ) -> StageResult:
        """
        Similar to ``write`` but the written segments are left in an "incomplete" state, unable to be read until they
        are finalized. This enables multiple writers to a single symbol - all writing staged data at the same time -
        with one process able to later finalize all staged data rendering the data readable by clients.
        To finalize staged data see ``finalize_staged_data`` or ``sort_and_finalize_staged_data``.

        Check out the [demo notebook](https://docs.arcticdb.io/latest/notebooks/ArcticDB_staged_data_with_tokens/) for more info and examples.

        Parameters
        ----------
        symbol : str
            Symbol name. Limited to 255 characters. The following characters are not supported in symbols:
            ``"*", "&", "<", ">"``
        data : NormalizableType
            Data to be written. Staged data must be normalizable.
        validate_index:
            Check that the index is sorted prior to writing. In the case of unsorted data, throw an UnsortedDataException.
            Note that no checks are performed for Arrow input data.
        sort_on_index:
            If an appropriate index is present, sort the data on it. In combination with sort_columns the
            index will be used as the primary sort column, and the others as secondaries.
        sort_columns:
            Sort the data by specific columns prior to writing.
        index_column: Optional[str], default=None
            Optional specification of timeseries index column if data is an Arrow table. Ignored if data is not an Arrow
            table.

        Returns
        -------
        StageResult
            Structure describing the segments that were staged and which can later be passed to ``finalize_staged_data``
            or ``sort_and_finalize_staged_data`` to specify which data to finalize.

        """

        if not self._allowed_input_type(data):
            raise ArcticUnsupportedDataTypeException(
                "data is of a type that cannot be normalized. Consider using "
                f"write_pickle instead. type(data)=[{type(data)}]"
            )

        return self._nvs.stage(
            symbol,
            data,
            validate_index=validate_index,
            sort_on_index=sort_on_index,
            sort_columns=sort_columns,
            index_column=index_column,
            norm_failure_options_msg="Failed to normalize data. It is inadvisable to pickle staged data"
            " as it will not be possible to finalize it.",
        )

    def write(
        self,
        symbol: str,
        data: NormalizableType,
        metadata: Any = None,
        prune_previous_versions: bool = False,
        staged=False,
        validate_index=True,
        index_column: Optional[str] = None,
        recursive_normalizers: bool = None,
    ) -> VersionedItem:
        """
        Write ``data`` to the specified ``symbol``. If ``symbol`` already exists then a new version will be created to
        reference the newly written data. For more information on versions see the documentation for the `read`
        primitive.

        ``data`` must be of a format that can be normalised into Arctic's internal storage structure. Pandas
        DataFrames, Pandas Series and Numpy NDArrays can all be normalised. Normalised data will be split along both the
        columns and rows into segments. By default, a segment will contain 100,000 rows and 127 columns.

        If this library has ``write_deduplication`` enabled then segments will be deduplicated against storage prior to
        write to reduce required IO operations and storage requirements. Data will be effectively deduplicated for all
        segments up until the first differing row when compared to storage. As a result, modifying the beginning
        of ``data`` with respect to previously written versions may significantly reduce the effectiveness of
        deduplication.

        Note that `write` is not designed for multiple concurrent writers over a single symbol.
        For that case see ``stage()``.

        Note: ArcticDB will use the 0-th level index of the Pandas DataFrame for its on-disk index.

        Any non-`DatetimeIndex` will converted into an internal `RowCount` index. That is, ArcticDB will assign each
        row a monotonically increasing integer identifier and that will be used for the index.

        See the Metadata section of our online documentation for details about how metadata is persisted and caveats.

        Parameters
        ----------
        symbol : str
            Symbol name. Limited to 255 characters. The following characters are not supported in symbols:
            ``"*", "&", "<", ">"``
        data : NormalizableType
            Data to be written. To write non-normalizable data, use `write_pickle`.
        metadata : Any, default=None
            Optional metadata to persist along with the symbol.
        prune_previous_versions : bool, default=False
            Removes previous (non-snapshotted) versions from the database.
        staged: bool, default=False
            Deprecated. Use stage() instead.
            Whether to write to a staging area rather than immediately to the library.
            See documentation on `finalize_staged_data` for more information.
        validate_index: bool, default=True
            If True, verify that the index of `data` supports date range searches and update operations.
            This tests that the data is sorted in ascending order, using Pandas DataFrame.index.is_monotonic_increasing.
            Note that no checks are performed for Arrow input data.
        index_column: Optional[str], default=None
            Optional specification of timeseries index column if data is an Arrow table. Ignored if data is not an Arrow
            table.
        recursive_normalizers: bool, default None
            Whether to recursively normalize nested data structures when writing sequence-like or dict-like data.
            If None, falls back to the corresponding setting in the library configuration. For libraries created with < v6.4.0,
            the default library configuration is True, otherwise it is False.
            The library configuration can be modified via Arctic.modify_library_option(). Please refer to
            https://docs.arcticdb.io/latest/api/arctic/#arcticdb.Arctic.modify_library_option for more details.
            The data structure can be nested or a mix of lists, dictionaries and tuples.
            Example:
                data = {"a": np.arange(5), "b": pd.DataFrame({"col": [1, 2, 3]})}
                lib.write(symbol, data, recursive_normalizers=False) # ArcticUnsupportedDataTypeException will be thrown
                lib.write(symbol, data, recursive_normalizers=True) # The data will be successfully written
                ac.modify_library_option(lib, ModifiableLibraryOption.RECURSIVE_NORMALIZERS, True)
                lib.write(symbol, data) # The data will be successfully written
            Please refer to https://docs.arcticdb.io/latest/notebooks/arcticdb_demo_recursive_normalizers for more details
            of this feature.
            Please check https://docs.arcticdb.io/latest/runtime_config/#versionstorerecursivenormalizermetastructure for the plan of
            introducing meta structure v2. V2 has removed the dependency on pickle for normalizing meta structure. Please consider switching to V2
            as V1 will be deprecated in future v7.0.0 release. However note that reading V2 meta structure requires ArcticDB version >= 6.7.0,
            or KeyError will be raised.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the written symbol in the store.

        Raises
        ------
        ArcticUnsupportedDataTypeException
            If ``data`` is not of NormalizableType.
        UnsortedDataException
            If data is unsorted and validate_index is set to True.

        Examples
        --------

        >>> df = pd.DataFrame({'column': [5,6,7]})
        >>> lib.write("symbol", df, metadata={'my_dictionary': 'is_great'})
        >>> lib.read("symbol").data
           column
        0       5
        1       6
        2       7

        WritePayload objects can be unpacked and used as parameters:
        >>> w = adb.WritePayload("symbol", df, metadata={'the': 'metadata'})
        >>> lib.write(*w, staged=True)
        """
        is_recursive_normalizers_enabled = self._nvs._is_recursive_normalizers_enabled(
            **{"recursive_normalizers": recursive_normalizers}
        )
        if not self._allowed_input_type(data):
            if is_recursive_normalizers_enabled:
                if staged:
                    raise ArcticUnsupportedDataTypeException(
                        "Staged data cannot be natively normalized. The recursive normalizer is enabled but is not allowed to work on staged data."
                    )
            else:
                raise ArcticUnsupportedDataTypeException(
                    "Data is of a type that cannot be normalized. Consider using "
                    f"write_pickle instead. type(data)=[{type(data)}]"
                )

        return self._nvs.write(
            symbol=symbol,
            data=data,
            metadata=metadata,
            prune_previous_version=prune_previous_versions,
            pickle_on_failure=False,
            parallel=staged,
            validate_index=validate_index,
            index_column=index_column,
            norm_failure_options_msg="Using write_pickle will allow the object to be written. However, many operations "
            "(such as date_range filtering and column selection) will not work on pickled data.",
            recursive_normalizers=recursive_normalizers,
            recursive_normalize_msgpack_no_pickle_fallback=True,
        )

    def write_pickle(
        self,
        symbol: str,
        data: Any,
        metadata: Any = None,
        prune_previous_versions: bool = False,
        staged=False,
        recursive_normalizers: bool = None,
    ) -> VersionedItem:
        """
        See `write`. This method differs from `write` only in that ``data`` can be of any type that is serialisable via
        the Pickle library. There are significant downsides to storing data in this way:

        - Retrieval can only be done in bulk. Calls to `read` will not support `date_range`, `query_builder` or `columns`.
        - The data cannot be updated or appended to via the update and append methods.
        - Writes cannot be deduplicated in any way.

        Parameters
        ----------
        symbol
            See documentation on `write`.
        data : `Any`
            Data to be written.
        metadata
            See documentation on `write`.
        prune_previous_versions
            See documentation on `write`.
        staged
            See documentation on `write`.
        recursive_normalizers: bool, default None
            See documentation on `write`.
            If enabled, attempts to recursively normalize data before falling back to pickling.
            If the leaf nodes cannot be natively normalized, they will be pickled,
            resulting in the overall data being recursively normalized and partially pickled.
            Example:
                data = {"a": np.arange(5), "b": ABC()} # ABC is some custom class that cannot be natively normalized
                # Exception will be thrown, as the leaf node requires pickling to normalize
                lib.write(symbol, data, recursive_normalizers=True)
                # The data will be successfully written by partially pickling the leaf node
                lib.write_pickle(symbol, data, recursive_normalizers=True)
                # The data will be successfully written by pickling the whole object
                lib.write_pickle(symbol, data)

        Returns
        -------
        VersionedItem
            See documentation on `write`.

        Examples
        --------

        >>> lib.write_pickle("symbol", [1,2,3])
        >>> lib.read("symbol").data
        [1, 2, 3]

        See Also
        --------
        write: For more detailed documentation.
        """
        return self._nvs.write(
            symbol=symbol,
            data=data,
            metadata=metadata,
            prune_previous_version=prune_previous_versions,
            pickle_on_failure=True,
            parallel=staged,
            recursive_normalizers=recursive_normalizers,
            recursive_normalize_msgpack_no_pickle_fallback=False,
        )

    @staticmethod
    def _raise_if_duplicate_symbols_in_batch(batch):
        symbols = {p.symbol for p in batch}
        if len(symbols) < len(batch):
            raise ArcticDuplicateSymbolsInBatchException

    def _raise_if_unsupported_type_in_write_batch(self, payloads):
        bad_symbols = []
        for p in payloads:
            if not self._allowed_input_type(p.data):
                bad_symbols.append((p.symbol, type(p.data)))

        if not bad_symbols:
            return

        error_message = (
            "payload contains some data of types that cannot be normalized. Consider using "
            f"write_pickle_batch instead. symbols with bad datatypes={bad_symbols[:5]}"
        )
        if len(bad_symbols) > 5:
            error_message += f" (and more)... {len(bad_symbols)} data in total have bad types."
        raise ArcticUnsupportedDataTypeException(error_message)

    def write_batch(
        self, payloads: List[WritePayload], prune_previous_versions: bool = False, validate_index=True
    ) -> List[Union[VersionedItem, DataError]]:
        """
        Write a batch of multiple symbols.

        Parameters
        ----------
        payloads : `List[WritePayload]`
            Symbols and their corresponding data. There must not be any duplicate symbols in `payload`.
        prune_previous_versions: `bool`, default=False
            Removes previous (non-snapshotted) versions from the database.
        validate_index: bool, default=True
            Verify that each entry in the batch has an index that supports date range searches and update operations.
            This tests that the data is sorted in ascending order, using Pandas DataFrame.index.is_monotonic_increasing.
            Note that no checks are performed for Arrow input data.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            List of versioned items. The data attribute will be None for each versioned item.
            i-th entry corresponds to i-th element of `payloads`. Each result correspond to
            a structure containing metadata and version number of the written symbols in the store, in the
            same order as `payload`.
            If a key error or any other internal exception is raised, a DataError object is returned, with symbol,
            error_code, error_category, and exception_string properties.

        Raises
        ------
        ArcticDuplicateSymbolsInBatchException
            When duplicate symbols appear in payload.
        ArcticUnsupportedDataTypeException
            If data that is not of NormalizableType appears in any of the payloads.

        See Also
        --------
        write: For more detailed documentation.

        Examples
        --------

        Writing a simple batch:
        >>> df_1 = pd.DataFrame({'column': [1,2,3]})
        >>> df_2 = pd.DataFrame({'column': [4,5,6]})
        >>> payload_1 = adb.WritePayload("symbol_1", df_1, metadata={'the': 'metadata'})
        >>> payload_2 = adb.WritePayload("symbol_2", df_2)
        >>> items = lib.write_batch([payload_1, payload_2])
        >>> lib.read("symbol_1").data
           column
        0       1
        1       2
        2       3
        >>> lib.read("symbol_2").data
           column
        0       4
        1       5
        2       6
        >>> items[0].symbol, items[1].symbol
        ('symbol_1', 'symbol_2')
        """
        self._raise_if_duplicate_symbols_in_batch(payloads)
        self._raise_if_unsupported_type_in_write_batch(payloads)

        throw_on_error = False
        return self._nvs._batch_write_internal(
            [p.symbol for p in payloads],
            [p.data for p in payloads],
            [p.metadata for p in payloads],
            prune_previous_version=prune_previous_versions,
            pickle_on_failure=False,
            validate_index=validate_index,
            index_column_vector=[p.index_column for p in payloads],
            throw_on_error=throw_on_error,
            norm_failure_options_msg="Using write_pickle_batch will allow the object to be written. However, many "
            "operations (such as date_range filtering and column selection) will not work on "
            "pickled data.",
        )

    def write_pickle_batch(
        self, payloads: List[WritePayload], prune_previous_versions: bool = False
    ) -> List[Union[VersionedItem, DataError]]:
        """
        Write a batch of multiple symbols, pickling their data if necessary.

        Parameters
        ----------
        payloads : `List[WritePayload]`
            Symbols and their corresponding data. There must not be any duplicate symbols in `payload`.
        prune_previous_versions: `bool`, default=False
            Removes previous (non-snapshotted) versions from the database.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            Structures containing metadata and version number of the written symbols in the store, in the
            same order as `payload`. If a key error or any other internal exception is raised, a DataError object is
            returned, with symbol, error_code, error_category, and exception_string properties.

        Raises
        ------
        ArcticDuplicateSymbolsInBatchException
            When duplicate symbols appear in payload.

        See Also
        --------
        write: For more detailed documentation.
        write_pickle: For information on the implications of providing data that needs to be pickled.
        """
        self._raise_if_duplicate_symbols_in_batch(payloads)

        return self._nvs._batch_write_internal(
            [p.symbol for p in payloads],
            [p.data for p in payloads],
            [p.metadata for p in payloads],
            prune_previous_version=prune_previous_versions,
            pickle_on_failure=True,
            validate_index=False,
            throw_on_error=False,
        )

    def append(
        self,
        symbol: str,
        data: NormalizableType,
        metadata: Any = None,
        prune_previous_versions: bool = False,
        validate_index: bool = True,
        index_column: Optional[str] = None,
    ) -> VersionedItem:
        """
        Appends the given data to the existing, stored data. Append always appends along the index. A new version will
        be created to reference the newly-appended data. Append only accepts data for which the index of the first
        row is equal to or greater than the index of the last row in the existing data.

        Appends containing differing column sets to the existing data are only possible if the library has been
        configured to support dynamic schemas.

        If `append` is called on a symbol that does not exist, it will create it. This is convenient when setting up
        a new symbol, but be careful - it will not work for creating a new version of an existing symbol. Use `write`
        in that case.

        Note that `append` is not designed for multiple concurrent writers over a single symbol.

        Parameters
        ----------
        symbol
            Symbol name.
        data
            Data to be written.
        metadata
            Optional metadata to persist along with the new symbol version. Note that the metadata is
            not combined in any way with the metadata stored in the previous version.
        prune_previous_versions
            Removes previous (non-snapshotted) versions from the database.
        validate_index
            If True, verify that the index of `data` supports date range searches and update operations.
            This tests that the data is sorted in ascending order, using Pandas DataFrame.index.is_monotonic_increasing.
            Note that no checks are performed for Arrow input data.
        index_column: Optional[str], default=None
            Optional specification of timeseries index column if data is an Arrow table. Ignored if data is not an Arrow
            table.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the written symbol in the store.

        Raises
        ------
        UnsortedDataException
            If data is unsorted, when validate_index is set to True.

        Examples
        --------

        >>> df = pd.DataFrame(
        ...    {'column': [1,2,3]},
        ...    index=pd.date_range(start='1/1/2018', end='1/03/2018')
        ... )
        >>> df
                    column
        2018-01-01       1
        2018-01-02       2
        2018-01-03       3
        >>> lib.write("symbol", df)
        >>> to_append_df = pd.DataFrame(
        ...    {'column': [4,5,6]},
        ...    index=pd.date_range(start='1/4/2018', end='1/06/2018')
        ... )
        >>> to_append_df
                    column
        2018-01-04       4
        2018-01-05       5
        2018-01-06       6
        >>> lib.append("symbol", to_append_df)
        >>> lib.read("symbol").data
                    column
        2018-01-01       1
        2018-01-02       2
        2018-01-03       3
        2018-01-04       4
        2018-01-05       5
        2018-01-06       6
        """

        if not self._allowed_input_type(data):
            raise ArcticUnsupportedDataTypeException(
                f"data is of a type that cannot be normalized. type(data)=[{type(data)}]"
            )

        return self._nvs.append(
            symbol=symbol,
            dataframe=data,
            metadata=metadata,
            prune_previous_version=prune_previous_versions,
            validate_index=validate_index,
            index_column=index_column,
        )

    def append_batch(
        self, append_payloads: List[WritePayload], prune_previous_versions: bool = False, validate_index=True
    ) -> List[Union[VersionedItem, DataError]]:
        """
        Append data to multiple symbols in a batch fashion. This is more efficient than making multiple `append` calls in
        succession as some constant-time operations can be executed only once rather than once for each element of
        `append_payloads`.
        Note that this isn't an atomic operation - it's possible for one symbol to be fully written and readable before
        another symbol.

        Parameters
        ----------
        append_payloads : `List[WritePayload]`
            Symbols and their corresponding data. There must not be any duplicate symbols in `append_payloads`.
        prune_previous_versions : bool, default=False
            Removes previous (non-snapshotted) versions from the database.
        validate_index: bool, default=True
            Verify that each entry in the batch has an index that supports date range searches and update operations.
            This tests that the data is sorted in ascending order, using Pandas DataFrame.index.is_monotonic_increasing.
            Note that no checks are performed for Arrow input data.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            List of versioned items. i-th entry corresponds to i-th element of `append_payloads`.
            Each result correspond to a structure containing metadata and version number of the affected
            symbol in the store. If a key error or any other internal exception is raised, a DataError object is returned, with symbol,
            error_code, error_category, and exception_string properties.

        Raises
        ------
        ArcticDuplicateSymbolsInBatchException
            When duplicate symbols appear in payload.
        ArcticUnsupportedDataTypeException
            If data that is not of NormalizableType appears in any of the payloads.
        """

        self._raise_if_duplicate_symbols_in_batch(append_payloads)
        self._raise_if_unsupported_type_in_write_batch(append_payloads)
        throw_on_error = False

        return self._nvs._batch_append_to_versioned_items(
            [p.symbol for p in append_payloads],
            [p.data for p in append_payloads],
            [p.metadata for p in append_payloads],
            prune_previous_version=prune_previous_versions,
            validate_index=validate_index,
            throw_on_error=throw_on_error,
            index_column_vector=[p.index_column for p in append_payloads],
        )

    def update(
        self,
        symbol: str,
        data: Union[pd.DataFrame, pd.Series],
        metadata: Any = None,
        upsert: bool = False,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None,
        prune_previous_versions: bool = False,
        index_column: Optional[str] = None,
    ) -> VersionedItem:
        """
        Overwrites existing symbol data with the contents of ``data``. The entire range between the first and last index
        entry in ``data`` is replaced in its entirety with the contents of ``data``, adding additional index entries if
        required. `update` only operates over the outermost index level - this means secondary index rows will be
        removed if not contained in ``data``.

        Both the existing symbol version and ``data`` must be timeseries-indexed.

        In the case where ``data`` has zero rows, nothing will be done and no new version will be created. This means that
        `update` cannot be used with ``date_range`` to just delete a subset of the data. We have `delete_data_in_range`
        for exactly this purpose and to make it very clear when deletion is intended.

        Note that `update` is not designed for multiple concurrent writers over a single symbol.

        If using static schema then all the column names of ``data``, their order, and their type must match the columns already in storage.

        If dynamic schema is used then data will override everything in storage for the entire index of ``data``. Update
        will not keep columns from storage which are not in ``data``.

        The update will split the first and last segments in the storage that intersect with 'data'. Therefore, frequent
        calls to update might lead to data fragmentation (see the example below).

        Parameters
        ----------
        symbol
            Symbol name.
        data
            Timeseries indexed data to use for the update.
        metadata
            Metadata to persist along with the new symbol version.
        upsert: bool, default=False
            If True, will write the data even if the symbol does not exist.
        date_range: `Tuple[Optional[Timestamp], Optional[Timestamp]]`, default=None
            If a range is specified, it will delete the stored value within the range and overwrite it with the data in
            ``data``. This allows the user to update with data that might only be a subset of the stored value. Leaving
            any part of the tuple as None leaves that part of the range open ended. Only data with date_range will be
            modified, even if ``data`` covers a wider date range.
        prune_previous_versions: bool, default=False
            Removes previous (non-snapshotted) versions from the database.
        index_column: Optional[str], default=None
            Optional specification of timeseries index column if data is an Arrow table. Ignored if data is not an Arrow
            table.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the written symbol in the store.

        Examples
        --------

        >>> df = pd.DataFrame(
        ...    {'column': [1,2,3,4]},
        ...    index=pd.date_range(start='1/1/2018', end='1/4/2018')
        ... )
        >>> df
                    column
        2018-01-01       1
        2018-01-02       2
        2018-01-03       3
        2018-01-04       4
        >>> lib.write("symbol", df)
        >>> update_df = pd.DataFrame(
        ...    {'column': [400, 40]},
        ...    index=pd.date_range(start='1/1/2018', end='1/3/2018', freq='2D')
        ... )
        >>> update_df
                    column
        2018-01-01     400
        2018-01-03      40
        >>> lib.update("symbol", update_df)
        >>> # Note that 2018-01-02 is gone despite not being in update_df
        >>> lib.read("symbol").data
                    column
        2018-01-01     400
        2018-01-03      40
        2018-01-04       4

        Update will split the first and the last segment intersecting with ``data``
        >>> index = pd.date_range(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01"))
        >>> df = pd.DataFrame({f"col_{i}": range(len(index)) for i in range(1)}, index=index)
        >>> lib.write("test", df)
        >>> lt=lib._dev_tools.library_tool()
        >>> print(lt.read_index("test"))
        start_index                     end_index  version_id stream_id          creation_ts         content_hash  index_type  key_type  start_col  end_col  start_row  end_row
        2024-01-01  2024-02-01 00:00:00.000000001           0   b'test'  1738599073224386674  9652922778723941392          84         2          1        2          0       32
        >>> update_index=pd.date_range(pd.Timestamp("2024-01-10"), freq="ns", periods=200000)
        >>> update = pd.DataFrame({f"col_{i}": [1] for i in range(1)}, index=update_index)
        >>> lib.update("test", update)
        >>> print(lt.read_index("test"))
        start_index                                    end_index  version_id stream_id          creation_ts          content_hash  index_type  key_type  start_col  end_col  start_row  end_row
        2024-01-01 00:00:00.000000 2024-01-09 00:00:00.000000001           1   b'test'  1738599073268200906  13838161946080117383          84         2          1        2          0        9
        2024-01-10 00:00:00.000000 2024-01-10 00:00:00.000100000           1   b'test'  1738599073256354553  15576483210589662891          84         2          1        2          9   100009
        2024-01-10 00:00:00.000100 2024-01-10 00:00:00.000200000           1   b'test'  1738599073256588040  12429442054752910013          84         2          1        2     100009   200009
        2024-01-11 00:00:00.000000 2024-02-01 00:00:00.000000001           1   b'test'  1738599073268493107   5975110026983744452          84         2          1        2     200009   200031

        """

        if not self._allowed_input_type(data):
            raise ArcticUnsupportedDataTypeException(
                f"data is of a type that cannot be normalized. type(data)=[{type(data)}]"
            )

        return self._nvs.update(
            symbol=symbol,
            data=data,
            metadata=metadata,
            upsert=upsert,
            date_range=date_range,
            prune_previous_version=prune_previous_versions,
            index_column=index_column,
        )

    def update_batch(
        self,
        update_payloads: List[UpdatePayload],
        upsert: bool = False,
        prune_previous_versions: bool = False,
    ) -> List[Union[VersionedItem, DataError]]:
        """
        Perform an update operation on a list of symbols in parallel. All constrains on
        [update](#arcticdb.version_store.library.Library.update) apply to this call as well.

        Parameters
        ----------
        update_payloads: List[UpdatePayload]
            List of `UpdatePayload`. Each element of the list describes an update operation for a
            particular symbol. Providing the symbol name, data, etc. The same symbol should not appear twice in this
            list.
        prune_previous_versions: bool, default=False
            Removes previous (non-snapshotted) versions from the library.
        upsert: bool, default=False
            If True any symbol in `update_payloads` which is not already in the library will be created.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            List of versioned items. i-th entry corresponds to i-th element of `update_payloads`. Each result correspond
            to a structure containing metadata and version number of the affected symbol in the store. If a key error or
            any other internal exception is raised, a DataError object is returned, with symbol, error_code,
            error_category, and exception_string properties.

        Raises
        ------
        ArcticDuplicateSymbolsInBatchException
            When duplicate symbols appear in payload.
        ArcticUnsupportedDataTypeException
            If data that is not of NormalizableType appears in any of the payloads.

        Examples
        ------
        >>> df1 = pd.DataFrame({'column_1': [1, 2, 3]}, index=pd.date_range("2025-01-01", periods=3))
        >>> df1
                    column_1
        2025-01-01         1
        2025-01-02         2
        2025-01-03         3
        >>> df2 = pd.DataFrame({'column_2': [10, 11]}, index=pd.date_range("2024-01-01", periods=2))
        >>> df2
                    column_2
        2024-01-01        10
        2024-01-02        11
        >>> lib.write("symbol_1", df1)
        >>> lib.write("symbol_2", df1)
        >>> lib.update_batch([arcticdb.library.UpdatePayload("symbol_1", pd.DataFrame({"column_1": [4, 5]}, index=pd.date_range("2025-01-03", periods=2))), arcticdb.library.UpdatePayload("symbol_2", pd.DataFrame({"column_2": [-1]}, index=pd.date_range("2023-01-01", periods=1)))])
        [VersionedItem(symbol='symbol_1', library='test', data=n/a, version=1, metadata=(None,), host='LMDB(path=...)', timestamp=1737542783853861819), VersionedItem(symbol='symbol_2', library='test', data=n/a, version=1, metadata=(None,), host='LMDB(path=...)', timestamp=1737542783851798754)]
        >>> lib.read("symbol_1").data
                    column_1
        2025-01-01         1
        2025-01-02         2
        2025-01-03         4
        2025-01-04         5
        >>> lib.read("symbol_2").data
                    column_2
        2023-01-01        -1
        2024-01-01        10
        2024-01-02        11
        """

        self._raise_if_duplicate_symbols_in_batch(update_payloads)
        self._raise_if_unsupported_type_in_write_batch(update_payloads)

        batch_update_result = self._nvs._batch_update_internal(
            [p.symbol for p in update_payloads],
            [p.data for p in update_payloads],
            [p.metadata for p in update_payloads],
            [p.date_range for p in update_payloads],
            prune_previous_version=prune_previous_versions,
            upsert=upsert,
            index_column_vector=[p.index_column for p in update_payloads],
        )
        return batch_update_result

    def delete_staged_data(self, symbol: str) -> None:
        """
        Removes staged data.

        Parameters
        ----------
        symbol : `str`
            Symbol to remove staged data for.

        See Also
        --------
        write
            Documentation on the ``staged`` parameter explains the concept of staged data in more detail.
        """
        self._nvs.remove_incomplete(symbol)

    def finalize_staged_data(
        self,
        symbol: str,
        mode: Optional[Union[StagedDataFinalizeMethod, str]] = StagedDataFinalizeMethod.WRITE,
        prune_previous_versions: bool = False,
        metadata: Any = None,
        validate_index=True,
        delete_staged_data_on_failure: bool = False,
        stage_results: Optional[List[StageResult]] = None,
    ) -> VersionedItem:
        """
        Finalizes staged data, making it available for reads. All staged segments must be ordered and non-overlapping.
        ``finalize_staged_data`` is less time-consuming than ``sort_and_finalize_staged_data``.

        If ``mode`` is ``StagedDataFinalizeMethod.APPEND`` or ``append`` the index of the first row of the new segment must be equal to or greater
        than the index of the last row in the existing data.

        If ``Static Schema`` is used all staged block must have matching schema (same column names, same dtype, same column ordering)
        and must match the existing data if mode is ``StagedDataFinalizeMethod.APPEND``. For more information about schema options see
        documentation for ``arcticdb.LibraryOptions.dynamic_schema``

        If the symbol does not exist both ``StagedDataFinalizeMethod.APPEND`` and ``StagedDataFinalizeMethod.WRITE`` will create it.

        Calling ``finalize_staged_data`` without having staged data for the symbol will throw ``UserInputException``. Use
        ``get_staged_symbols`` to check if there are staged segments for the symbol.

        Calling ``finalize_staged_data`` if any of the staged segments contains NaT in its index will throw ``SortingException``.

        Parameters
        ----------
        symbol : `str`
            Symbol to finalize data for.

        mode : Union[str, StagedDataFinalizeMethod], default=StagedDataFinalizeMethod.WRITE
            Finalize mode. Valid options are WRITE or APPEND. Write collects the staged data and writes them to a
            new timeseries. Append collects the staged data and appends them to the latest version.

            Also accepts strings "write" or "append" (case-insensitive).

        prune_previous_versions: bool, default=False
            Removes previous (non-snapshotted) versions from the database.
        metadata : Any, default=None
            Optional metadata to persist along with the symbol.
        validate_index: bool, default=True
            If True, and staged segments are timeseries, will verify that the index of the symbol after this operation
            supports date range searches and update operations. This requires that the indexes of the staged segments
            are non-overlapping with each other, and, in the case of `StagedDataFinalizeMethod.APPEND`, fall after the
            last index value in the previous version.  Note that no checks are performed for Arrow input data.
        delete_staged_data_on_failure : bool, default=False
            Determines the handling of staged data when an exception occurs during the execution of the
            ``finalize_staged_data`` function.

            - If set to True, all staged data for the specified symbol will be deleted if an exception occurs.
               If ``stage_results`` is provided, only the provided ones will be deleted.
            - If set to False, the staged data will be retained and will be used in subsequent calls to
              ``finalize_staged_data``.

            To manually delete staged data, use the ``delete_staged_data`` function.
        stage_results: Optional[List[StageResult]], default=None
            If specified, only the data corresponding to the provided ``StageResult``s will be finalized. See ``stage``.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the written symbol in the store.
            The data member will be None.

        Raises
        ------
        SortingException

            - If any two staged segments for a given symbol have overlapping indexes
            - If any staged segment for a given symbol is not sorted
            - If the first index value of the new segment is not greater or equal than the last index value of
                the existing data when ``StagedDataFinalizeMethod.APPEND`` is used.
            - If any staged segment contains NaT in the index

        UserInputException

            - If there are no staged segments when ``finalize_staged_data`` is called
            - If all of the following conditions are met:

                1. Static schema is used.
                2. The width of the DataFrame exceeds the value of ``LibraryOptions.columns_per_segment``.
                3. The symbol contains data that was not written by `finalize_staged_data`.
                4. Finalize mode is append

        SchemaException

            - If static schema is used and not all staged segments have matching schema.
            - If static schema is used, mode is ``StagedDataFinalizeMethod.APPEND`` and the schema of the new segment
                is not the same as the schema of the existing data
            - If dynamic schema is used and different segments have the same column names but their dtypes don't have a
                common type (e.g string and any numeric type)
            - If a different index name is encountered in the staged data, regardless of the schema mode

        See Also
        --------
        stage
            Documentation on the ``stage`` method explains the concept of staged data in more detail.

        Examples
        --------
        Finalizing all of the staged data
        >>> result1 = lib.stage("sym", pd.DataFrame({"col": [3, 4]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3), pd.Timestamp(2024, 1, 4)])))
        >>> result2 = lib.stage("sym", pd.DataFrame({"col": [1, 2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 2)])))
        >>> lib.finalize_staged_data("sym")
        >>> lib.read("sym").data
                    col
        2024-01-01    1
        2024-01-02    2
        2024-01-03    3
        2024-01-04    4

        Finalizing only some of the staged data
        >>> lib.finalize_staged_data("staged", StagedDataFinalizeMethod.WRITE, stage_results=[result1])
        >>> lib.read("staged").data
                    col
        2000-01-03    3
        2000-01-04    4
        """
        mode = Library._normalize_staged_data_mode(mode)

        return self._nvs.compact_incomplete(
            symbol,
            append=mode == StagedDataFinalizeMethod.APPEND,
            convert_int_to_float=False,
            metadata=metadata,
            prune_previous_version=prune_previous_versions,
            validate_index=validate_index,
            delete_staged_data_on_failure=delete_staged_data_on_failure,
            stage_results=stage_results,
        )

    def sort_and_finalize_staged_data(
        self,
        symbol: str,
        mode: Optional[Union[StagedDataFinalizeMethod, str]] = StagedDataFinalizeMethod.WRITE,
        prune_previous_versions: bool = False,
        metadata: Any = None,
        delete_staged_data_on_failure: bool = False,
        stage_results: Optional[List[StageResult]] = None,
    ) -> VersionedItem:
        """
        Sorts and merges all staged data, making it available for reads. This differs from `finalize_staged_data` in that it
        can support staged segments with interleaved time periods and staged segments which are not internally sorted. The
        end result will be sorted. This requires performing a full sort in memory so can be time-consuming.

        If ``mode`` is ``StagedDataFinalizeMethod.APPEND`` the index of the first row of the sorted block must be equal to or greater
        than the index of the last row in the existing data.

        If ``Static Schema`` is used all staged block must have matching schema (same column names, same dtype, same column ordering)
        and must match the existing data if mode is ``StagedDataFinalizeMethod.APPEND``. For more information about schema options see
        documentation for ``arcticdb.LibraryOptions.dynamic_schema``

        If the symbol does not exist both ``StagedDataFinalizeMethod.APPEND`` and ``StagedDataFinalizeMethod.WRITE`` will create it.

        Calling ``sort_and_finalize_staged_data`` without having staged data for the symbol will throw ``UserInputException``. Use
        ``get_staged_symbols`` to check if there are staged segments for the symbol.

        Calling ``sort_and_finalize_staged_data`` if any of the staged segments contains NaT in its index will throw ``SortingException``.

        Parameters
        ----------
        symbol : str
            Symbol to finalize data for.

        mode : Union[str, StagedDataFinalizeMethod], default=StagedDataFinalizeMethod.WRITE
            Finalize mode. Valid options are WRITE or APPEND. Write collects the staged data and writes them to a
            new timeseries. Append collects the staged data and appends them to the latest version.

            Also accepts strings "write" or "append" (case-insensitive).

        prune_previous_versions : bool, default=False
            Removes previous (non-snapshotted) versions from the database.

        metadata : Any, default=None
            Optional metadata to persist along with the symbol.

        delete_staged_data_on_failure : bool, default=False
            Determines the handling of staged data when an exception occurs during the execution of the
            `sort_and_finalize_staged_data` function.

            - If set to True, all staged data for the specified symbol will be deleted if an exception occurs.
            - If set to False, the staged data will be retained and will be used in subsequent calls to
              ``sort_and_finalize_staged_data``.

            To manually delete staged data, use the ``delete_staged_data`` function.

        stage_results: Optional[List[StageResult]], default=None
            If specified, only the data corresponding to the provided ``StageResult``s will be finalized. See ``stage``.
        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the written symbol in the store.
            The data member will be None.

        Raises
        ------
        SortingException

            - If the first index value of the sorted block is not greater or equal than the last index value of
                the existing data when ``StagedDataFinalizeMethod.APPEND`` is used.
            - If any staged segment contains NaT in the index

        UserInputException

            - If there are no staged segments when ``sort_and_finalize_staged_data`` is called
            - If all of the following conditions are met:

                1. Static schema is used.
                2. The width of the DataFrame exceeds the value of ``LibraryOptions.columns_per_segment``.
                3. The symbol contains data that was not written by `sort_and_finalize_staged_data`.
                4. Finalize mode is append

        SchemaException

            - If static schema is used and not all staged segments have matching schema.
            - If static schema is used, mode is ``StagedDataFinalizeMethod.APPEND`` and the schema of the sorted and merged
                staged segment is not the same as the schema of the existing data
            - If dynamic schema is used and different segments have the same column names but their dtypes don't have a
                common type (e.g string and any numeric type)
            - If a different index name is encountered in the staged data, regardless of the schema mode

        See Also
        --------
        write
            Documentation on the ``staged`` parameter explains the concept of staged data in more detail.

        Examples
        --------
        >>> lib.write("sym", pd.DataFrame({"col": [2, 4]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2), pd.Timestamp(2024, 1, 4)])), staged=True)
        >>> lib.write("sym", pd.DataFrame({"col": [3, 1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3), pd.Timestamp(2024, 1, 1)])), staged=True)
        >>> lib.sort_and_finalize_staged_data("sym")
        >>> lib.read("sym").data
                    col
        2024-01-01    1
        2024-01-02    2
        2024-01-03    3
        2024-01-04    4
        """
        mode = Library._normalize_staged_data_mode(mode)
        compaction_result = self._nvs.version_store.sort_merge(
            symbol,
            normalize_metadata(metadata),
            append=mode == StagedDataFinalizeMethod.APPEND,
            prune_previous_versions=prune_previous_versions,
            delete_staged_data_on_failure=delete_staged_data_on_failure,
            stage_results=stage_results,
        )
        if isinstance(compaction_result, _ae.version_store.VersionedItem):
            return self._nvs._convert_thin_cxx_item_to_python(compaction_result, metadata)
        elif isinstance(compaction_result, List):
            # We expect this to be a list of errors
            check(compaction_result, "List of errors in compaction result should never be empty")
            check(
                all(isinstance(c, KeyNotFoundInStageResultInfo) for c in compaction_result),
                "Compaction errors should always be KeyNotFoundInStageResultInfo",
            )
            raise MissingKeysInStageResultsError(
                "Missing keys during sort and finalize", tokens_with_missing_keys=compaction_result
            )
        else:
            raise RuntimeError(
                f"Unexpected type for compaction_result {type(compaction_result)}. This indicates a bug in ArcticDB."
            )

    def get_staged_symbols(self) -> List[str]:
        """
        Returns all symbols with staged, unfinalized data.

        Returns
        -------
        List[str]
            Symbol names.

        See Also
        --------
        write
            Documentation on the ``staged`` parameter explains the concept of staged data in more detail.
        """
        return self._nvs.list_symbols_with_incomplete_data()

    def read(
        self,
        symbol: str,
        as_of: Optional[AsOf] = None,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None,
        row_range: Optional[Tuple[int, int]] = None,
        columns: Optional[List[str]] = None,
        query_builder: Optional[QueryBuilder] = None,
        lazy: bool = False,
        output_format: Optional[Union[OutputFormat, str]] = None,
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]] = None,
    ) -> Union[VersionedItem, LazyDataFrame]:
        """
        Read data for the named symbol.  Returns a VersionedItem object with a data and metadata element (as passed into
        write).

        Parameters
        ----------
        symbol : str
            Symbol name.

        as_of : AsOf, default=None
            Return the data as it was as of the point in time. ``None`` means that the latest version should be read. The
            various types of this parameter mean:
            - ``int``: specific version number. Negative indexing is supported, with -1 representing the latest version, -2 the version before that, etc.
            - ``str``: snapshot name which contains the version
            - ``datetime.datetime`` : the version of the data that existed ``as_of`` the requested point in time

        date_range: Tuple[Optional[Timestamp], Optional[Timestamp]], default=None
            DateRange to restrict read data to.

            Applicable only for time-indexed Pandas dataframes or series. Returns only the
            part of the data that falls within the given range (inclusive). None on either end leaves that part of the
            range open-ended. Hence specifying ``(None, datetime(2025, 1, 1)`` declares that you wish to read all data up
            to and including 20250101.
            The same effect can be achieved by using the date_range clause of the QueryBuilder class, which will be
            slower, but return data with a smaller memory footprint. See the QueryBuilder.date_range docstring for more
            details.

            Only one of date_range or row_range can be provided.

        row_range : `Optional[Tuple[Optional[int], Optional[int]]]`, default=None
            Row range to read data for. Inclusive of the lower bound, exclusive of the upper bound.
            lib.read(symbol, row_range=(start, end)).data should behave the same as df.iloc[start:end], including in
            the handling of negative start/end values.
            Leaving either element as None leaves that side of the range open-ended. For example (5, None) would
            include everything from the 5th row onwards.
            Only one of date_range or row_range can be provided.

        columns: List[str], default=None
            Applicable only for Pandas data. Determines which columns to return data for. Special values:
            - ``None``: Return a dataframe containing all columns
            - ``[]``: Return a dataframe containing only the index columns

        query_builder: Optional[QueryBuilder], default=None
            A QueryBuilder object to apply to the dataframe before it is returned. For more information see the
            documentation for the QueryBuilder class (``from arcticdb import QueryBuilder; help(QueryBuilder)``).

        lazy: bool, default=False:
            Defer query execution until `collect` is called on the returned `LazyDataFrame` object. See documentation
            on `LazyDataFrame` for more details.

        output_format: Optional[Union[OutputFormat, str]], default=None
            Output format for the returned dataframe.
            If `None`, uses the output format from the `Library` instance.
            See `OutputFormat` documentation for details on available formats.

        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]], default=None
            String column format when using `PYARROW` or `POLARS` output formats.
            If `None`, uses the `arrow_string_format_default` from the `Library` instance.
            See `ArrowOutputStringFormat` documentation for details on available string formats.

        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]], default=None
            Per-column overrides for `arrow_string_format_default`. Keys are column names.

        Returns
        -------
        Union[VersionedItem, LazyDataFrame]
            If lazy is False, VersionedItem object that contains a .data and .metadata element.
            If lazy is True, a LazyDataFrame object on which further querying can be performed prior to collect.

        Examples
        --------

        >>> df = pd.DataFrame({'column': [5,6,7]})
        >>> lib.write("symbol", df, metadata={'my_dictionary': 'is_great'})
        >>> lib.read("symbol").data
           column
        0       5
        1       6
        2       7

        The default read behaviour is also available through subscripting:

        >>> lib["symbol"].data
           column
        0       5
        1       6
        2       7

        Passing an output_format can change the resulting dataframe type. For example, to return a PyArrow table:

        >>> lib.read("symbol", output_format="PYARROW").data
        pyarrow.Table
        column: int64
        ----
        column: [[5,6,7]]
        """
        if lazy:
            return LazyDataFrame(
                self,
                ReadRequest(
                    symbol=symbol,
                    as_of=as_of,
                    date_range=date_range,
                    row_range=row_range,
                    columns=columns,
                    query_builder=query_builder,
                    output_format=output_format,
                    arrow_string_format_default=arrow_string_format_default,
                    arrow_string_format_per_column=arrow_string_format_per_column,
                ),
            )
        else:
            return self._nvs.read(
                symbol=symbol,
                as_of=as_of,
                date_range=date_range,
                row_range=row_range,
                columns=columns,
                query_builder=query_builder,
                output_format=output_format,
                arrow_string_format_default=arrow_string_format_default,
                arrow_string_format_per_column=arrow_string_format_per_column,
                implement_read_index=True,
                iterate_snapshots_if_tombstoned=False,
            )

    def _read_as_record_batch_reader(
        self,
        symbol: str,
        as_of: Optional[AsOf] = None,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None,
        row_range: Optional[Tuple[int, int]] = None,
        columns: Optional[List[str]] = None,
        query_builder: Optional[QueryBuilder] = None,
    ) -> "ArcticRecordBatchReader":
        """
        Read data and return a lazy Arrow RecordBatchReader that streams data segment-by-segment.

        This is an internal method used by sql() and duckdb() for memory-efficient streaming.
        """
        from arcticdb.version_store.duckdb import ArcticRecordBatchReader

        cpp_iterator = self._nvs.read_as_record_batch_iterator(
            symbol=symbol,
            as_of=as_of,
            date_range=date_range,
            row_range=row_range,
            columns=columns,
            query_builder=query_builder,
        )

        return ArcticRecordBatchReader(cpp_iterator)

    def sql(
        self,
        query: str,
        as_of: Optional[Union[AsOf, Dict[str, AsOf]]] = None,
        output_format: Optional[Union[OutputFormat, str]] = None,
    ):
        """
        Execute SQL query on ArcticDB symbols using DuckDB.

        Symbols referenced in the query (via FROM or JOIN clauses) are automatically
        registered as tables in DuckDB. Data is streamed segment-by-segment for
        memory efficiency.

        Where possible, column selections, WHERE filters, date range filters, and
        LIMIT clauses are pushed down to ArcticDB's storage engine so that only
        the required data is read from storage.

        Parameters
        ----------
        query : str
            SQL query. Reference ArcticDB symbols as table names.
            Example: ``"SELECT col1, SUM(col2) FROM my_symbol WHERE col1 > 100 GROUP BY col1"``
        as_of : AsOf or Dict[str, AsOf], default=None
            Version to query. Can be:

            - A single value (int, str, or datetime) applied to **all** symbols in the query.
            - A dict mapping symbol names to individual versions, allowing different symbols
              to be read at different points in time. Symbols not present in the dict use
              the latest version.

            See `read()` for details on version specification.
        output_format : OutputFormat, default=None
            Format for the result. Defaults to PANDAS.
            Options: OutputFormat.PANDAS, OutputFormat.PYARROW, OutputFormat.POLARS

        Returns
        -------
        pandas.DataFrame, pyarrow.Table, or polars.DataFrame
            Query result in the requested format.

        Examples
        --------
        Simple filter and aggregation:

        >>> df = lib.sql('''
        ...     SELECT ticker, AVG(price) as avg_price
        ...     FROM trades
        ...     WHERE date > '2024-01-01'
        ...     GROUP BY ticker
        ... ''')

        Query specific versions per symbol:

        >>> df = lib.sql(
        ...     "SELECT t.ticker, p.close FROM trades t JOIN prices p ON t.ticker = p.ticker",
        ...     as_of={"trades": 3, "prices": 0}
        ... )

        Get result as Arrow table:

        >>> table = lib.sql(
        ...     "SELECT * FROM prices WHERE price > 100",
        ...     output_format="pyarrow"
        ... )

        Raises
        ------
        ImportError
            If duckdb package is not installed.
        ValueError
            If no symbols could be extracted from the query.

        Notes
        -----
        - DuckDB is an optional dependency. Install with: ``pip install duckdb``
        - For complex queries with multiple symbols or custom table aliases,
          use `duckdb()` instead.
        - Data is processed using streaming Arrow record batches for memory efficiency.

        See Also
        --------
        explain : Inspect which pushdown optimizations apply to a query.
        duckdb_register : Register symbols into an external DuckDB connection.
        duckdb : Context manager for complex multi-symbol SQL queries.
        """
        from arcticdb.version_store.duckdb.duckdb import _check_duckdb_available
        from arcticdb.version_store.duckdb.pushdown import extract_pushdown_from_sql, is_table_discovery_query

        duckdb = _check_duckdb_available()

        # Check if this is a table discovery query (SHOW TABLES, SHOW ALL TABLES)
        if is_table_discovery_query(query):
            # For table discovery queries, register all symbols from the library
            symbols = self.list_symbols()
            pushdown_by_table = {}
        else:
            # Extract symbol names and pushdown info from SQL AST in a single parse
            pushdown_by_table, symbols = extract_pushdown_from_sql(query)

        # Resolve SQL table names to actual ArcticDB symbol names (case-insensitive).
        # SQL identifiers are conventionally case-insensitive, but ArcticDB symbols are
        # case-sensitive. We build a lookup map to find the real symbol name regardless
        # of the case used in SQL.
        library_symbols = self.list_symbols()
        symbol_lookup = {s.lower(): s for s in library_symbols}

        # Create DuckDB connection and register data with pushdown applied
        conn = None
        try:
            conn = duckdb.connect(":memory:")
            # Resolve as_of: if dict, look up per-symbol; if scalar, apply globally
            as_of_is_dict = isinstance(as_of, dict)

            for sql_name in symbols:
                # Resolve: exact match first, then case-insensitive
                if sql_name in library_symbols:
                    real_symbol = sql_name
                elif sql_name.lower() in symbol_lookup:
                    real_symbol = symbol_lookup[sql_name.lower()]
                else:
                    real_symbol = sql_name  # Let ArcticDB produce a clear "not found" error

                # Resolve per-symbol as_of from dict, falling back to None (latest)
                if as_of_is_dict:
                    if real_symbol in as_of:
                        symbol_as_of = as_of[real_symbol]
                    elif sql_name in as_of:
                        symbol_as_of = as_of[sql_name]
                    else:
                        symbol_as_of = None
                else:
                    symbol_as_of = as_of

                pushdown = pushdown_by_table.get(sql_name)
                if pushdown:
                    reader = self._read_as_record_batch_reader(
                        real_symbol,
                        as_of=symbol_as_of,
                        columns=pushdown.columns,
                        date_range=pushdown.date_range,
                        query_builder=pushdown.query_builder,
                    )
                else:
                    reader = self._read_as_record_batch_reader(real_symbol, as_of=symbol_as_of)

                # Register under the SQL name so DuckDB can find it from the query
                conn.register(sql_name, reader.to_pyarrow_reader())

            # Execute query and get Arrow result
            result_arrow = conn.execute(query).fetch_arrow_table()

            # Convert to requested format
            if output_format is None:
                output_fmt_str = OutputFormat.PANDAS.lower()
            elif isinstance(output_format, OutputFormat):
                output_fmt_str = output_format.lower()
            else:
                output_fmt_str = str(output_format).lower()

            if output_fmt_str == OutputFormat.PYARROW.lower():
                data = result_arrow
            elif output_fmt_str == OutputFormat.POLARS.lower():
                import polars as pl

                data = pl.from_arrow(result_arrow)
            else:
                # Default to pandas
                data = result_arrow.to_pandas()

            return data

        finally:
            if conn is not None:
                conn.close()

    def explain(self, query: str) -> dict:
        """
        Explain which pushdown optimizations would be applied to a SQL query.

        Parses the SQL query and reports which operations can be pushed down
        to ArcticDB's storage engine (column projection, filters, date ranges,
        LIMIT). Does not execute the query or read any data.

        Parameters
        ----------
        query : str
            SQL query to analyze.

        Returns
        -------
        dict
            Dictionary describing the pushdown optimizations, with keys:

            - ``query`` (str): The original query
            - ``symbols`` (list[str]): Symbols referenced in the query
            - ``columns_pushed_down`` (list[str]): Columns selected at storage level
            - ``filter_pushed_down`` (bool): Whether WHERE filters are pushed down
            - ``date_range_pushed_down`` (bool): Whether date range filtering is pushed down
            - ``limit_pushed_down`` (int): LIMIT value pushed down to storage

            Only keys with active pushdowns are included (except ``query`` and ``symbols``
            which are always present).

        Examples
        --------
        >>> info = lib.explain("SELECT price FROM trades WHERE price > 100")
        >>> print(info)
        {'query': '...', 'symbols': ['trades'], 'columns_pushed_down': ['price'], 'filter_pushed_down': True}

        See Also
        --------
        sql : Execute the query and return results.
        """
        from arcticdb.version_store.duckdb.duckdb import _check_duckdb_available
        from arcticdb.version_store.duckdb.pushdown import extract_pushdown_from_sql

        _check_duckdb_available()

        pushdown_by_table, symbols = extract_pushdown_from_sql(query)

        info = {"query": query, "symbols": list(symbols)}
        all_columns_pushed = []
        any_filter_pushed = False
        any_date_range_pushed = False
        limit_pushed = None

        for symbol, pushdown in pushdown_by_table.items():
            if pushdown.columns_pushed_down:
                all_columns_pushed.extend(pushdown.columns_pushed_down)
            if pushdown.filter_pushed_down:
                any_filter_pushed = True
            if pushdown.date_range_pushed_down:
                any_date_range_pushed = True
            if pushdown.limit_pushed_down:
                limit_pushed = pushdown.limit_pushed_down

        if all_columns_pushed:
            info["columns_pushed_down"] = list(set(all_columns_pushed))
        if any_filter_pushed:
            info["filter_pushed_down"] = True
        if limit_pushed:
            info["limit_pushed_down"] = limit_pushed
        if any_date_range_pushed:
            info["date_range_pushed_down"] = True

        return info

    def duckdb(self, connection: Any = None) -> "DuckDBContext":
        """
        Create a DuckDB context for complex multi-symbol SQL queries.

        The context manager allows explicit symbol registration with custom
        aliases, filters, and versions. Use this for JOINs across multiple
        symbols or when you need fine-grained control over the query.

        Parameters
        ----------
        connection : duckdb.DuckDBPyConnection, optional
            External DuckDB connection to use. If provided, ArcticDB will register
            symbols into this connection but will NOT close it when the context exits.
            This allows joining ArcticDB data with data from other sources (Parquet
            files, CSV, other databases) that are already registered in the connection.
            If not provided, a new in-memory connection is created and closed on exit.

        Returns
        -------
        DuckDBContext
            Context manager for SQL queries.

        Examples
        --------
        Basic usage with ArcticDB symbols only:

        >>> with lib.duckdb() as ddb:
        ...     # Register symbols with different versions/filters
        ...     ddb.register_symbol("trades", date_range=(start, end))
        ...     ddb.register_symbol("prices", as_of=-1, alias="latest_prices")
        ...
        ...     # Execute JOIN query
        ...     result = ddb.query('''
        ...         SELECT t.ticker, t.quantity * p.price as notional
        ...         FROM trades t
        ...         JOIN latest_prices p ON t.ticker = p.ticker
        ...         WHERE t.quantity > 1000
        ...     ''')

        Join ArcticDB data with external data sources:

        >>> import duckdb
        >>> # Create connection with external data
        >>> conn = duckdb.connect()
        >>> conn.execute("CREATE TABLE benchmarks AS SELECT * FROM 'benchmarks.parquet'")
        >>>
        >>> # Join ArcticDB data with external tables
        >>> with lib.duckdb(connection=conn) as ddb:
        ...     ddb.register_symbol("portfolio_returns")
        ...     result = ddb.query('''
        ...         SELECT r.date, r.ticker, r.return - b.return as alpha
        ...         FROM portfolio_returns r
        ...         JOIN benchmarks b ON r.date = b.date
        ...     ''')
        >>>
        >>> # Connection is still open - ArcticDB did not close it
        >>> conn.execute("SELECT COUNT(*) FROM benchmarks")

        >>> # Method chaining
        >>> with lib.duckdb() as ddb:
        ...     result = (ddb
        ...         .register_symbol("trades")
        ...         .register_symbol("prices")
        ...         .query("SELECT * FROM trades JOIN prices USING (ticker)"))

        Raises
        ------
        ImportError
            If duckdb package is not installed.

        Notes
        -----
        - DuckDB is an optional dependency. Install with: pip install duckdb
        - Data is streamed lazily; symbols are not fully loaded until queried.
        - When no connection is provided, a new in-memory connection is created
          and automatically closed when exiting the context.
        - When an external connection is provided, ArcticDB will NOT close it,
          allowing continued use after the context exits.

        See Also
        --------
        sql : Simple SQL queries on single symbols.
        """
        from arcticdb.version_store.duckdb import DuckDBContext

        return DuckDBContext(self, connection=connection)

    def duckdb_register(
        self,
        conn,
        symbols: Optional[List[str]] = None,
        as_of: Optional[AsOf] = None,
    ) -> List[str]:
        """
        Register ArcticDB symbols as tables in a DuckDB connection.

        Each symbol is read as an Arrow table and registered into the connection,
        making it queryable via standard DuckDB SQL. The data is materialized in
        memory so that tables can be queried multiple times.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection to register tables into.
        symbols : list of str, optional
            Symbols to register. If None, registers all symbols from ``list_symbols()``.
        as_of : AsOf, optional
            Version to read for all symbols. See `read()` for details.

        Returns
        -------
        list of str
            Names of registered symbols.

        Examples
        --------
        Register all symbols:

        >>> import duckdb
        >>> conn = duckdb.connect()
        >>> lib.duckdb_register(conn)
        ['trades', 'prices']
        >>> conn.sql("SELECT * FROM trades WHERE price > 100").df()

        Register specific symbols:

        >>> lib.duckdb_register(conn, symbols=["trades", "prices"])
        >>> conn.sql("SHOW TABLES").df()

        Use with standard DuckDB features:

        >>> lib.duckdb_register(conn)
        >>> conn.sql("DESCRIBE trades").df()
        >>> conn.sql("SELECT * FROM trades LIMIT 10").show()

        See Also
        --------
        sql : One-shot SQL queries with automatic pushdown optimization.
        duckdb : Context manager for streaming queries with fine-grained control.
        """
        import pyarrow as pa

        from arcticdb.version_store.duckdb.duckdb import _check_duckdb_available, _BaseDuckDBContext

        _check_duckdb_available()
        _BaseDuckDBContext._validate_external_connection(conn)

        if symbols is None:
            symbols = self.list_symbols()

        registered = []
        for symbol in symbols:
            arrow_table = self.read(symbol, as_of=as_of).data
            if not isinstance(arrow_table, pa.Table):
                # read() returns pandas by default, convert to Arrow
                arrow_table = pa.Table.from_pandas(arrow_table)
            conn.register(symbol, arrow_table)
            registered.append(symbol)

        return registered

    def read_batch(
        self,
        symbols: List[Union[str, ReadRequest]],
        query_builder: Optional[QueryBuilder] = None,
        lazy: bool = False,
        output_format: Optional[Union[OutputFormat, str]] = None,
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]] = None,
    ) -> Union[List[Union[VersionedItem, DataError]], LazyDataFrameCollection]:
        """
        Reads multiple symbols.

        Parameters
        ----------
        symbols : List[Union[str, ReadRequest]]
            List of symbols to read.

        query_builder: Optional[QueryBuilder], default=None
            A single QueryBuilder to apply to all the dataframes before they are returned. If this argument is passed
            then none of the ``symbols`` may have their own query_builder specified in their request.

        lazy: bool, default=False:
            Defer query execution until `collect` is called on the returned `LazyDataFrameCollection` object. See
            documentation on `LazyDataFrameCollection` for more details.

        output_format: Optional[Union[OutputFormat, str]], default=None
            Output format for the returned dataframes.
            If `None`, uses the output format from the `Library` instance.
            See `OutputFormat` documentation for details on available formats.

        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]], default=None
            String column format when using `PYARROW` or `POLARS` output formats.
            Serves as the default for the entire batch. String format settings in individual `ReadRequest` objects
            override this batch-level setting.
            If `None`, uses the `arrow_string_format_default` from the `Library` instance.
            See `ArrowOutputStringFormat` documentation for details on available string formats.

        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]], default=None
            Per-column overrides for `arrow_string_format_default`. Keys are column names.
            Only applied to symbols that don't have `arrow_string_format_per_column` set in their `ReadRequest`.

        Returns
        -------
        Union[List[Union[VersionedItem, DataError]], LazyDataFrameCollection]
            If lazy is False:
            A list of the read results, whose i-th element corresponds to the i-th element of the ``symbols`` parameter.
            If the specified version does not exist, a DataError object is returned, with symbol, version_request_type,
            version_request_data properties, error_code, error_category, and exception_string properties. If a key error or
            any other internal exception occurs, the same DataError object is also returned.
            If lazy is True:
            A LazyDataFrameCollection object on which further querying can be performed prior to collection.

        Raises
        ------
        ArcticInvalidApiUsageException
            If kwarg query_builder and per-symbol query builders both used.

        Examples
        --------
        >>> lib.write("s1", pd.DataFrame())
        >>> lib.write("s2", pd.DataFrame({"col": [1, 2, 3]}))
        >>> lib.write("s2", pd.DataFrame(), prune_previous_versions=False)
        >>> lib.write("s3", pd.DataFrame())
        >>> batch = lib.read_batch(["s1", adb.ReadRequest("s2", as_of=0), "s3", adb.ReadRequest("s2", as_of=1000)])
        >>> batch[0].data.empty
        True
        >>> batch[1].data.empty
        False
        >>> batch[2].data.empty
        True
        >>> batch[3].symbol
        "s2"
        >>> isinstance(batch[3], adb.DataError)
        True
        >>> batch[3].version_request_type
        VersionRequestType.SPECIFIC
        >>> batch[3].version_request_data
        1000
        >>> batch[3].error_code
        ErrorCode.E_NO_SUCH_VERSION
        >>> batch[3].error_category
        ErrorCategory.MISSING_DATA

        See Also
        --------
        read
        """
        symbol_strings = []
        as_ofs = []
        date_ranges = []
        row_ranges = []
        columns = []
        query_builders = []
        per_symbol_arrow_string_format_default = []
        per_symbol_arrow_string_format_per_column = []

        def handle_read_request(s_):
            symbol_strings.append(s_.symbol)
            as_ofs.append(s_.as_of)
            date_ranges.append(s_.date_range)
            row_ranges.append(s_.row_range)
            columns.append(s_.columns)

            if s_.query_builder is not None and query_builder is not None:
                raise ArcticInvalidApiUsageException(
                    "kwarg query_builder and per-symbol query builders cannot "
                    f"both be used but {s_} had its own query_builder specified."
                )
            else:
                query_builders.append(s_.query_builder)

            per_symbol_arrow_string_format_default.append(s_.arrow_string_format_default)
            per_symbol_arrow_string_format_per_column.append(s_.arrow_string_format_per_column)

        def handle_symbol(s_):
            symbol_strings.append(s_)
            for l_ in (
                as_ofs,
                date_ranges,
                row_ranges,
                columns,
                query_builders,
                per_symbol_arrow_string_format_default,
                per_symbol_arrow_string_format_per_column,
            ):
                l_.append(None)

        for s in symbols:
            if isinstance(s, str):
                handle_symbol(s)
            elif isinstance(s, ReadRequest):
                handle_read_request(s)
            else:
                raise ArcticInvalidApiUsageException(
                    f"Unsupported item in the symbols argument s=[{s}] type(s)=[{type(s)}]. Only [str] and"
                    " [ReadRequest] are supported."
                )
        throw_on_error = False
        if lazy:
            lazy_dataframes = []
            for idx in range(len(symbol_strings)):
                q = copy.deepcopy(query_builder)
                if q is None and len(query_builders):
                    q = copy.deepcopy(query_builders[idx])
                lazy_dataframes.append(
                    LazyDataFrame(
                        self,
                        ReadRequest(
                            symbol=symbol_strings[idx],
                            as_of=as_ofs[idx],
                            date_range=date_ranges[idx],
                            row_range=row_ranges[idx],
                            columns=columns[idx],
                            query_builder=q,
                            output_format=output_format,
                            arrow_string_format_default=per_symbol_arrow_string_format_default[idx]
                            or arrow_string_format_default,
                            arrow_string_format_per_column=per_symbol_arrow_string_format_per_column[idx]
                            or arrow_string_format_per_column,
                        ),
                    )
                )
            return LazyDataFrameCollection(
                lazy_dataframes,
                arrow_string_format_default=arrow_string_format_default,
                arrow_string_format_per_column=arrow_string_format_per_column,
            )
        else:
            return self._nvs._batch_read_to_versioned_items(
                symbol_strings,
                as_ofs,
                date_ranges,
                row_ranges,
                columns,
                query_builder or query_builders,
                throw_on_error,
                implement_read_index=True,
                iterate_snapshots_if_tombstoned=False,
                output_format=output_format,
                arrow_string_format_default=arrow_string_format_default,
                arrow_string_format_per_column=arrow_string_format_per_column,
                per_symbol_arrow_string_format_default=per_symbol_arrow_string_format_default,
                per_symbol_arrow_string_format_per_column=per_symbol_arrow_string_format_per_column,
            )

    def read_batch_and_join(
        self,
        symbols: List[ReadRequest],
        query_builder: QueryBuilder,
        output_format: Optional[Union[OutputFormat, str]] = None,
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]] = None,
    ) -> VersionedItemWithJoin:
        """
        Reads multiple symbols in a batch, and then joins them together using the first clause in the `query_builder`
        argument. If there are subsequent clauses in the `query_builder` argument, then these are applied to the joined
        data.

        Parameters
        ----------
        symbols : List[Union[str, ReadRequest]]
            List of symbols to read.

        query_builder: QueryBuilder
            The first clause must be a multi-symbol join, such as `concat`. Any subsequent clauses must work on
            individual dataframes, and will be applied to the joined data.

        output_format: Optional[Union[OutputFormat, str]], default=None
            Output format for the returned joined dataframe.
            If `None`, uses the output format from the `Library` instance.
            See `OutputFormat` documentation for details on available formats.

        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]], default=None
            String column format when using `PYARROW` or `POLARS` output formats.
            If `None`, uses the `arrow_string_format_default` from the `Library` instance.
            See `ArrowOutputStringFormat` documentation for details on available string formats.

        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]], default=None
            Per-column overrides for `arrow_string_format_default`. Keys are column names.

        Returns
        -------
        VersionedItemWithJoin
            Contains a .data field with the joined together data, and a list of VersionedItem objects describing the
            version number, metadata, etc., of the symbols that were joined together.

        Raises
        ------
        UserInputException
            * If the first clause in `query_builder` is not a multi-symbol join
            * If any subsequent clauses in `query_builder` are not single-symbol clauses
            * If any of the specified symbols are recursively normalized
        MissingDataException
            * If a symbol or the version of symbol specified in as_ofs does not exist or has been deleted
        SchemaException
            * If the schema of symbols to be joined are incompatible. Examples of incompatible schemas include:
                * Trying to join a Series to a DataFrame
                * Different index types, including MultiIndexes with different numbers of levels
                * Incompatible column types e.g. joining a string column to an integer column

        Examples
        --------
        Join 2 symbols together without any pre or post processing.

        >>> df0 = pd.DataFrame(
            {
                "col1": [0.5],
                "col2": [1],
            },
            index=[pd.Timestamp("2025-01-01")],
        )
        >>> df1 = pd.DataFrame(
            {
                "col3": ["hello"],
                "col2": [2],
            },
            index=[pd.Timestamp("2025-01-02")],
        )
        >>> q = adb.QueryBuilder()
        >>> q = q.concat("outer")
        >>> lib.write("symbol0", df0)
        >>> lib.write("symbol1", df1)
        >>> lib.read_batch_and_join(["symbol0", "symbol1"], query_builder=q).data

                                   col1     col2     col3
            2025-01-01 00:00:00     0.5        1     None
            2025-01-02 00:00:00     NaN        2  "hello"

        >>> q = adb.QueryBuilder()
        >>> q = q.concat("inner")
        >>> lib.read_batch_and_join(["symbol0", "symbol1"], query_builder=q).data

                                   col2
            2025-01-01 00:00:00       1
            2025-01-02 00:00:00       2
        """
        symbol_strings = []
        as_ofs = []
        date_ranges = []
        row_ranges = []
        columns = []
        per_symbol_query_builders = []

        def handle_read_request(s_):
            symbol_strings.append(s_.symbol)
            as_ofs.append(s_.as_of)
            date_ranges.append(s_.date_range)
            row_ranges.append(s_.row_range)
            columns.append(s_.columns)
            per_symbol_query_builders.append(s_.query_builder)

        def handle_symbol(s_):
            symbol_strings.append(s_)
            for l_ in (as_ofs, date_ranges, row_ranges, columns, per_symbol_query_builders):
                l_.append(None)

        for s in symbols:
            if isinstance(s, str):
                handle_symbol(s)
            elif isinstance(s, ReadRequest):
                handle_read_request(s)
            else:
                raise ArcticInvalidApiUsageException(
                    f"Unsupported item in the symbols argument s=[{s}] type(s)=[{type(s)}]. Only [str] and"
                    " [ReadRequest] are supported."
                )

        return self._nvs.batch_read_and_join(
            symbol_strings,
            query_builder,
            as_ofs,
            date_ranges,
            row_ranges,
            columns,
            per_symbol_query_builders,
            implement_read_index=True,
            iterate_snapshots_if_tombstoned=False,
            output_format=output_format,
            arrow_string_format_default=arrow_string_format_default,
            arrow_string_format_per_column=arrow_string_format_per_column,
        )

    def read_metadata(self, symbol: str, as_of: Optional[AsOf] = None) -> VersionedItem:
        """
        Return the metadata saved for a symbol.  This method is faster than read as it only loads the metadata, not the
        data itself.

        Parameters
        ----------
        symbol
            Symbol name
        as_of : AsOf, default=None
            Return the metadata as it was as of the point in time. See documentation on `read` for documentation on
            the different forms this parameter can take.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the affected symbol in the store. The data attribute
            will be None.
        """
        return self._nvs.read_metadata(symbol, as_of, iterate_snapshots_if_tombstoned=False)

    def read_metadata_batch(self, symbols: List[Union[str, ReadInfoRequest]]) -> List[Union[VersionedItem, DataError]]:
        """
        Reads the metadata of multiple symbols.

        Parameters
        ----------
        symbols : List[Union[str, ReadInfoRequest]]
            List of symbols to read metadata.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            A list of the read metadata results, whose i-th element corresponds to the i-th element of the ``symbols`` parameter.
            A VersionedItem object with the metadata field set as None will be returned if the requested version of the
            symbol exists but there is no metadata
            If the specified version does not exist, a DataError object is returned, with symbol, version_request_type,
            version_request_data properties, error_code, error_category, and exception_string properties. If a key error or
            any other internal exception occurs, the same DataError object is also returned.

        See Also
        --------
        read_metadata
        """
        symbol_strings, as_ofs = self.parse_list_of_symbols(symbols)

        include_errors_and_none_meta = True
        return self._nvs._batch_read_metadata_to_versioned_items(symbol_strings, as_ofs, include_errors_and_none_meta)

    def write_metadata(
        self,
        symbol: str,
        metadata: Any,
        prune_previous_versions: bool = False,
    ) -> VersionedItem:
        """
        Write metadata under the specified symbol name to this library. The data will remain unchanged.
        A new version will be created.

        If the symbol is missing, it causes a write with empty data (None, pickled, can't append) and the supplied
        metadata.

        This method should be faster than `write` as it involves no data segment read/write operations.

        See the Metadata section of our online documentation for details about how metadata is persisted and caveats.

        Parameters
        ----------
        symbol
            Symbol name for the item
        metadata
            Metadata to persist along with the symbol
        prune_previous_versions : bool, default=False
            Removes previous (non-snapshotted) versions from the database. Note that metadata is versioned alongside the
            data it is referring to, and so this operation removes old versions of data as well as metadata.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the affected symbol in the store.
        """
        return self._nvs.write_metadata(symbol, metadata, prune_previous_version=prune_previous_versions)

    def write_metadata_batch(
        self,
        write_metadata_payloads: List[WriteMetadataPayload],
        prune_previous_versions: bool = False,
    ) -> List[Union[VersionedItem, DataError]]:
        """
        Write metadata to multiple symbols in a batch fashion. This is more efficient than making multiple `write_metadata` calls
        in succession as some constant-time operations can be executed only once rather than once for each element of
        `write_metadata_payloads`.
        Note that this isn't an atomic operation - it's possible for the metadata for one symbol to be fully written and
        readable before another symbol.

        See the Metadata section of our online documentation for details about how metadata is persisted and caveats.

        Parameters
        ----------
        write_metadata_payloads : `List[WriteMetadataPayload]`
            Symbols and their corresponding metadata. There must not be any duplicate symbols in `payload`.
        prune_previous_versions : bool, default=False
            Removes previous (non-snapshotted) versions from the database. Note that metadata is versioned alongside the
            data it is referring to, and so this operation removes old versions of data as well as metadata.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            List of versioned items. The data attribute will be None for each versioned item.
            i-th entry corresponds to i-th element of `write_metadata_payloads`. Each result correspond to
            a structure containing metadata and version number of the affected symbol in the store.
            If any internal exception is raised, a DataError object is returned, with symbol,
            error_code, error_category, and exception_string properties.

        Raises
        ------
        ArcticDuplicateSymbolsInBatchException
            When duplicate symbols appear in write_metadata_payloads.

        Examples
        --------

        Writing a simple batch:
        >>> payload_1 = adb.WriteMetadataPayload("symbol_1", {'the': 'metadata_1'})
        >>> payload_2 = adb.WriteMetadataPayload("symbol_2", {'the': 'metadata_2'})
        >>> items = lib.write_metadata_batch([payload_1, payload_2])
        >>> lib.read_metadata("symbol_1")
        {'the': 'metadata_1'}
        >>> lib.read_metadata("symbol_2")
        {'the': 'metadata_2'}
        """

        self._raise_if_duplicate_symbols_in_batch(write_metadata_payloads)
        throw_on_error = False
        return self._nvs._batch_write_metadata_to_versioned_items(
            [p.symbol for p in write_metadata_payloads],
            [p.metadata for p in write_metadata_payloads],
            prune_previous_version=prune_previous_versions,
            throw_on_error=throw_on_error,
        )

    def snapshot(
        self,
        snapshot_name: str,
        metadata: Any = None,
        skip_symbols: Optional[List[str]] = None,
        versions: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Creates a named snapshot of the data within a library.

        By default, the latest version of every symbol that has not been deleted will be contained within the snapshot.
        You can change this behaviour with either ``versions`` (an allow-list) or with ``skip_symbols`` (a deny-list).
        Concurrent writes with prune previous versions set while the snapshot is being taken can potentially lead to
        corruption of the affected symbols in the snapshot.

        The symbols and versions contained within the snapshot will persist regardless of new symbols and versions
        being written to the library afterwards. If a version or symbol referenced in a snapshot is deleted then the
        underlying data will be preserved to ensure the snapshot is still accessible. Only once all referencing
        snapshots have been removed will the underlying data be removed as well.

        At most one of ``skip_symbols`` and ``versions`` may be truthy.

        Parameters
        ----------
        snapshot_name
            Name of the snapshot.
        metadata : Any, default=None
            Optional metadata to persist along with the snapshot.
        skip_symbols : List[str], default=None
            Optional symbols to be excluded from the snapshot.
        versions: Dict[str, int], default=None
            Optional dictionary of versions of symbols to snapshot. For example `versions={"a": 2, "b": 3}` will
            snapshot version 2 of symbol "a" and version 3 of symbol "b".

        Raises
        ------
        InternalException
            If a snapshot already exists with ``snapshot_name``. You must explicitly delete the pre-existing snapshot.
        MissingDataException
            If a symbol or the version of symbol specified in versions does not exist or has been deleted in the library,
            or, the library has no symbol.
        """
        # We deliberately check the snapshot name only with the v2 API to avoid disruption to legacy users on the v1 API
        self._nvs.version_store.verify_snapshot(snapshot_name)
        self._nvs.snapshot(snap_name=snapshot_name, metadata=metadata, skip_symbols=skip_symbols, versions=versions)

    def delete(self, symbol: str, versions: Optional[Union[int, Iterable[int]]] = None) -> None:
        """
        Delete all versions of the symbol from the library, unless ``version`` is specified, in which case only those
        versions are deleted.

        This may not actually delete the underlying data if a snapshot still references the version. See ``snapshot`` for
        more detail.

        Note that this may require data to be removed from the underlying storage which can be slow.

        This method does not remove any staged data, use ``delete_staged_data`` for that.

        If no symbol called ``symbol`` exists then this is a no-op. In particular this method does not raise in this case.

        Parameters
        ----------
        symbol
            Symbol to delete.

        versions
            Version or versions of symbol to delete. If ``None`` then all versions will be deleted.
        """
        if versions is None:
            self._nvs.delete(symbol)
            return

        if isinstance(versions, int):
            versions = (versions,)

        self._nvs.delete_versions(symbol, versions)

    def delete_batch(self, delete_requests: List[Union[str, DeleteRequest]]) -> List[Optional[DataError]]:
        """
        Delete multiple symbols in a batch fashion.

        Parameters
        ----------
        delete_requests : List[Union[str, DeleteRequest]]
            List of symbols to delete. Can be either:
            - String symbols (delete all versions of the symbol)
            - DeleteRequest objects (delete specific versions of the symbol, must have at least one version)

        Returns
        -------
        List[DataError]
            List of DataError objects, one for each symbol that was not deleted due to an error.
            If the symbol was already deleted, there will be no error, just a warning.
        """
        symbols = []
        versions = []

        for request in delete_requests:
            if isinstance(request, str):
                # Delete all versions of the symbol
                symbols.append(request)
                versions.append([])  # Empty list means delete all versions
            elif isinstance(request, DeleteRequest):
                # Delete specific versions of the symbol
                symbols.append(request.symbol)
                versions.append(request.version_ids)
            else:
                raise ArcticInvalidApiUsageException(
                    f"Unsupported item in the delete_requests argument request=[{request}] type(request)=[{type(request)}]. "
                    "Only [str] and [DeleteRequest] are supported."
                )

        return self._nvs.version_store.batch_delete(symbols, versions)

    def prune_previous_versions(self, symbol) -> None:
        """Removes all (non-snapshotted) versions from the database for the given symbol, except the latest.

        Parameters
        ----------
        symbol : `str`
            Symbol name to prune.
        """
        self._nvs.prune_previous_versions(symbol)

    def delete_data_in_range(
        self,
        symbol: str,
        date_range: Tuple[Optional[Timestamp], Optional[Timestamp]],
        prune_previous_versions: bool = False,
    ) -> None:
        """Delete data within the given date range, creating a new version of ``symbol``.

        The existing symbol version must be timeseries-indexed.

        Parameters
        ----------
        symbol
            Symbol name.
        date_range
            The date range in which to delete data. Leaving any part of the tuple as None leaves that part of the range
            open ended.
        prune_previous_versions : bool, default=False
            Removes previous (non-snapshotted) versions from the database.

        Examples
        --------
        >>> df = pd.DataFrame({"column": [5, 6, 7, 8]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
        >>> lib.write("symbol", df)
        >>> lib.delete_data_in_range("symbol", date_range=(datetime.datetime(2018, 1, 1), datetime.datetime(2018, 1, 2)))
        >>> lib["symbol"].version
        1
        >>> lib["symbol"].data
                        column
        2018-01-03       7
        2018-01-04       8
        """
        if date_range is None:
            raise ArcticInvalidApiUsageException("date_range must be given but was None")
        self._nvs.delete(symbol, date_range=date_range, prune_previous_version=prune_previous_versions)

    def delete_snapshot(self, snapshot_name: str) -> None:
        """
        Delete a named snapshot. This may take time if the given snapshot is the last reference to the underlying
        symbol(s) as the underlying data will be removed as well.

        Parameters
        ----------
        snapshot_name
            The snapshot name to delete.

        Raises
        ------
        Exception
            If the named snapshot does not exist.
        """
        return self._nvs.delete_snapshot(snapshot_name)

    def list_symbols(self, snapshot_name: Optional[str] = None, regex: Optional[str] = None) -> List[str]:
        """
        Return the symbols in this library.

        Parameters
        ----------
        regex
            If passed, returns only the symbols which match the regex.

        snapshot_name
            Return the symbols available under the snapshot. If None then considers symbols that are live in the
            library as of the current time.

        Returns
        -------
        List[str]
            Symbols in the library.
        """
        return self._nvs.list_symbols(snapshot=snapshot_name, regex=regex)

    def has_symbol(self, symbol: str, as_of: Optional[AsOf] = None) -> bool:
        """
        Whether this library contains the given symbol.

        Parameters
        ----------
        symbol
            Symbol name for the item
        as_of : AsOf, default=None
            Return the data as it was as_of the point in time. See `read` for more documentation. If absent then
            considers symbols that are live in the library as of the current time.

        Returns
        -------
        bool
            True if the symbol is in the library, False otherwise.

        Examples
        --------
        >>> lib.write("symbol", pd.DataFrame())
        >>> lib.has_symbol("symbol")
        True
        >>> lib.has_symbol("another_symbol")
        False

        The __contains__ operator also checks whether a symbol exists in this library as of now:

        >>> "symbol" in lib
        True
        >>> "another_symbol" in lib
        False
        """
        return self._nvs.has_symbol(symbol, as_of=as_of)

    def list_snapshots(self, load_metadata: Optional[bool] = True) -> Union[List[str], Dict[str, Any]]:
        """
        List the snapshots in the library.

        Parameters
        ----------
        load_metadata : `Optional[bool]`, default=True
            Load the snapshot metadata. May be slow so opt for false if you don't need it.

        Returns
        -------
        Union[List[str], Dict[str, Any]]
            Snapshots in the library. Returns a list of snapshot names if load_metadata is False, otherwise returns a
            dictionary where keys are snapshot names and values are metadata associated with that snapshot.
        """
        result = self._nvs.list_snapshots(load_metadata)
        return result if load_metadata else list(result.keys())

    def list_versions(
        self,
        symbol: Optional[str] = None,
        snapshot: Optional[str] = None,
        latest_only: bool = False,
        skip_snapshots: bool = False,
    ) -> Dict[SymbolVersion, VersionInfo]:
        """
        Get the versions in this library, filtered by the passed in parameters.

        Parameters
        ----------
        symbol
            Symbol to return versions for.  If None returns versions across all symbols in the library.
        snapshot
            Only return the versions contained in the named snapshot.

            Limitation: If this is specified then every VersionInfo in the result will have `deleted=False`. This may
            change in future.
        latest_only : bool, default=False
            Only include the latest version for each returned symbol.
        skip_snapshots : bool, default=False
            Don't populate version list with snapshot information. Can improve performance significantly if there are
            many snapshots.

        Returns
        -------
        Dict[SymbolVersion, VersionInfo]
            Dictionary describing the version for each symbol-version pair in the library. Since symbol version is a
            (named) tuple you can index in to the dictionary simply as shown in the examples below.

        Examples
        --------
        >>> df = pd.DataFrame()
        >>> lib.write("symbol", df, metadata=10)
        >>> lib.write("symbol", df, metadata=11, prune_previous_versions=False)
        >>> lib.snapshot("snapshot")
        >>> lib.write("symbol", df, metadata=12, prune_previous_versions=False)
        >>> lib.delete("symbol", versions=(1, 2))
        >>> versions = lib.list_versions("symbol")
        >>> versions["symbol", 1].deleted
        True
        >>> versions["symbol", 1].snapshots
        ["my_snap"]
        """
        versions = self._nvs.list_versions(
            symbol=symbol,
            snapshot=snapshot,
            latest_only=latest_only,
            skip_snapshots=skip_snapshots,
        )
        return {
            SymbolVersion(v["symbol"], v["version"]): VersionInfo(v["date"], v["deleted"], v["snapshots"])
            for v in versions
        }

    def head(
        self,
        symbol: str,
        n: int = 5,
        as_of: Optional[AsOf] = None,
        columns: List[str] = None,
        lazy: bool = False,
        output_format: Optional[Union[OutputFormat, str]] = None,
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]] = None,
    ) -> Union[VersionedItem, LazyDataFrame]:
        """
        Read the first n rows of data for the named symbol. If n is negative, return all rows except the last n rows.

        Parameters
        ----------
        symbol
            Symbol name.
        n : int, default=5
            Number of rows to select if non-negative, otherwise number of rows to exclude.
        as_of : AsOf, default=None
            See documentation on `read`.
        columns
            See documentation on `read`.
        lazy : bool, default=False
            See documentation on `read`.
        output_format: Optional[Union[OutputFormat, str]], default=None
            See documentation on `read`.
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]], default=None
            See documentation on `read`.
        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]], default=None
            See documentation on `read`.

        Returns
        -------
        Union[VersionedItem, LazyDataFrame]
            If lazy is False, VersionedItem object that contains a .data and .metadata element.
            If lazy is True, a LazyDataFrame object on which further querying can be performed prior to collect.
        """
        if lazy:
            q = QueryBuilder().head(n)
            return LazyDataFrame(
                self,
                ReadRequest(
                    symbol=symbol,
                    as_of=as_of,
                    columns=columns,
                    query_builder=q,
                    output_format=output_format,
                    arrow_string_format_default=arrow_string_format_default,
                    arrow_string_format_per_column=arrow_string_format_per_column,
                ),
            )
        else:
            return self._nvs.head(
                symbol=symbol,
                n=n,
                as_of=as_of,
                columns=columns,
                implement_read_index=True,
                iterate_snapshots_if_tombstoned=False,
                output_format=output_format,
                arrow_string_format_default=arrow_string_format_default,
                arrow_string_format_per_column=arrow_string_format_per_column,
            )

    def tail(
        self,
        symbol: str,
        n: int = 5,
        as_of: Optional[Union[int, str]] = None,
        columns: List[str] = None,
        lazy: bool = False,
        output_format: Optional[Union[OutputFormat, str]] = None,
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]] = None,
        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]] = None,
    ) -> Union[VersionedItem, LazyDataFrame]:
        """
        Read the last n rows of data for the named symbol. If n is negative, return all rows except the first n rows.

        Parameters
        ----------
        symbol
            Symbol name.
        n : int, default=5
            Number of rows to select if non-negative, otherwise number of rows to exclude.
        as_of : AsOf, default=None
            See documentation on `read`.
        columns
            See documentation on `read`.
        lazy : bool, default=False
            See documentation on `read`.
        output_format: Optional[Union[OutputFormat, str]], default=None
            See documentation on `read`.
        arrow_string_format_default: Optional[Union[ArrowOutputStringFormat, "pa.DataType"]], default=None
            See documentation on `read`.
        arrow_string_format_per_column: Optional[Dict[str, Union[ArrowOutputStringFormat, "pa.DataType"]]], default=None
            See documentation on `read`.

        Returns
        -------
        Union[VersionedItem, LazyDataFrame]
            If lazy is False, VersionedItem object that contains a .data and .metadata element.
            If lazy is True, a LazyDataFrame object on which further querying can be performed prior to collect.
        """
        if lazy:
            q = QueryBuilder().tail(n)
            return LazyDataFrame(
                self,
                ReadRequest(
                    symbol=symbol,
                    as_of=as_of,
                    columns=columns,
                    query_builder=q,
                    output_format=output_format,
                    arrow_string_format_default=arrow_string_format_default,
                    arrow_string_format_per_column=arrow_string_format_per_column,
                ),
            )
        else:
            return self._nvs.tail(
                symbol=symbol,
                n=n,
                as_of=as_of,
                columns=columns,
                implement_read_index=True,
                iterate_snapshots_if_tombstoned=False,
                output_format=output_format,
                arrow_string_format_default=arrow_string_format_default,
                arrow_string_format_per_column=arrow_string_format_per_column,
            )

    @staticmethod
    def _info_to_desc(info: Dict[str, Any]) -> SymbolDescription:
        last_update_time = pd.to_datetime(info["last_update"], utc=True)
        if IS_PANDAS_TWO:
            # Pandas 2.0.0 now uses `datetime.timezone.utc` instead of `pytz.UTC`.
            # See: https://github.com/pandas-dev/pandas/issues/34916
            # We enforce the use of `pytz.UTC` for consistency.
            last_update_time = last_update_time.replace(tzinfo=pytz.UTC)
        columns = tuple(NameWithDType(n, t) for n, t in zip(info["col_names"]["columns"], info["dtype"]))
        index = tuple(NameWithDType(n, t) for n, t in zip(info["col_names"]["index"], info["col_names"]["index_dtype"]))
        return SymbolDescription(
            columns=columns,
            index=index,
            row_count=info["rows"],
            last_update_time=last_update_time,
            index_type=info["index_type"],
            date_range=info["date_range"],
            sorted=info["sorted"],
        )

    def get_description(self, symbol: str, as_of: Optional[AsOf] = None) -> SymbolDescription:
        """
        Returns descriptive data for ``symbol``.

        Parameters
        ----------
        symbol
            Symbol name.
        as_of : AsOf, default=None
            See documentation on `read`.

        Returns
        -------
        SymbolDescription
            Named tuple containing the descriptive data.

        See Also
        --------
        SymbolDescription
            For documentation on each field.
        """
        info = self._nvs.get_info(
            symbol,
            as_of,
            date_range_ns_precision=True,
            iterate_snapshots_if_tombstoned=False,
        )
        return self._info_to_desc(info)

    @staticmethod
    def parse_list_of_symbols(symbols: List[Union[str, ReadInfoRequest]]) -> (List, List):
        symbol_strings = []
        as_ofs = []

        def handle_read_request(s: ReadInfoRequest):
            symbol_strings.append(s.symbol)
            as_ofs.append(s.as_of)

        def handle_symbol(s: str):
            symbol_strings.append(s)
            as_ofs.append(None)

        for s in symbols:
            if isinstance(s, str):
                handle_symbol(s)
            elif isinstance(s, ReadInfoRequest):
                handle_read_request(s)
            else:
                raise ArcticInvalidApiUsageException(
                    f"Unsupported item in the symbols argument s=[{s}] type(s)=[{type(s)}]. Only [str] and"
                    " [ReadInfoRequest] are supported."
                )

        return (symbol_strings, as_ofs)

    def get_description_batch(
        self, symbols: List[Union[str, ReadInfoRequest]]
    ) -> List[Union[SymbolDescription, DataError]]:
        """
        Returns descriptive data for a list of ``symbols``.

        Parameters
        ----------
        symbols : List[Union[str, ReadInfoRequest]]
            List of symbols to read.

        Returns
        -------
        List[Union[SymbolDescription, DataError]]
            A list of the descriptive data, whose i-th element corresponds to the i-th element of the ``symbols`` parameter.
            If the specified version does not exist, a DataError object is returned, with symbol, version_request_type,
            version_request_data properties, error_code, error_category, and exception_string properties. If a key error or
            any other internal exception occurs, the same DataError object is also returned.

        See Also
        --------
        SymbolDescription
            For documentation on each field.
        """
        symbol_strings, as_ofs = self.parse_list_of_symbols(symbols)

        throw_on_error = False
        date_range_ns_precision = True
        descriptions = self._nvs._batch_read_descriptor(symbol_strings, as_ofs, throw_on_error, date_range_ns_precision)

        description_results = [
            description if isinstance(description, DataError) else self._info_to_desc(description)
            for description in descriptions
        ]

        return description_results

    def reload_symbol_list(self) -> None:
        """
        Forces the symbol list cache to be reloaded.

        This can take a long time on large libraries or certain S3 implementations, and once started, it cannot be
        safely interrupted. If the call is interrupted somehow (exception/process killed), please call this again ASAP.
        """
        self._nvs.version_store.reload_symbol_list()

    def compact_symbol_list(self) -> None:
        """
        Compact the symbol list cache into a single key in the storage

        Returns
        -------
        The number of symbol list keys prior to compaction


        Raises
        ------
        PermissionException
            Library has been opened in read-only mode
        InternalException
            Storage lock required to compact the symbol list could not be acquired
        """
        return self._nvs.compact_symbol_list()

    def is_symbol_fragmented(self, symbol: str, segment_size: Optional[int] = None) -> bool:
        """
        Check whether the number of segments that would be reduced by compaction is more than or equal to the
        value specified by the configuration option "SymbolDataCompact.SegmentCount" (defaults to 100).

        Parameters
        ----------
        symbol: `str`
            Symbol name.
        segment_size: `int`
            Target for maximum no. of rows per segment, after compaction.
            If parameter is not provided, library option for segments's maximum row size will be used

        Notes
        ----------
        Config map setting - SymbolDataCompact.SegmentCount will be replaced by a library setting
        in the future. This API will allow overriding the setting as well.

        Returns
        -------
        bool
        """
        return self._nvs.is_symbol_fragmented(symbol, segment_size)

    def defragment_symbol_data(
        self,
        symbol: str,
        segment_size: Optional[int] = None,
        prune_previous_versions: bool = False,
    ) -> VersionedItem:
        """
        Compacts fragmented segments by merging row-sliced segments (https://docs.arcticdb.io/technical/on_disk_storage/#data-layer).
        This method calls `is_symbol_fragmented` to determine whether to proceed with the defragmentation operation.

        CAUTION - Please note that a major restriction of this method at present is that any column slicing present on the data will be
        removed in the new version created as a result of this method.
        As a result, if the impacted symbol has more than 127 columns (default value), the performance of selecting individual columns of
        the symbol (by using the `columns` parameter) may be negatively impacted in the defragmented version.
        If your symbol has less than 127 columns this caveat does not apply.
        For more information, please see `columns_per_segment` here:

        https://docs.arcticdb.io/api/arcticdb/arcticdb.LibraryOptions

        Parameters
        ----------
        symbol: `str`
            Symbol name.
        segment_size: `int`
            Target for maximum no. of rows per segment, after compaction.
            If parameter is not provided, library option - "segment_row_size" will be used
            Note that no. of rows per segment, after compaction, may exceed the target.
            It is for achieving smallest no. of segment after compaction. Please refer to below example for further explanation.
        prune_previous_versions : bool, default=False
            Removes previous (non-snapshotted) versions from the database.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the defragmented symbol in the store.

        Raises
        ------
        1002 ErrorCategory.INTERNAL:E_ASSERTION_FAILURE
            If `is_symbol_fragmented` returns false.
        2001 ErrorCategory.NORMALIZATION:E_UNIMPLEMENTED_INPUT_TYPE
            If library option - "bucketize_dynamic" is ON

        Examples
        --------
        >>> lib.write("symbol", pd.DataFrame({"A": [0]}, index=[pd.Timestamp(0)]))
        >>> lib.append("symbol", pd.DataFrame({"A": [1, 2]}, index=[pd.Timestamp(1), pd.Timestamp(2)]))
        >>> lib.append("symbol", pd.DataFrame({"A": [3]}, index=[pd.Timestamp(3)]))
        >>> lib_tool = lib._dev_tools.library_tool()
        >>> lib_tool.read_index(sym)
                            start_index                     end_index  version_id stream_id          creation_ts          content_hash  index_type  key_type  start_col  end_col  start_row  end_row
        1970-01-01 00:00:00.000000000 1970-01-01 00:00:00.000000001          20    b'sym'  1678974096622685727   6872717287607530038          84         2          1        2          0        1
        1970-01-01 00:00:00.000000001 1970-01-01 00:00:00.000000003          21    b'sym'  1678974096931527858  12345256156783683504          84         2          1        2          1        3
        1970-01-01 00:00:00.000000003 1970-01-01 00:00:00.000000004          22    b'sym'  1678974096970045987   7952936283266921920          84         2          1        2          3        4
        >>> lib.version_store.defragment_symbol_data("symbol", 2)
        >>> lib_tool.read_index(sym)  # Returns two segments rather than three as a result of the defragmentation operation
                            start_index                     end_index  version_id stream_id          creation_ts         content_hash  index_type  key_type  start_col  end_col  start_row  end_row
        1970-01-01 00:00:00.000000000 1970-01-01 00:00:00.000000003          23    b'sym'  1678974097067271451  5576804837479525884          84         2          1        2          0        3
        1970-01-01 00:00:00.000000003 1970-01-01 00:00:00.000000004          23    b'sym'  1678974097067427062  7952936283266921920          84         2          1        2          3        4

        Notes
        ----------
        Config map setting - SymbolDataCompact.SegmentCount will be replaced by a library setting
        in the future. This API will allow overriding the setting as well.
        """
        return self._nvs.defragment_symbol_data(symbol, segment_size, prune_previous_versions)

    def merge_experimental(
        self,
        symbol: str,
        source: NormalizableType,
        strategy: MergeStrategy = MergeStrategy(),
        on: Optional[List[str]] = None,
        metadata: Any = None,
        prune_previous_versions: bool = False,
        upsert: bool = False,
    ):
        """
        Merge new data into an existing symbol's DataFrame according to a specified strategy.

        See [Merge Notebook](../notebooks/ArcticDB_merge.ipynb) for usage examples.

        !!! warning
            This API is under development and is subject to change. The API is not subject to semver and can change in
            minor or patch releases.

            Only date time indexed symbols and sources are supported at the moment.

            Dynamic schema is not supported.

        Parameters
        ----------
        symbol : str
            The symbol to merge data into.
        source : pandas.DataFrame or pandas.Series
            The new data to merge. In the case of timeseries, the index must be sorted.
        strategy : Optional[MergeStrategy], default=MergeStrategy(matched="update", not_matched_by_target="insert")
            !!! warning
                Only `MergeStrategy(matched="update", not_matched_by_target="do_nothing")` is implemented

            Determines how to handle matched and unmatched rows. Accepted strategies are:
                - MergeStrategy(matched="update", not_matched_by_target="do_nothing"): Update matched rows, leave others unchanged.
                - MergeStrategy(matched="do_nothing", not_matched_by_target="insert"): Insert unmatched rows from source.
                - MergeStrategy(matched="update", not_matched_by_target="insert"): Update matched rows and insert unmatched rows.
            Note: If the strategy includes "update" on matched, a row in the target cannot be matched by multiple rows in the source.

            The elements of `MergeStrategy` can be either values of the `MergeAction` enum or case-insensitive strings
            representing the enum values.
        on : Optional[List[str]]
            !!! warning
                Not yet implemented

            Columns which are used to determine row equality between source and target. A row is considered matched when
            all specified columns have equal values in both source and target.

            IMPORTANT: For date-time indexed data, the index is always included in matching and cannot be excluded.
        metadata : Any, optional
            Metadata to save alongside the new version.
        prune_previous_versions : bool, default False
            If True, removes previous versions from the version list.
        upsert : bool, default False
            !!! warning
                Not yet implemented

            If True and `not_matched_by_target="insert"`, creates the symbol if it does not exist.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the written symbol in the store. The data attribute will
            not be populated.

        Raises
        ------
        StorageException
            If symbol doesn't exist and `upsert=False`
        UserInputException
            If strategy is not one of the supported strategies listed above
        SortingException
            If date-time index is used and source or target are not sorted
        SchemaException
            If dynamic schema is used or if source's schema is incompatible with target's schema

        Examples
        --------

        >>> lib.write("symbol", pd.DataFrame({'a': [1, 2, 3]}, index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(2), pd.Timestamp(3)])))
        >>> lib.merge_experimental("symbol", pd.DataFrame({"a": [100, 200]}, index=pd.DatetimeIndex([pd.Timestamp(2), pd.Timestamp(4)])), strategy=MergeStrategy(matched="update", not_matched_by_target="do_nothing"))))
        >>> lib.read("symbol").data
                                       a
        1970-01-01 00:00:00.000000001  1
        1970-01-01 00:00:00.000000002  100
        1970-01-01 00:00:00.000000003  3
        """
        return self._nvs.merge_experimental(
            symbol=symbol,
            source=source,
            strategy=strategy,
            on=on,
            metadata=metadata,
            prune_previous_versions=prune_previous_versions,
            upsert=upsert,
        )

    @property
    def name(self) -> str:
        """The name of this library."""
        return self._nvs.name()

    def admin_tools(self) -> AdminTools:
        """Administrative utilities that operate on this library."""
        return AdminTools(self._nvs)

    @staticmethod
    def _normalize_staged_data_mode(mode: Union[StagedDataFinalizeMethod, str]) -> StagedDataFinalizeMethod:
        if mode is None:
            return StagedDataFinalizeMethod.WRITE

        if isinstance(mode, StagedDataFinalizeMethod):
            return mode

        if isinstance(mode, str):
            mode = mode.lower()

        if mode == "write":
            return StagedDataFinalizeMethod.WRITE
        elif mode == "append":
            return StagedDataFinalizeMethod.APPEND
        else:
            raise ArcticInvalidApiUsageException(
                "mode must be one of StagedDataFinalizeMethod.WRITE, StagedDataFinalizeMethod.APPEND, 'write', 'append'"
            )
