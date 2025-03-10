"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import copy
import datetime

import pytz
from enum import Enum, auto
from typing import Optional, Any, Tuple, Dict, Union, List, Iterable, NamedTuple
from numpy import datetime64

from arcticdb.options import \
    LibraryOptions, EnterpriseLibraryOptions, ModifiableLibraryOption, ModifiableEnterpriseLibraryOption
from arcticdb.preconditions import check
from arcticdb.supported_types import Timestamp
from arcticdb.util._versions import IS_PANDAS_TWO

from arcticdb.version_store.processing import ExpressionNode, QueryBuilder
from arcticdb.version_store._store import NativeVersionStore, VersionedItem, VersionQueryInput
from arcticdb_ext.exceptions import ArcticException
from arcticdb_ext.version_store import DataError, OutputFormat
import pandas as pd
import numpy as np
import logging
from arcticdb.version_store._normalization import normalize_metadata

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
        date_range_fields_equal = (self.date_range[0] == other.date_range[0] or (np.isnat(self.date_range[0]) and np.isnat(other.date_range[0]))) and \
                                  (self.date_range[1] == other.date_range[1] or (np.isnat(self.date_range[1]) and np.isnat(other.date_range[1])))
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

    def __init__(self, symbol: str, data: Union[Any, NormalizableType], metadata: Any = None):
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

        See Also
        --------
        Library.write_pickle: For information on the implications of providing data that needs to be pickled.
        """
        self.symbol = symbol
        self.data = data
        self.metadata = metadata

    def __repr__(self):
        res = f"WritePayload(symbol={self.symbol}, data_id={id(self.data)}"
        res += f", metadata={self.metadata}" if self.metadata is not None else ""
        res += ")"
        return res

    def __iter__(self):
        yield self.symbol
        yield self.data
        if self.metadata is not None:
            yield self.metadata


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
    row_range: Optional[Tuple[int, int]], default=none
        See `read` method.
    columns: Optional[List[str]], default=none
        See `read` method.
    query_builder: Optional[Querybuilder], default=none
        See `read` method.

    See Also
    --------
    Library.read: For documentation on the parameters.
    """

    symbol: str
    as_of: Optional[AsOf] = None
    date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None
    row_range: Optional[Tuple[int, int]] = None
    columns: Optional[List[str]] = None
    query_builder: Optional[QueryBuilder] = None

    def __repr__(self):
        res = f"ReadRequest(symbol={self.symbol}"
        res += f", as_of={self.as_of}" if self.as_of is not None else ""
        res += f", date_range={self.date_range}" if self.date_range is not None else ""
        res += f", row_range={self.row_range}" if self.row_range is not None else ""
        res += f", columns={self.columns}" if self.columns is not None else ""
        res += f", query_builder={self.query_builder}" if self.query_builder is not None else ""
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

class UpdatePayload:

    def __init__(
        self,
        symbol: str,
        data: NormalizableType,
        metadata: Any = None,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None
    ):
        self.symbol = symbol
        self.data = data
        self.metadata = metadata
        self.date_range = date_range

    def __repr__(self):
        return(
            f"UpdatePayload(symbol={self.symbol}, data_id={id(self.data)}"
            f", metadata={self.metadata}" if self.metadata is not None else ""
            f", date_range={self.date_range}" if self.date_range is not None else ""
        )

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
            f"LazyDataFrameCollection init requires all provided lazy dataframes to be referring to the same library, but received: {[lib for lib in lib_set]}"
        )
        super().__init__()
        self._lazy_dataframes = lazy_dataframes
        if len(self._lazy_dataframes):
            self._lib = self._lazy_dataframes[0].lib

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
        return self._lib.read_batch(self._read_requests())

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
        return "LazyDataFrameCollection(" + str(self._lazy_dataframes) + (" | " if len(query_builder_repr) else "") + query_builder_repr + ")"

    def __repr__(self) -> str:
        return self.__str__()


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
        self._nvs._normalizer.df._skip_df_consolidation = True
        self._dev_tools = DevTools(nvs)

    def __repr__(self):
        return "Library(%s, path=%s, storage=%s)" % (
            self.arctic_instance_desc,
            self._nvs._lib_cfg.lib_desc.name,
            self._nvs.get_backing_store(),
        )

    def __getitem__(self, symbol: str) -> VersionedItem:
        return self.read(symbol)

    def __contains__(self, symbol: str):
        return self.has_symbol(symbol)

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
            replication=write_options.sync_passive.enabled,
            background_deletion=write_options.delayed_deletes
        )

    def stage(
        self,
        symbol: str,
        data: NormalizableType,
        validate_index=True,
        sort_on_index=False,
        sort_columns: List[str] = None
    ):
        """
        Write a staged data chunk to storage, that will not be visible until finalize_staged_data is called on
        the symbol. Equivalent to write() with staged=True.

        Parameters
        ----------
        symbol : str
            Symbol name. Limited to 255 characters. The following characters are not supported in symbols:
            ``"*", "&", "<", ">"``
        data : NormalizableType
            Data to be written. Staged data must be normalizable.
        validate_index:
            Check that the index is sorted prior to writing. In the case of unsorted data, throw an UnsortedDataException
        sort_on_index:
            If an appropriate index is present, sort the data on it. In combination with sort_columns the
            index will be used as the primary sort column, and the others as secondaries.
        sort_columns:
            Sort the data by specific columns prior to writing.
        """
        self._nvs.stage(
            symbol,
            data,
            validate_index=validate_index,
            sort_on_index=sort_on_index,
            sort_columns=sort_columns,
            norm_failure_options_msg="Failed to normalize data. It is inadvisable to pickle staged data"
                                     " as it will not be possible to finalize it.")

    def write(
        self,
        symbol: str,
        data: NormalizableType,
        metadata: Any = None,
        prune_previous_versions: bool = False,
        staged=False,
        validate_index=True,
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

        Note that `write` is not designed for multiple concurrent writers over a single symbol *unless the staged
        keyword argument is set to True*. If ``staged`` is True, written segments will be staged and left in an
        "incomplete" stage, unable to be read until they are finalized. This enables multiple
        writers to a single symbol - all writing staged data at the same time - with one process able to later finalize
        all staged data rendering the data readable by clients. To finalize staged data, see `finalize_staged_data`.

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
        staged : bool, default=False
            Whether to write to a staging area rather than immediately to the library.
            See documentation on `finalize_staged_data` for more information.
        validate_index: bool, default=True
            If True, verify that the index of `data` supports date range searches and update operations.
            This tests that the data is sorted in ascending order, using Pandas DataFrame.index.is_monotonic_increasing.

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

        Staging data for later finalisation (enables concurrent writes):

        >>> df = pd.DataFrame({'column': [5,6,7]}, index=pd.date_range(start='1/1/2000', periods=3))
        >>> lib.write("staged", df, staged=True)  # Multiple staged writes can occur in parallel
        >>> lib.finalize_staged_data("staged", StagedDataFinalizeMethod.WRITE)  # Must be run after all staged writes have completed
        >>> lib.read("staged").data  # Would return error if run before finalization
                    column
        2000-01-01       5
        2000-01-02       6
        2000-01-03       7

        WritePayload objects can be unpacked and used as parameters:
        >>> w = adb.WritePayload("symbol", df, metadata={'the': 'metadata'})
        >>> lib.write(*w, staged=True)
        """
        if not isinstance(data, NORMALIZABLE_TYPES):
            raise ArcticUnsupportedDataTypeException(
                "data is of a type that cannot be normalized. Consider using "
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
            norm_failure_options_msg="Using write_pickle will allow the object to be written. However, many operations "
                                     "(such as date_range filtering and column selection) will not work on pickled data.",
        )

    def write_pickle(
        self, symbol: str, data: Any, metadata: Any = None, prune_previous_versions: bool = False, staged=False
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
        )

    @staticmethod
    def _raise_if_duplicate_symbols_in_batch(batch):
        symbols = {p.symbol for p in batch}
        if len(symbols) < len(batch):
            raise ArcticDuplicateSymbolsInBatchException

    @staticmethod
    def _raise_if_unsupported_type_in_write_batch(payloads):
        bad_symbols = []
        for p in payloads:
            if not isinstance(p.data, NORMALIZABLE_TYPES):
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
    ) -> Optional[VersionedItem]:
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
        return self._nvs.append(
            symbol=symbol,
            dataframe=data,
            metadata=metadata,
            prune_previous_version=prune_previous_versions,
            validate_index=validate_index,
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
        )

    def update(
        self,
        symbol: str,
        data: Union[pd.DataFrame, pd.Series],
        metadata: Any = None,
        upsert: bool = False,
        date_range: Optional[Tuple[Optional[Timestamp], Optional[Timestamp]]] = None,
        prune_previous_versions: bool = False,
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
        return self._nvs.update(
            symbol=symbol,
            data=data,
            metadata=metadata,
            upsert=upsert,
            date_range=date_range,
            prune_previous_version=prune_previous_versions,
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
            List `arcticdb.library.UpdatePayload`. Each element of the list describes an update operation for a
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
        )
        return batch_update_result

    def delete_staged_data(self, symbol: str):
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
        mode: Optional[StagedDataFinalizeMethod] = StagedDataFinalizeMethod.WRITE,
        prune_previous_versions: bool = False,
        metadata: Any = None,
        validate_index = True,
        delete_staged_data_on_failure: bool = False
    ) -> VersionedItem:
        """
        Finalizes staged data, making it available for reads. All staged segments must be ordered and non-overlapping.
        ``finalize_staged_data`` is less time consuming than ``sort_and_finalize_staged_data``.

        If ``mode`` is ``StagedDataFinalizeMethod.APPEND`` the index of the first row of the new segment must be equal to or greater
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

        mode : `StagedDataFinalizeMethod`, default=StagedDataFinalizeMethod.WRITE
            Finalize mode. Valid options are WRITE or APPEND. Write collects the staged data and writes them to a
            new version. Append collects the staged data and appends them to the latest version.
        prune_previous_versions: bool, default=False
            Removes previous (non-snapshotted) versions from the database.
        metadata : Any, default=None
            Optional metadata to persist along with the symbol.
        validate_index: bool, default=True
            If True, and staged segments are timeseries, will verify that the index of the symbol after this operation
            supports date range searches and update operations. This requires that the indexes of the staged segments
            are non-overlapping with each other, and, in the case of `StagedDataFinalizeMethod.APPEND`, fall after the
            last index value in the previous version.
        delete_staged_data_on_failure : bool, default=False
            Determines the handling of staged data when an exception occurs during the execution of the 
            ``finalize_staged_data`` function.

            - If set to True, all staged data for the specified symbol will be deleted if an exception occurs.
            - If set to False, the staged data will be retained and will be used in subsequent calls to 
              ``finalize_staged_data``.

            To manually delete staged data, use the ``delete_staged_data`` function.
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
        write
            Documentation on the ``staged`` parameter explains the concept of staged data in more detail.

        Examples
        --------
        >>> lib.write("sym", pd.DataFrame({"col": [3, 4]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3), pd.Timestamp(2024, 1, 4)])), staged=True)
        >>> lib.write("sym", pd.DataFrame({"col": [1, 2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 2)])), staged=True)
        >>> lib.finalize_staged_data("sym")
        >>> lib.read("sym").data
                    col
        2024-01-01    1
        2024-01-02    2
        2024-01-03    3
        2024-01-04    4
        """
        return self._nvs.compact_incomplete(
            symbol,
            append=mode == StagedDataFinalizeMethod.APPEND,
            convert_int_to_float=False,
            metadata=metadata,
            prune_previous_version=prune_previous_versions,
            validate_index=validate_index,
            delete_staged_data_on_failure=delete_staged_data_on_failure
        )

    def sort_and_finalize_staged_data(
        self,
        symbol: str,
        mode: Optional[StagedDataFinalizeMethod] = StagedDataFinalizeMethod.WRITE,
        prune_previous_versions: bool = False,
        metadata: Any = None,
        delete_staged_data_on_failure: bool = False
    ) -> VersionedItem:
        """
        Sorts and merges all staged data, making it available for reads. This differs from `finalize_staged_data` in that it
        can support staged segments with interleaved time periods and staged segments which are not internally sorted. The
        end result will be sorted. This requires performing a full sort in memory so can be time consuming.

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

        mode : `StagedDataFinalizeMethod`, default=StagedDataFinalizeMethod.WRITE
            Finalize mode. Valid options are WRITE or APPEND. Write collects the staged data and writes them to a
            new timeseries. Append collects the staged data and appends them to the latest version.

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
        vit = self._nvs.version_store.sort_merge(
            symbol,
            normalize_metadata(metadata),
            mode == StagedDataFinalizeMethod.APPEND,
            prune_previous_versions=prune_previous_versions,
            delete_staged_data_on_failure=delete_staged_data_on_failure
        )
        return self._nvs._convert_thin_cxx_item_to_python(vit, metadata)

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
        lazy: bool = False
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
            part of the data that falls withing the given range (inclusive). None on either end leaves that part of the
            range open-ended. Hence specifying ``(None, datetime(2025, 1, 1)`` declares that you wish to read all data up
            to and including 20250101.
            The same effect can be achieved by using the date_range clause of the QueryBuilder class, which will be
            slower, but return data with a smaller memory footprint. See the QueryBuilder.date_range docstring for more
            details.

            Only one of date_range or row_range can be provided.

        row_range: `Optional[Tuple[int, int]]`, default=None
            Row range to read data for. Inclusive of the lower bound, exclusive of the upper bound
            lib.read(symbol, row_range=(start, end)).data should behave the same as df.iloc[start:end], including in
            the handling of negative start/end values.

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
                implement_read_index=True,
                iterate_snapshots_if_tombstoned=False
            )

    def read_batch(
        self,
        symbols: List[Union[str, ReadRequest]],
        query_builder: Optional[QueryBuilder] = None,
        lazy: bool = False,
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

        def handle_symbol(s_):
            symbol_strings.append(s_)
            for l_ in (as_ofs, date_ranges, row_ranges, columns, query_builders):
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
                        )
                    )
                )
            return LazyDataFrameCollection(lazy_dataframes)
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
        self, write_metadata_payloads: List[WriteMetadataPayload], prune_previous_versions: bool = False,
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
        self._nvs.snapshot(snap_name=snapshot_name, metadata=metadata, skip_symbols=skip_symbols, versions=versions)

    def delete(self, symbol: str, versions: Optional[Union[int, Iterable[int]]] = None):
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

        for v in versions:
            self._nvs.delete_version(symbol, v)

    def prune_previous_versions(self, symbol):
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
    ):
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
            )

    def tail(
        self,
            symbol: str,
            n: int = 5,
            as_of: Optional[Union[int, str]] = None,
            columns: List[str] = None,
            lazy: bool = False,
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

    def reload_symbol_list(self):
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

    @property
    def name(self):
        """The name of this library."""
        return self._nvs.name()
