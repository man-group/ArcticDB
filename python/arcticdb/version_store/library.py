"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import datetime

import pytz
from enum import Enum, auto
from typing import Optional, Any, Tuple, Dict, AnyStr, Union, List, Iterable, NamedTuple
from numpy import datetime64

from arcticdb.options import LibraryOptions
from arcticdb.supported_types import Timestamp
from arcticdb.util._versions import IS_PANDAS_TWO

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.version_store._store import NativeVersionStore, VersionedItem, VersionQueryInput
from arcticdb_ext.exceptions import ArcticException
from arcticdb_ext.version_store import DataError
import pandas as pd
import numpy as np
import logging

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
    index : NameWithDType
        Index of the symbol.
    index_type : str {"NA", "index", "multi_index"}
        Whether the index is a simple index or a multi_index. ``NA`` indicates that the stored data does not have an index.
    row_count : int
        Number of rows.
    last_update_time : datetime64
        The time of the last update to the symbol, in UTC.
    date_range : Tuple[Union[datetime.datetime, numpy.datetime64], Union[datetime.datetime, numpy.datetime64]]
        The values of the index column in the first and last rows of this symbol in UTC. Both values will be NaT if:
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
    index: NameWithDType
    index_type: str
    row_count: int
    last_update_time: datetime64
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
        return f"WritePayload(symbol={self.symbol}, data_id={id(self.data)}, metadata={self.metadata})"

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


class StagedDataFinalizeMethod(Enum):
    WRITE = auto()
    APPEND = auto()


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
        write_options = self._nvs.lib_cfg().lib_desc.version.write_options
        return LibraryOptions(
            dynamic_schema=write_options.dynamic_schema,
            dedup=write_options.de_duplication,
            rows_per_segment=write_options.segment_row_size,
            columns_per_segment=write_options.column_group_size,
            encoding_version=self._nvs.lib_cfg().lib_desc.version.encoding_version,
        )

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
        writers to a single symbol - all writing staged data at the same time - with one process able to later finalise
        all staged data rendering the data readable by clients. To finalise staged data, see `finalize_staged_data`.

        Note: ArcticDB will use the 0-th level index of the Pandas DataFrame for its on-disk index.

        Any non-`DatetimeIndex` will converted into an internal `RowCount` index. That is, ArcticDB will assign each
        row a monotonically increasing integer identifier and that will be used for the index.

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
            Each unit of staged data must a) be datetime indexed and b) not overlap with any other unit of
            staged data. Note that this will create symbols with Dynamic Schema enabled.
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
            See `write`.
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
        )

    def write_pickle_batch(
        self, payloads: List[WritePayload], prune_previous_versions: bool = False, staged=False
    ) -> List[VersionedItem]:
        """
        Write a batch of multiple symbols, pickling their data if necessary.

        Parameters
        ----------
        payloads : `List[WritePayload]`
            Symbols and their corresponding data. There must not be any duplicate symbols in `payload`.
        prune_previous_versions: `bool`, default=False
            See `write`.
        staged: `bool`, default=False
            See `write`.

        Returns
        -------
        List[VersionedItem]
            Structures containing metadata and version number of the written symbols in the store, in the
            same order as `payload`.

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

        return self._nvs.batch_write(
            [p.symbol for p in payloads],
            [p.data for p in payloads],
            [p.metadata for p in payloads],
            prune_previous_version=prune_previous_versions,
            pickle_on_failure=True,
            parallel=staged,
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
            Removes previous (non-snapshotted) versions from the database when True.
        validate_index: bool, default=True
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
        prune_previous_versions=False,
    ) -> VersionedItem:
        """
        Overwrites existing symbol data with the contents of ``data``. The entire range between the first and last index
        entry in ``data`` is replaced in its entirety with the contents of ``data``, adding additional index entries if
        required. `update` only operates over the outermost index level - this means secondary index rows will be
        removed if not contained in ``data``.

        Both the existing symbol version and ``data`` must be timeseries-indexed.

        Note that `update` is not designed for multiple concurrent writers over a single symbol.

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
        prune_previous_versions
            Removes previous (non-snapshotted) versions from the database when True.

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
        """
        return self._nvs.update(
            symbol=symbol,
            data=data,
            metadata=metadata,
            upsert=upsert,
            date_range=date_range,
            prune_previous_version=prune_previous_versions,
        )

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
        prune_previous_versions: Optional[bool] = False,
    ):
        """
        Finalises staged data, making it available for reads.

        Parameters
        ----------
        symbol : `str`
            Symbol to finalize data for.

        mode : `StagedDataFinalizeMethod`, default=StagedDataFinalizeMethod.WRITE
            Finalise mode. Valid options are WRITE or APPEND. Write collects the staged data and writes them to a
            new timeseries. Append collects the staged data and appends them to the latest version.
        prune_previous_versions
            Removes previous (non-snapshotted) versions from the database.

        See Also
        --------
        write
            Documentation on the ``staged`` parameter explains the concept of staged data in more detail.
        """
        self._nvs.compact_incomplete(
            symbol,
            append=mode == StagedDataFinalizeMethod.APPEND,
            convert_int_to_float=False,
            prune_previous_version=prune_previous_versions,
        )

    def sort_and_finalize_staged_data(
        self, symbol: str, mode: Optional[StagedDataFinalizeMethod] = StagedDataFinalizeMethod.WRITE
    ):
        """
        sort_merge will sort and finalize staged data. This differs from `finalize_staged_data` in that it
        can support staged segments with interleaved time periods - the end result will be ordered. This requires
        performing a full sort in memory so can be time consuming.

        Parameters
        ----------
        symbol : `str`
            Symbol to finalize data for.

        mode : `StagedDataFinalizeMethod`, default=StagedDataFinalizeMethod.WRITE
            Finalise mode. Valid options are WRITE or APPEND. Write collects the staged data and writes them to a
            new timeseries. Append collects the staged data and appends them to the latest version.

        See Also
        --------
        write
            Documentation on the ``staged`` parameter explains the concept of staged data in more detail.
        """

        self._nvs.version_store.sort_merge(symbol, None, mode == StagedDataFinalizeMethod.APPEND, False)

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
    ) -> VersionedItem:
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
            Applicable only for Pandas data. Determines which columns to return data for.

        query_builder: Optional[QueryBuilder], default=None
            A QueryBuilder object to apply to the dataframe before it is returned. For more information see the
            documentation for the QueryBuilder class (``from arcticdb import QueryBuilder; help(QueryBuilder)``).

        Returns
        -------
        VersionedItem object that contains a .data and .metadata element

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
        return self._nvs.read(
            symbol=symbol,
            as_of=as_of,
            date_range=date_range,
            row_range=row_range,
            columns=columns,
            query_builder=query_builder,
        )

    def read_batch(
        self, symbols: List[Union[str, ReadRequest]], query_builder: Optional[QueryBuilder] = None
    ) -> List[Union[VersionedItem, DataError]]:
        """
        Reads multiple symbols.

        Parameters
        ----------
        symbols : List[Union[str, ReadRequest]]
            List of symbols to read.

        query_builder: Optional[QueryBuilder], default=None
            A single QueryBuilder to apply to all the dataframes before they are returned. If this argument is passed
            then none of the ``symbols`` may have their own query_builder specified in their request.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            A list of the read results, whose i-th element corresponds to the i-th element of the ``symbols`` parameter.
            If the specified version does not exist, a DataError object is returned, with symbol, version_request_type,
            version_request_data properties, error_code, error_category, and exception_string properties. If a key error or
            any other internal exception occurs, the same DataError object is also returned.

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
        return self._nvs._batch_read_to_versioned_items(
            symbol_strings, as_ofs, date_ranges, row_ranges, columns, query_builder or query_builders, throw_on_error
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
        return self._nvs.read_metadata(symbol, as_of)

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

    def write_metadata(self, symbol: str, metadata: Any) -> VersionedItem:
        """
        Write metadata under the specified symbol name to this library. The data will remain unchanged.
        A new version will be created.

        If the symbol is missing, it causes a write with empty data (None, pickled, can't append) and the supplied
        metadata.

        This method should be faster than `write` as it involves no data segment read/write operations.

        Parameters
        ----------
        symbol
            Symbol name for the item
        metadata
            Metadata to persist along with the symbol

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the affected symbol in the store.
        """
        return self._nvs.write_metadata(symbol, metadata, prune_previous_version=False)

    def write_metadata_batch(
        self, write_metadata_payloads: List[WriteMetadataPayload], prune_previous_versions=None
    ) -> List[Union[VersionedItem, DataError]]:
        """
        Write metadata to multiple symbols in a batch fashion. This is more efficient than making multiple `write_metadata` calls
        in succession as some constant-time operations can be executed only once rather than once for each element of
        `write_metadata_payloads`.
        Note that this isn't an atomic operation - it's possible for the metadata for one symbol to be fully written and
        readable before another symbol.

        Parameters
        ----------
        write_metadata_payloads : `List[WriteMetadataPayload]`
            Symbols and their corresponding metadata. There must not be any duplicate symbols in `payload`.
        prune_previous_versions : `Optional[bool]`, default=None
            Remove previous versions from version list. Uses library default if left as None.

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
        """
        self._nvs.snapshot(snap_name=snapshot_name, metadata=metadata, skip_symbols=skip_symbols, versions=versions)

    def delete(self, symbol: str, versions: Optional[Union[int, Iterable[int]]] = None):
        """
        Delete all versions of the symbol from the library, unless ``version`` is specified, in which case only those
        versions are deleted.

        This may not actually delete the underlying data if a snapshot still references the version. See `snapshot` for
        more detail.

        Note that this may require data to be removed from the underlying storage which can be slow.

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

    def delete_data_in_range(self, symbol: str, date_range: Tuple[Optional[Timestamp], Optional[Timestamp]]):
        """Delete data within the given date range, creating a new version of ``symbol``.

        The existing symbol version must be timeseries-indexed.

        Parameters
        ----------
        symbol
            Symbol name.

        date_range
            The date range in which to delete data. Leaving any part of the tuple as None leaves that part of the range
            open ended.

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
        self._nvs.delete(symbol, date_range=date_range)

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

    def list_snapshots(self) -> Dict[str, Any]:
        """
        List the snapshots in the library.

        Returns
        -------
        Dict[str, Any]
            Snapshots in the library. Keys are snapshot names, values are metadata associated with that snapshot.
        """
        return self._nvs.list_snapshots()

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
            iterate_on_failure=False,
            skip_snapshots=skip_snapshots,
        )
        return {
            SymbolVersion(v["symbol"], v["version"]): VersionInfo(v["date"], v["deleted"], v["snapshots"])
            for v in versions
        }

    def head(self, symbol: str, n: int = 5, as_of: Optional[AsOf] = None, columns: List[str] = None) -> VersionedItem:
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

        Returns
        -------
        VersionedItem object that contains a .data and .metadata element.
        """
        return self._nvs.head(symbol=symbol, n=n, as_of=as_of, columns=columns)

    def tail(
        self, symbol: str, n: int = 5, as_of: Optional[Union[int, str]] = None, columns: List[str] = None
    ) -> VersionedItem:
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

        Returns
        -------
        VersionedItem object that contains a .data and .metadata element.
        """
        return self._nvs.tail(symbol=symbol, n=n, as_of=as_of, columns=columns)

    @staticmethod
    def _info_to_desc(info: Dict[str, Any]) -> SymbolDescription:
        last_update_time = pd.to_datetime(info["last_update"], utc=True)
        if IS_PANDAS_TWO:
            # Pandas 2.0.0 now uses `datetime.timezone.utc` instead of `pytz.UTC`.
            # See: https://github.com/pandas-dev/pandas/issues/34916
            # We enforce the use of `pytz.UTC` for consistency.
            last_update_time = last_update_time.replace(tzinfo=pytz.UTC)
        columns = tuple(NameWithDType(n, t) for n, t in zip(info["col_names"]["columns"], info["dtype"]))
        index = NameWithDType(info["col_names"]["index"], info["col_names"]["index_dtype"])
        date_range = tuple(
            map(
                lambda x: x.replace(tzinfo=datetime.timezone.utc) if not np.isnat(np.datetime64(x)) else x,
                info["date_range"],
            )
        )
        return SymbolDescription(
            columns=columns,
            index=index,
            row_count=info["rows"],
            last_update_time=last_update_time,
            index_type=info["index_type"],
            date_range=date_range,
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
        info = self._nvs.get_info(symbol, as_of)
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
        descriptions = self._nvs._batch_read_descriptor(symbol_strings, as_ofs, throw_on_error)

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

    def defragment_symbol_data(self, symbol: str, segment_size: Optional[int] = None) -> VersionedItem:
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
        >>> lib.read_index(sym)
                            start_index                     end_index  version_id stream_id          creation_ts          content_hash  index_type  key_type  start_col  end_col  start_row  end_row
        1970-01-01 00:00:00.000000000 1970-01-01 00:00:00.000000001          20    b'sym'  1678974096622685727   6872717287607530038          84         2          1        2          0        1
        1970-01-01 00:00:00.000000001 1970-01-01 00:00:00.000000003          21    b'sym'  1678974096931527858  12345256156783683504          84         2          1        2          1        3
        1970-01-01 00:00:00.000000003 1970-01-01 00:00:00.000000004          22    b'sym'  1678974096970045987   7952936283266921920          84         2          1        2          3        4
        >>> lib.version_store.defragment_symbol_data("symbol", 2)
        >>> lib.read_index(sym)  # Returns two segments rather than three as a result of the defragmentation operation
                            start_index                     end_index  version_id stream_id          creation_ts         content_hash  index_type  key_type  start_col  end_col  start_row  end_row
        1970-01-01 00:00:00.000000000 1970-01-01 00:00:00.000000003          23    b'sym'  1678974097067271451  5576804837479525884          84         2          1        2          0        3
        1970-01-01 00:00:00.000000003 1970-01-01 00:00:00.000000004          23    b'sym'  1678974097067427062  7952936283266921920          84         2          1        2          3        4

        Notes
        ----------
        Config map setting - SymbolDataCompact.SegmentCount will be replaced by a library setting
        in the future. This API will allow overriding the setting as well.
        """
        return self._nvs.defragment_symbol_data(symbol, segment_size)

    @property
    def name(self):
        """The name of this library."""
        return self._nvs.name()
