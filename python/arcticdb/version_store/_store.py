"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import datetime
import os
import sys
import pandas as pd
import pytz
import re
import six
import itertools
import attr
import warnings
import difflib
from datetime import datetime
from numpy import datetime64
from pandas import Timestamp, to_datetime, Timedelta
from typing import Any, Optional, Union, List, Mapping, Iterable, Sequence, Tuple, Dict, Set, TYPE_CHECKING
from contextlib import contextmanager

from arcticc.pb2.descriptors_pb2 import TypeDescriptor, SortedValue
from arcticc.pb2.storage_pb2 import LibraryConfig, EnvironmentConfigsMap
from arcticdb.preconditions import check
from arcticdb.supported_types import DateRangeInput, ExplicitlySupportedDates
from arcticdb.toolbox.library_tool import LibraryTool
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.storage import OpenMode as _OpenMode
from arcticdb.encoding_version import EncodingVersion
from arcticdb_ext.storage import (
    create_mem_config_resolver as _create_mem_config_resolver,
    LibraryIndex as _LibraryIndex,
    Library as _Library,
)
from arcticdb.version_store.read_result import ReadResult
from arcticdb_ext.version_store import IndexRange as _IndexRange
from arcticdb_ext.version_store import RowRange as _RowRange
from arcticdb_ext.version_store import SignedRowRange as _SignedRowRange
from arcticdb_ext.version_store import PythonVersionStore as _PythonVersionStore
from arcticdb_ext.version_store import PythonVersionStoreReadQuery as _PythonVersionStoreReadQuery
from arcticdb_ext.version_store import PythonVersionStoreUpdateQuery as _PythonVersionStoreUpdateQuery
from arcticdb_ext.version_store import PythonVersionStoreReadOptions as _PythonVersionStoreReadOptions
from arcticdb_ext.version_store import PythonVersionStoreVersionQuery as _PythonVersionStoreVersionQuery
from arcticdb_ext.version_store import ColumnStats as _ColumnStats
from arcticdb_ext.version_store import StreamDescriptorMismatch
from arcticdb_ext.version_store import DataError
from arcticdb.authorization.permissions import OpenMode
from arcticdb.exceptions import ArcticDbNotYetImplemented, ArcticNativeException
from arcticdb.flattener import Flattener
from arcticdb.log import version as log
from arcticdb.version_store._custom_normalizers import get_custom_normalizer, CompositeCustomNormalizer
from arcticdb.version_store._normalization import (
    NPDDataFrame,
    normalize_metadata,
    denormalize_user_metadata,
    denormalize_dataframe,
    MsgPackNormalizer,
    CompositeNormalizer,
    FrameData,
    _IDX_PREFIX_LEN,
    get_timezone_from_metadata,
    _from_tz_timestamp,
    restrict_data_to_date_range_only,
    normalize_dt_range_to_ts,
)

# These chars are encoded by S3 and on doing a list_symbols they will show up as the encoded form eg. &amp
UNSUPPORTED_S3_CHARS = {"\0", "*", "<", ">"}
MAX_SYMBOL_SIZE = (2**8) - 1


TimeSeriesType = Union[pd.DataFrame, pd.Series]


IS_WINDOWS = sys.platform == "win32"


# auto_attribs=True breaks Cython-ising this code. As a result must manually create attr.ib instances.
@attr.s(slots=True, auto_attribs=False)
class VersionedItem:
    """
    Return value for many operations that captures the result and associated information.

    Attributes
    ----------
    library: str
        Library this result relates to.
    symbol: str
        Read or modified symbol.
    data: Any
        For data retrieval (read) operations, contains the data read.
        For data modification operations, the value might not be populated.
    version: int
        For data retrieval operations, the version the `as_of` argument resolved to.
        For data modification operations, the version the data has been written under.
    metadata: Any
        The metadata saved alongside `data`.
        Availability depends on the method used and may be different from that of `data`.
    host: Optional[str]
        Informational / for backwards compatibility.
    """

    symbol: str = attr.ib()
    library: str = attr.ib()
    data: Any = attr.ib(repr=lambda d: "n/a" if d is None else str(type(d)))
    version: int = attr.ib()
    metadata: Any = attr.ib(default=None)
    host: Optional[str] = attr.ib(default=None)

    def __iter__(self):  # Backwards compatible with the old NamedTuple implementation
        warnings.warn("Don't iterate VersionedItem. Use attrs.astuple() explicitly", SyntaxWarning, stacklevel=2)
        return iter(attr.astuple(self))


def _env_config_from_lib_config(lib_cfg, env):
    cfg = EnvironmentConfigsMap()
    e = cfg.env_by_id[env]
    e.lib_by_path[lib_cfg.lib_desc.name].CopyFrom(lib_cfg.lib_desc)
    for sid in lib_cfg.lib_desc.storage_ids:
        e.storage_by_id[sid].CopyFrom(lib_cfg.storage_by_id[sid])
    return cfg


VersionQueryInput = Union[int, str, ExplicitlySupportedDates, None]


def _normalize_dt_range(dtr: DateRangeInput) -> _IndexRange:
    start, end = normalize_dt_range_to_ts(dtr)
    return _IndexRange(start.value, end.value)


def _handle_categorical_columns(symbol, data, throw=True):
    if isinstance(data, (pd.DataFrame, pd.Series)):
        categorical_columns = []
        if isinstance(data, pd.DataFrame):
            for column_name, dtype in data.dtypes.items():
                if dtype.name == "category":
                    categorical_columns.append(column_name)
        else:
            # Series
            if data.dtype.name == "category":
                categorical_columns.append(data.name)
        if len(categorical_columns) > 0:
            message = (
                "Symbol: {}\nDataFrame/Series contains categorical data, cannot append or update\nCategorical"
                " columns: {}".format(symbol, categorical_columns)
            )
            if throw:
                raise ArcticDbNotYetImplemented(message)
            else:
                log.warn(message)


_BATCH_BAD_ARGS: Dict[Any, Sequence[str]] = {}
_STREAM_DESCRIPTOR_SPLIT = re.compile(r"(?<=>), ")


def _check_batch_kwargs(batch_fun, non_batch_fun, kwargs: Dict):
    cached = _BATCH_BAD_ARGS.get(batch_fun, None)
    if not cached:
        batch_args = set(batch_fun.__code__.co_varnames[: batch_fun.__code__.co_argcount])
        none_args = set(non_batch_fun.__code__.co_varnames[: non_batch_fun.__code__.co_argcount])
        cached = _BATCH_BAD_ARGS[batch_fun] = none_args - batch_args
    union = cached & kwargs.keys()
    if union:
        log.warning("Using non-batch arguments {} with {}", union, batch_fun.__name__)


@contextmanager
def _diff_long_stream_descriptor_mismatch(nvs):  # Diffing strings is easier done in Python than C++
    try:
        yield
    except StreamDescriptorMismatch as sdm:
        nvs.last_mismatch_msg = sdm.args[0]
        preamble, existing, new_val = sdm.args[0].split("\n")
        existing = _STREAM_DESCRIPTOR_SPLIT.split(existing[existing.find("=") + 1 :])
        new_val = _STREAM_DESCRIPTOR_SPLIT.split(new_val[new_val.find("=") + 1 :])
        diff = difflib.unified_diff(existing, new_val, n=0)
        new_msg_lines = (
            preamble,
            "(Showing only the mismatch. Full col list saved in the `last_mismatch_msg` attribute of the lib instance.",
            "'-' marks columns missing from the argument, '+' for unexpected.)",
            *(x for x in itertools.islice(diff, 3, None) if not x.startswith("@@")),
        )
        sdm.args = ("\n".join(new_msg_lines),)
        raise


def _assume_true(name, kwargs):
    if name in kwargs and kwargs.get(name) is False:
        return False
    else:
        return True


def _assume_false(name, kwargs):
    if name not in kwargs or kwargs.get(name) is False:
        return False
    else:
        return True


class NativeVersionStore:
    """
    NativeVersionStore objects provide access to ArcticDB libraries, enabling fundamental library operations
    such as list, reading and writing symbols (tables).
    """

    _warned_about_list_version_latest_only_and_snapshot: bool = False

    def __init__(self, library, env, lib_cfg=None, open_mode=OpenMode.DELETE):
        # type: (_Library, Optional[str], Optional[LibraryConfig], OpenMode)->None
        fail_on_missing = library.config.fail_on_missing_custom_normalizer if library.config is not None else False
        custom_normalizer = get_custom_normalizer(fail_on_missing)
        self._initialize(library, env, lib_cfg, custom_normalizer, open_mode)

    def _init_norm_failure_handler(self):
        # init normalization failure handler
        nfh = self._cfg.WhichOneof("norm_failure_handler") if self._cfg is not None else None
        if nfh is None or nfh == "msg_pack":
            if nfh is not None:
                nfh = getattr(self._cfg, nfh, None)

            use_norm_failure_handler_known_types = (
                self._cfg.use_norm_failure_handler_known_types if self._cfg is not None else False
            )

            self._normalizer = CompositeNormalizer(
                MsgPackNormalizer(nfh), use_norm_failure_handler_known_types=use_norm_failure_handler_known_types
            )
        else:
            raise ArcticDbNotYetImplemented("No other normalization failure handler")

    def _initialize(self, library, env, lib_cfg, custom_normalizer, open_mode):
        self._library = library
        self._cfg = library.config
        self.version_store = _PythonVersionStore(self._library)
        self.env = env or "local"
        self._lib_cfg = lib_cfg
        self._custom_normalizer = custom_normalizer
        self._init_norm_failure_handler()
        self._open_mode = open_mode

    @classmethod
    def create_store_from_lib_config(cls, lib_cfg, env, open_mode=OpenMode.DELETE):
        lib = cls.create_lib_from_lib_config(lib_cfg, env, open_mode)
        return cls(library=lib, lib_cfg=lib_cfg, env=env, open_mode=open_mode)

    @staticmethod
    def create_library_config(
            cfg, env, lib_name, encoding_version=EncodingVersion.V1
    ):
        from arcticdb.version_store.helper import extract_lib_config
        lib_cfg = extract_lib_config(cfg.env_by_id[env], lib_name)
        lib_cfg.lib_desc.version.encoding_version = encoding_version
        return lib_cfg

    @classmethod
    def create_store_from_config(
        cls, cfg, env, lib_name, open_mode=OpenMode.DELETE, encoding_version=EncodingVersion.V1
    ):
        lib_cfg = NativeVersionStore.create_library_config(
            cfg, env, lib_name, encoding_version=encoding_version
        )
        lib = cls.create_lib_from_lib_config(lib_cfg, env, open_mode)
        return cls(library=lib, lib_cfg=lib_cfg, env=env, open_mode=open_mode)

    @staticmethod
    def create_lib_from_lib_config(lib_cfg, env, open_mode):
        envs_cfg = _env_config_from_lib_config(lib_cfg, env)
        cfg_resolver = _create_mem_config_resolver(envs_cfg)
        lib_idx = _LibraryIndex.create_from_resolver(env, cfg_resolver)
        return lib_idx.get_library(lib_cfg.lib_desc.name, _OpenMode(open_mode))

    @staticmethod
    def create_lib_from_config(cfg, env, lib_name):
        cfg_resolver = _create_mem_config_resolver(cfg)
        lib_idx = _LibraryIndex.create_from_resolver(env, cfg_resolver)
        return lib_idx.get_library(lib_name, _OpenMode(OpenMode.DELETE))

    def __setstate__(self, state):
        lib_cfg = LibraryConfig()
        lib_cfg.ParseFromString(state["lib_cfg"])
        custom_norm = CompositeCustomNormalizer([], False)
        custom_norm.__setstate__(state["custom_norm"])
        env = state["env"]
        open_mode = state["open_mode"]
        self._initialize(
            library=NativeVersionStore.create_lib_from_lib_config(lib_cfg, env, open_mode),
            env=env,
            lib_cfg=lib_cfg,
            custom_normalizer=custom_norm,
            open_mode=open_mode,
        )

    def __getstate__(self):
        return {
            "env": self.env,
            "lib_cfg": self._lib_cfg.SerializeToString(),
            "custom_norm": self._custom_normalizer.__getstate__() if self._custom_normalizer is not None else "",
            "open_mode": self._open_mode,
        }

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "{}: Library: {}, Primary Storage: {}.".format(
            self.__class__.__name__, self._lib_cfg.lib_desc.name, self.get_backing_store()
        )

    def get_backing_store(self):
        backing_store = ""
        try:
            primary_backing_url = list(self._lib_cfg.storage_by_id.values())[0].config.type_url
            storage_val = re.search("cxx.arctic.org/arcticc.pb2.(.*)_pb2.Config", primary_backing_url)
            backing_store = storage_val.group(1)
        except Exception as e:
            log.error("Could not get primary backing store for lib due to: {}".format(e))
        return backing_store

    def _try_normalize(self, symbol, dataframe, metadata, pickle_on_failure, dynamic_strings, coerce_columns, **kwargs):
        dynamic_schema = self.resolve_defaults(
            "dynamic_schema", self._lib_cfg.lib_desc.version.write_options, False, **kwargs
        )
        try:
            udm = normalize_metadata(metadata) if metadata is not None else None
            opt_custom = self._custom_normalizer.normalize(dataframe)
            if opt_custom is not None:
                item, custom_norm_meta = opt_custom
                item, norm_meta = self._normalizer.normalize(item)
                norm_meta.custom.CopyFrom(custom_norm_meta)
            else:
                # TODO: just for pandas dataframes for now.
                item, norm_meta = self._normalizer.normalize(
                    dataframe,
                    pickle_on_failure=pickle_on_failure,
                    dynamic_strings=dynamic_strings,
                    coerce_columns=coerce_columns,
                    dynamic_schema=dynamic_schema,
                    **kwargs,
                )
        except ArcticDbNotYetImplemented as ex:
            log.error("Not supported: normalizing symbol={}, data={}, metadata={}, {}", symbol, dataframe, metadata, ex)
            raise
        except Exception as ex:
            log.error("Error while normalizing symbol={}, data={}, metadata={}, {}", symbol, dataframe, metadata, ex)
            raise ArcticNativeException(str(ex))

        if norm_meta is None:
            raise ArcticNativeException("Cannot normalize input {}".format(symbol))

        return udm, item, norm_meta

    @staticmethod
    def check_symbol_validity(symbol):
        if not len(symbol) < MAX_SYMBOL_SIZE:
            raise ArcticDbNotYetImplemented(
                f"Symbol length {len(symbol)} chars exceeds the max supported length of {MAX_SYMBOL_SIZE} chars."
            )

        if len(set(symbol).intersection(UNSUPPORTED_S3_CHARS)):
            raise ArcticDbNotYetImplemented(
                f"The symbol '{symbol}' has one or more unsupported characters({','.join(UNSUPPORTED_S3_CHARS)})."
            )

    def _try_flatten_and_write_composite_object(
        self, symbol, data, metadata, pickle_on_failure, dynamic_strings, prune_previous
    ):
        fl = Flattener()
        if fl.can_flatten(data):
            metastruct, to_write = fl.create_meta_structure(data, symbol)
            # Collect all normalized items and metadata for normalization to write.
            items = []
            norm_metas = []
            # No items to write can happen if the entire structure is msgpack serializable. eg: [1,2] doesn't
            # need to go through a multi key process as the msgpack normalizer can handle it as is.
            if len(to_write) > 0:
                for k, v in to_write.items():
                    _, item, norm_meta = self._try_normalize(k, v, None, pickle_on_failure, dynamic_strings, None)
                    items.append(item)
                    norm_metas.append(norm_meta)
                normalized_udm = normalize_metadata(metadata) if metadata is not None else None
                normalized_metastruct = normalize_metadata(metastruct)
                vit_composite = self.version_store.write_versioned_composite_data(
                    symbol,
                    normalized_metastruct,
                    list(to_write.keys()),
                    items,
                    norm_metas,
                    normalized_udm,
                    prune_previous,
                )
                return VersionedItem(
                    symbol=vit_composite.symbol,
                    library=self._library.library_path,
                    version=vit_composite.version,
                    metadata=metadata,
                    data=None,
                    host=self.env,
                )

    def _write_options(self):
        return self._lib_cfg.lib_desc.version.write_options

    @staticmethod
    def resolve_defaults(param_name, proto_cfg, global_default, existing_value=None, uppercase=True, **kwargs):
        """
        Precedence: existing_value > kwargs > env > proto_cfg > global_default

        Parameters
        ----------
        param_name: str
        proto_cfg
            Gets the param_name attribute of this object
            Most often is `self._write_options()` for the Protobuf write_options.
        global_default
            FUTURE: store this in a central location
        existing_value:
            The value already supplied to the caller
        uppercase
            If true (default), will look for `param_name.upper()` in OS environment variables; otherwise, the original
            case.
        kwargs
            For passing through the caller's kwargs in which we look for `param_name`
            *Deprecating: use `existing_value`*
        """

        if existing_value is not None:
            return existing_value

        param_value = kwargs.get(param_name)
        if param_value is not None:
            return param_value

        env_name = param_name.upper() if uppercase else param_name
        env_value = os.getenv(env_name)
        if env_value is not None:
            return env_value not in ("", "0") and not env_value.lower().startswith("f")

        try:
            config_value = getattr(proto_cfg, param_name)
            if config_value is not None:
                return config_value
        except AttributeError:
            pass

        return global_default

    def write(
        self,
        symbol: str,
        data: Any,
        metadata: Optional[Any] = None,
        prune_previous_version: Optional[bool] = None,
        pickle_on_failure: Optional[bool] = None,
        validate_index: bool = False,
        **kwargs,
    ) -> Optional[VersionedItem]:
        """
        Write `data` to the specified `symbol`. If `symbol` already exists then a new version will be created to
        reference the newly written data. For more information on versions see the documentation for the `read`
        primitive.

        Pandas DataFrames, Pandas Series and Numpy NDArrays will be normalised into a common structure suitable for
        storage. Data that cannot be normalised can be written by pickling the data, however pickled data
        consumes more storage space, is less performant for reads and writes and does not support advanced query
        features. Pickling is therefore only supported via the `pickle_on_failure` flag.

        Normalised data will be divided into segments that are deduplicated against storage prior to write. As a result,
        if `data` contains only slight changes compared to pre-existing versions only the delta will be written.

        Note that `write` is not designed for multiple concurrent writers over a single symbol.

        Note: ArcticDB will use the 0-th level index of the Pandas DataFrame for its on-disk index.

        Any non-`DatetimeIndex` will converted into an internal `RowCount` index. That is, ArcticDB will assign each
        row a monotonically increasing integer identifier and that will be used for the index.

        Parameters
        ----------
        symbol : `str`
            Symbol name. Limited to 255 characters. The following characters are not supported in symbols:
            "*", "&", "<", ">"
        data : `Union[pd.DataFrame, pd.Series, np.array]`
            Data to be written.
        metadata : `Optional[Any]`, default=None
            Optional metadata to persist along with the symbol.
        prune_previous_version : `bool`, default=True
            Removes previous (non-snapshotted) versions from the database.
        pickle_on_failure: `bool`, default=False
            Pickle `data` if it can't be normalized.
        validate_index: bool, default=False
            If True, will verify that the index of `data` supports date range searches and update operations. This in effect tests that the data is sorted in ascending order.
            ArcticDB relies on Pandas to detect if data is sorted - you can call DataFrame.index.is_monotonic_increasing on your input DataFrame to see if Pandas believes the
            data to be sorted
        kwargs :
            passed through to the write handler

        Returns
        -------
        Optional[VersionedItem]
            Structure containing metadata and version number of the written symbol in the store.
            The data attribute will not be populated.

        Raises
        ------
        UnsortedDataException
            If data is unsorted, when validate_index is set to True.

        Examples
        --------

        >>> df = pd.DataFrame({'column': [5,6,7]})
        >>> lib.write("symbol", df, metadata={'my_dictionary': 'is_great'})
        >>> lib.read("symbol").data
           column
        0       5
        1       6
        2       7
        """
        self.check_symbol_validity(symbol)
        proto_cfg = self._lib_cfg.lib_desc.version.write_options

        dynamic_strings = self._resolve_dynamic_strings(kwargs)

        pickle_on_failure = self.resolve_defaults(
            "pickle_on_failure", proto_cfg, global_default=False, existing_value=pickle_on_failure, **kwargs
        )
        prune_previous_version = self.resolve_defaults(
            "prune_previous_version", proto_cfg, global_default=False, existing_value=prune_previous_version, **kwargs
        )
        recursive_normalizers = self.resolve_defaults(
            "recursive_normalizers", proto_cfg, global_default=False, uppercase=False, **kwargs
        )
        parallel = self.resolve_defaults("parallel", proto_cfg, global_default=False, uppercase=False, **kwargs)
        incomplete = self.resolve_defaults("incomplete", proto_cfg, global_default=False, uppercase=False, **kwargs)

        # TODO remove me when dynamic strings is the default everywhere
        if parallel:
            dynamic_strings = True

        coerce_columns = kwargs.get("coerce_columns", None)
        sparsify_floats = kwargs.get("sparsify_floats", False)

        _handle_categorical_columns(symbol, data, False)

        log.debug(
            "Writing with pickle_on_failure={}, prune_previous_version={}, recursive_normalizers={}",
            pickle_on_failure,
            prune_previous_version,
            recursive_normalizers,
        )

        # Do a multi_key write if the structured is nested and is not trivially normalizable via msgpack.
        if recursive_normalizers:
            vit = self._try_flatten_and_write_composite_object(
                symbol, data, metadata, pickle_on_failure, dynamic_strings, prune_previous_version
            )
            if isinstance(vit, VersionedItem):
                return vit

        udm, item, norm_meta = self._try_normalize(
            symbol, data, metadata, pickle_on_failure, dynamic_strings, coerce_columns
        )
        # TODO: allow_sparse for write_parallel / recursive normalizers as well.
        if isinstance(item, NPDDataFrame):
            if parallel:
                self.version_store.write_parallel(symbol, item, norm_meta, udm)
                return None
            elif incomplete:
                self.version_store.append_incomplete(symbol, item, norm_meta, udm)
                return None
            else:
                vit = self.version_store.write_versioned_dataframe(
                    symbol, item, norm_meta, udm, prune_previous_version, sparsify_floats, validate_index
                )

            return VersionedItem(
                symbol=vit.symbol,
                library=self._library.library_path,
                version=vit.version,
                metadata=metadata,
                data=None,
                host=self.env,
            )

    def _resolve_dynamic_strings(self, kwargs):
        proto_cfg = self._lib_cfg.lib_desc.version.write_options
        if IS_WINDOWS:
            # Fixed size strings not implemented yet for Windows as Py_UNICODE_SIZE is 2 whereas on Linux it is 4
            normal_value = self.resolve_defaults("dynamic_strings", proto_cfg, global_default=True, **kwargs)
            if not normal_value:
                log.debug(
                    "Windows only supports dynamic_strings=True, using dynamic strings despite configuration or kwarg"
                )
            dynamic_strings = True
        else:
            dynamic_strings = self.resolve_defaults("dynamic_strings", proto_cfg, global_default=False, **kwargs)
        return dynamic_strings

    last_mismatch_msg: Optional[str] = None

    def append(
        self,
        symbol: str,
        dataframe: TimeSeriesType,
        metadata: Optional[Any] = None,
        incomplete: bool = False,
        prune_previous_version: bool = False,
        validate_index: bool = False,
        **kwargs,
    ) -> Optional[VersionedItem]:
        # FUTURE: use @overload and Literal for the existence of the return value once we ditch Python 3.6
        """
        Appends the given data to the existing, stored data. Appends always appends along the index. A new version will
        be created to reference the newly-appended data. Append only accepts data for which the index of the first row
        is equal to or greater than the index of the last row in the existing data.

        Appends containing differing column sets to the existing data are only possible if the library has been
        configured to support dynamic schema.

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        dataframe : `TimeSeriesType`
            Data to be appended.
        metadata : `Optional[Any]`, default=None
            Optional metadata to persist along with the new symbol version. Note that the metadata is
            not combined in any way with the metadata stored in the previous version.
        prune_previous_version
            Removes previous (non-snapshotted) versions from the database.
        validate_index: bool, default=False
            If True, will verify that resulting symbol will support date range searches and update operations. This in effect tests that the previous version of the
            data and `data` are both sorted in ascending order. ArcticDB relies on Pandas to detect if data is sorted - you can call DataFrame.index.is_monotonic_increasing
            on your input DataFrame to see if Pandas believes the data to be sorted
        kwargs :
            passed through to the write handler

        Returns
        -------
        Optional[VersionedItem]
            Structure containing metadata and version number of the written symbol in the store.
            The data attribute will not be populated.

            Nothing is returned if `incomplete` is set because no new version will be written.

        Raises
        ------
        UnsortedDataException
            If data is unsorted, when validate_index is set to True.

        Examples
        --------

        >>> df = pd.DataFrame({'column': [1,2,3]}, index=pd.date_range(start='1/1/2018', end='1/3/2018'))
        >>> df
                    column
        2018-01-01       1
        2018-01-02       2
        2018-01-03       3
        >>> lib.write("symbol", df)
        >>> to_append_df = pd.DataFrame({'column': [4,5,6]}, index=pd.date_range(start='1/4/2018', end='1/6/2018'))
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
        self.check_symbol_validity(symbol)

        dynamic_strings = self._resolve_dynamic_strings(kwargs)
        coerce_columns = kwargs.get("coerce_columns", None)

        _handle_categorical_columns(symbol, dataframe)

        udm, item, norm_meta = self._try_normalize(symbol, dataframe, metadata, False, dynamic_strings, coerce_columns)

        write_if_missing = kwargs.get("write_if_missing", True)

        if isinstance(item, NPDDataFrame):
            with _diff_long_stream_descriptor_mismatch(self):
                if incomplete:
                    self.version_store.append_incomplete(symbol, item, norm_meta, udm)
                else:
                    vit = self.version_store.append(
                        symbol, item, norm_meta, udm, write_if_missing, prune_previous_version, validate_index
                    )
                    return VersionedItem(
                        symbol=vit.symbol,
                        library=self._library.library_path,
                        version=vit.version,
                        metadata=metadata,
                        data=None,
                        host=self.env,
                    )

    def update(
        self,
        symbol: str,
        data: TimeSeriesType,
        metadata: Any = None,
        date_range: Optional[DateRangeInput] = None,
        upsert: bool = False,
        prune_previous_version: bool = False,
        **kwargs,
    ) -> VersionedItem:
        """
        Overwrites existing symbol data with the contents of `data`. The entire range between the first and last index
        entry in `data` is replaced in its entirety with the contents of `data`, adding additional index entries if
        required. `update` only operates over the outermost index level - this means secondary index rows will be
        removed if not contained in `data`.

        Both the existing symbol version and `data` must be timeseries-indexed.

        Update is idempotent.

        Parameters
        ----------
        symbol: `str`
            Symbol name.
        data: `TimeSeriesType`
            Timeseries indexed data to use for the update.
        metadata: `Any`, default=None
            Optional metadata to persist along with the new symbol version. Note that the metadata is
            not combined in any way with the metadata stored in the previous version.
        date_range: None, or one of the types in DateRangeInput
            If a range is specified, it will clear/delete the data within the
            range and overwrite it with the data in `data`. This allows the user
            to update with data that might only be a subset of the
            original data.
        upsert: bool, default=False
            If True, will write the data even if the symbol does not exist.
        prune_previous_version
            Removes previous (non-snapshotted) versions from the database.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the written symbol in the store.
            The data attribute will be None.

        Examples
        --------

        >>> df = pd.DataFrame({'column': [1,2,3,4]}, index=pd.date_range(start='1/1/2018', end='1/4/2018'))
        >>> df
                    column
        2018-01-01       1
        2018-01-02       2
        2018-01-03       3
        2018-01-04       4
        >>> lib.write("symbol", df)
        >>> update_df = pd.DataFrame({'column': [400, 50]}, index=pd.date_range(start='1/1/2018', end='1/3/2018', freq='2D'))
        >>> update_df
                    column
        2018-01-01     400
        2018-01-03      40
        >>> lib.update("symbol", update_df)
        >>> lib.read("symbol").data  # Note that 2018-01-02 is removed despite not being part of `update_df`
                    column
        2018-01-01     400
        2018-01-03      40
        2018-01-04       4
        """
        self.check_symbol_validity(symbol)
        update_query = _PythonVersionStoreUpdateQuery()
        dynamic_strings = self._resolve_dynamic_strings(kwargs)
        dynamic_schema = self.resolve_defaults(
            "dynamic_schema", self._lib_cfg.lib_desc.version.write_options, False, **kwargs
        )
        coerce_columns = kwargs.get("coerce_columns", None)

        if date_range is not None:
            start, end = normalize_dt_range_to_ts(date_range)
            update_query.row_filter = _IndexRange(start.value, end.value)
            data = restrict_data_to_date_range_only(data, start=start, end=end)

        _handle_categorical_columns(symbol, data)

        udm, item, norm_meta = self._try_normalize(symbol, data, metadata, False, dynamic_strings, coerce_columns)

        if isinstance(item, NPDDataFrame):
            with _diff_long_stream_descriptor_mismatch(self):
                vit = self.version_store.update(
                    symbol, update_query, item, norm_meta, udm, upsert, dynamic_schema, prune_previous_version
                )
            return VersionedItem(
                symbol=vit.symbol,
                library=self._library.library_path,
                version=vit.version,
                metadata=metadata,
                data=None,
                host=self.env,
            )

    def create_column_stats(
        self, symbol: str, column_stats: Dict[str, Set[str]], as_of: Optional[VersionQueryInput] = None
    ) -> None:
        """
        Calculates the specified column statistics for each row-slice for the given symbol. In the future, these
        statistics will be used by `QueryBuilder` filtering operations to reduce the number of data segments read out
        of storage.

        Parameters
        ----------
        symbol: `str`
            Symbol name.
        column_stats: `Dict[str, Set[str]]`
            The column stats to create.
            Keys are column names.
            Values are sets of statistic types to build for that column. Options are:
                "MINMAX" : store the minimum and maximum value for the column in each row-slice
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        None
        """
        column_stats = self._get_column_stats(column_stats)
        version_query = self._get_version_query(as_of)
        self.version_store.create_column_stats_version(symbol, column_stats, version_query)

    def drop_column_stats(
        self, symbol: str, column_stats: Optional[Dict[str, Set[str]]] = None, as_of: Optional[VersionQueryInput] = None
    ) -> None:
        """
        Deletes the specified column statistics for the given symbol.

        Parameters
        ----------
        symbol: `str`
            Symbol name.
        column_stats: `Optional[Dict[str, Set[str]]], default=None`
            The column stats to drop. If not provided, all column stats will be dropped.
            See documentation of `create_column_stats` method for more details.
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        None
        """
        column_stats = self._get_column_stats(column_stats)
        version_query = self._get_version_query(as_of)
        self.version_store.drop_column_stats_version(symbol, column_stats, version_query)

    def read_column_stats(self, symbol: str, as_of: Optional[VersionQueryInput] = None, **kwargs) -> pd.DataFrame:
        """
        Read all the column statistics data that has been generated for the given symbol.

        Parameters
        ----------
        symbol: `str`
            Symbol name.
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        `pandas.DataFrame`
            DataFrame representing the stored column statistics for each row-slice in a human-readable format.
        """
        version_query = self._get_version_query(as_of, **kwargs)
        data = denormalize_dataframe(self.version_store.read_column_stats_version(symbol, version_query))
        return data

    def get_column_stats_info(
        self, symbol: str, as_of: Optional[VersionQueryInput] = None, **kwargs
    ) -> Dict[str, Set[str]]:
        """
        Read the column statistics dictionary for the given symbol.

        Parameters
        ----------
        symbol: `str`
            Symbol name.
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        `Dict[str, Set[str]]`
            A dict from column names to sets of column stats that have been generated for that column.
            In the same format as the `column_stats` argument provided to `create_column_stats` and `drop_column_stats`.
        """
        version_query = self._get_version_query(as_of, **kwargs)
        return self.version_store.get_column_stats_info_version(symbol, version_query).to_map()

    def _batch_read_keys(self, atom_keys):
        for result in self.version_store.batch_read_keys(atom_keys):
            read_result = ReadResult(*result)
            vitem = self._adapt_read_res(read_result)
            yield vitem

    def trim(self) -> None:
        """
        Calls trim on the allocator of the underlying version_store
        Should be called after gc.collect() to remove any reference cycles
        """
        self.version_store.trim()

    def batch_read(
        self,
        symbols: List[str],
        as_ofs: Optional[List[VersionQueryInput]] = None,
        date_ranges: Optional[List[Optional[DateRangeInput]]] = None,
        query_builder: Optional[Union[QueryBuilder, List[QueryBuilder]]] = None,
        columns: Optional[List[List[str]]] = None,
        **kwargs,
    ) -> Dict[str, VersionedItem]:
        """
        Reads multiple symbols in a batch fashion. This is more efficient than making multiple `read` calls in succession
        as some constant-time operations can be executed only once rather than once for each element of `symbols`.

        Parameters
        ----------
        symbols: `List[str]`
            List of symbols to read
        as_ofs: `Optional[List[VersionQueryInput]]`, default=None
            List of version queries. See documentation of `read` method for more details.
            i-th entry corresponds to i-th element of `symbols`.
        date_ranges: `Optional[List[Optional[DateRangeInput]]]`, default=None
            List of date ranges to filter the symbols.
            i-th entry corresponds to i-th element of `symbols`.
        columns: `List[List[str]]`, default=None
            Which columns to return for a dataframe.
            i-th entry restricts columns for the i-th element in `symbols`.
        query_builder: `Optional[Union[QueryBuilder, List[QueryBuilder]]]`, default=None
            Either a single QueryBuilder object to apply to all of the dataframes before they are
            returned, or a list of QueryBuilder objects of the same length as the symbols list.
            For more information see the documentation for the QueryBuilder class.
            i-th entry corresponds to i-th element of `symbols`.

        Examples
        --------

        >>> lib.batch_read(
                ['1', '2', '3'],  # Three symbols to read in batch mode
                date_ranges=[
                    (pd.Timestamp('2000-01-01 00:00:00'), pd.Timestamp('2000-02-01 00:00:00'))
                ]*3  # Note that number of date ranges must match number of symbols
            )

        Returns
        -------
        Dict
            Dictionary of symbol mapping with the versioned items
        """
        _check_batch_kwargs(NativeVersionStore.batch_read, NativeVersionStore.read, kwargs)
        throw_on_error = True
        versioned_items = self._batch_read_to_versioned_items(
            symbols, as_ofs, date_ranges, columns, query_builder, throw_on_error, kwargs
        )
        check(
            all(v is not None for v in versioned_items),
            "Null value from _batch_read_to_versioned_items. NoDataFoundException should have been thrown instead.",
        )
        return {v.symbol: v for v in versioned_items}

    def _batch_read_to_versioned_items(
        self, symbols, as_ofs, date_ranges, columns, query_builder, throw_on_error, kwargs=None
    ):
        if kwargs is None:
            kwargs = dict()
        version_queries = self._get_version_queries(len(symbols), as_ofs, **kwargs)
        read_queries = self._get_read_queries(len(symbols), date_ranges, columns, query_builder)
        read_options = self._get_read_options(**kwargs)
        read_options.set_batch_throw_on_error(throw_on_error)
        read_results = self.version_store.batch_read(symbols, version_queries, read_queries, read_options)
        versioned_items = []
        for i in range(len(read_results)):
            if isinstance(read_results[i], DataError):
                versioned_items.append(read_results[i])
            else:
                read_result = ReadResult(*read_results[i])
                read_query = read_queries[i]
                query = None
                if query_builder is not None:
                    query = query_builder if isinstance(query_builder, QueryBuilder) else query_builder[i]
                vitem = self._post_process_dataframe(read_result, read_query, query)
                versioned_items.append(vitem)
        return versioned_items

    def batch_read_metadata(
        self, symbols: List[str], as_ofs: Optional[List[VersionQueryInput]] = None, **kwargs
    ) -> Dict[str, VersionedItem]:
        """
        Reads the metadata for multiple symbols in a batch fashion. This is more efficient than making multiple
        `read_metadata` calls in succession as some constant-time operations can be executed only once rather than once
        for each element of `symbols`.
        Will raise if any of the symbols do not exist.

        Parameters
        ----------
        symbols: `List[str]`
            List of symbols to read the metadata for. Elements should not be duplicated.
        as_ofs: `Optional[List[VersionQueryInput]]`, default=None
            List of version queries. See documentation of `read` method for more details.
            i-th entry corresponds to i-th element of `symbols`.

        Examples
        --------

        >>> lib.batch_read_metadata(
                ['1', '2', '3'],  # Three symbols to read in batch mode
                as_ofs=[32, 33, 34]  # Note that number of as_ofs must match number of symbols
            )

        Returns
        -------
        Dict
            Dictionary of symbol mapping with the versioned items. The data attribute will be None.
        """
        _check_batch_kwargs(NativeVersionStore.batch_read_metadata, NativeVersionStore.read_metadata, kwargs)
        include_errors_and_none_meta = False
        meta_items = self._batch_read_metadata_to_versioned_items(
            symbols, as_ofs, include_errors_and_none_meta, **kwargs
        )
        return {v.symbol: v for v in meta_items}

    def _batch_read_metadata_to_versioned_items(self, symbols, as_ofs, include_errors_and_none_meta, **kwargs):
        version_queries = self._get_version_queries(len(symbols), as_ofs, **kwargs)
        read_options = self._get_read_options(**kwargs)
        # For historical reasons, NativeVersionStore.batch_read_metadata returns None if the requested version does not
        # exist, but should throw an exception for other errors. Library.read_metadata_batch should get DataError
        # objects if exceptions are thrown.
        read_options.set_batch_throw_on_error(not include_errors_and_none_meta)
        metadatas_or_errors = self.version_store.batch_read_metadata(symbols, version_queries, read_options)
        meta_items = []
        for metadata in metadatas_or_errors:
            if isinstance(metadata, DataError) and include_errors_and_none_meta:
                meta_items.append(metadata)
            if not isinstance(metadata, DataError):
                vitem, udm = metadata
                if udm is not None or include_errors_and_none_meta:
                    meta = denormalize_user_metadata(udm, self._normalizer) if udm is not None else None
                    meta_items.append(
                        VersionedItem(
                            symbol=vitem.symbol,
                            library=self._library.library_path,
                            data=None,
                            version=vitem.version,
                            metadata=meta,
                            host=self.env,
                        )
                    )
        return meta_items

    def batch_read_metadata_multi(
        self, symbols: List[str], as_ofs: Optional[List[VersionQueryInput]] = None, **kwargs
    ) -> Dict[str, Dict[int, VersionedItem]]:
        """
        Reads the metadata for multiple symbols in a batch fashion. This is more efficient than making multiple
        `read_metadata` calls in succession as some constant-time operations can be executed only once rather than once
        for each element of `symbols`.
        Will raise if any of the symbols do not exist.
        Behaviour is the same as batch_read_metadata, but allows the metadata for multiple versions of the same symbol
        to be read in one call.

        Parameters
        ----------
        symbols: `List[str]`
            List of symbols to read the metadata for. Elements can be duplicated.
        as_ofs: `Optional[List[VersionQueryInput]]`, default=None
            List of version queries. See documentation of `read` method for more details.
            i-th entry corresponds to i-th element of `symbols`.

        Examples
        --------

        >>> lib.batch_read_metadata_multi(
                ['1', '1', '2'],  # Three symbols to read in batch mode
                as_ofs=[32, 33, 34]  # Note that number of as_ofs must match number of symbols
            )

        Returns
        -------
        Dict
            Dictionary of symbol mapped to dictionary of [int, VersionedItem], where the int represents the version for
            each VersionedItem for each symbol. The data attribute will be None.
        """
        _check_batch_kwargs(NativeVersionStore.batch_read_metadata, NativeVersionStore.read_metadata, kwargs)
        results_dict = {}
        version_queries = self._get_version_queries(len(symbols), as_ofs, **kwargs)
        read_options = self._get_read_options(**kwargs)
        read_options.set_batch_throw_on_error(True)
        for result in self.version_store.batch_read_metadata(symbols, version_queries, read_options):
            vitem, udm = result
            meta = denormalize_user_metadata(udm, self._normalizer) if udm else None
            if vitem.symbol not in results_dict:
                results_dict[vitem.symbol] = {}

            results_dict[vitem.symbol][vitem.version] = VersionedItem(
                symbol=vitem.symbol,
                library=self._library.library_path,
                data=None,
                version=vitem.version,
                metadata=meta,
                host=self.env,
            )

        return results_dict

    def _convert_thin_cxx_item_to_python(self, v) -> VersionedItem:
        """Convert a cxx versioned item that does not contain data or metadata to a Python equivalent."""
        return VersionedItem(
            symbol=v.symbol,
            library=self._library.library_path,
            data=None,
            version=v.version,
            metadata=None,
            host=self.env,
        )

    def batch_write(
        self,
        symbols: List[str],
        data_vector: List[Any],
        metadata_vector: Optional[List[Any]] = None,
        prune_previous_version=None,
        pickle_on_failure=None,
        validate_index: bool = False,
        **kwargs,
    ) -> List[VersionedItem]:
        """
        Write data to multiple symbols in a batch fashion. This is more efficient than making multiple `write` calls in
        succession as some constant-time operations can be executed only once rather than once for each element of
        `symbols`.
        Note that this isn't an atomic operation - it's possible for one symbol to be fully written and readable before
        another symbol.

        Parameters
        ----------
        symbols : `List[str]`
            List of symbols to be written in batch. Elements should not be duplicated.
        data_vector : `List[Any]`
            Data to be written.
            i-th entry corresponds to i-th element of `symbols`.
        metadata_vector : `Optional[List[Any]]`, default=None
            Metadata to be written.
            i-th entry corresponds to i-th element of `symbols`.
        prune_previous_version : `Optional[bool]`, default=None
            Remove previous versions from version list. Uses library default if left as None.
        pickle_on_failure : `Optional[bool]`, default=None
            Pickle results if normalization fails. Uses library default if left as None.
        validate_index: bool, default=False
            If set to True, it will verify for each entry in the batch whether the index of the data supports date range searches and update operations.
            This in effect tests that the data is sorted in ascending order. ArcticDB relies on Pandas to detect if data is sorted -
            you can call DataFrame.index.is_monotonic_increasing on your input DataFrame to see if Pandas believes the data to be sorted
        kwargs :
            passed through to the write handler

        Examples
        --------

        >>> lib.batch_write(
                ['1', '2', '3'],  # Three symbols to write in one call
                [df1, df2, df3],  # Three dataframes
            )

        Returns
        -------
        List
            List of versioned items. The data attribute will be None for each versioned item.
            i-th entry corresponds to i-th element of `symbols`.

        Raises
        ------
        UnsortedDataException
            If data is unsorted, when validate_index is set to True.
        """
        _check_batch_kwargs(NativeVersionStore.batch_write, NativeVersionStore.write, kwargs)
        throw_on_error = True
        return self._batch_write_internal(
            symbols,
            data_vector,
            metadata_vector,
            prune_previous_version,
            pickle_on_failure,
            validate_index,
            throw_on_error,
            **kwargs,
        )

    def _batch_write_internal(
        self,
        symbols: List[str],
        data_vector: List[Any],
        metadata_vector: Optional[List[Any]] = None,
        prune_previous_version=None,
        pickle_on_failure=None,
        validate_index: bool = False,
        throw_on_error: bool = True,
        **kwargs,
    ) -> List[VersionedItem]:
        for symbol in symbols:
            self.check_symbol_validity(symbol)

        proto_cfg = self._lib_cfg.lib_desc.version.write_options
        prune_previous_version = self.resolve_defaults(
            "prune_previous_version", proto_cfg, global_default=False, existing_value=prune_previous_version
        )

        dynamic_strings = self._resolve_dynamic_strings(kwargs)
        pickle_on_failure = self.resolve_defaults(
            "pickle_on_failure", proto_cfg, global_default=False, existing_value=pickle_on_failure, **kwargs
        )
        if metadata_vector is None:
            metadata_itr = itertools.repeat(None)
        else:
            metadata_itr = iter(metadata_vector)

        for idx in range(len(symbols)):
            _handle_categorical_columns(symbols[idx], data_vector[idx], False)

        normalized_infos = [
            self._try_normalize(
                symbols[idx],
                data_vector[idx],
                next(metadata_itr),
                pickle_on_failure=pickle_on_failure,
                dynamic_strings=dynamic_strings,
                coerce_columns=None,
            )
            for idx in range(len(symbols))
        ]
        udms = [info[0] for info in normalized_infos]
        items = [info[1] for info in normalized_infos]
        norm_metas = [info[2] for info in normalized_infos]
        cxx_versioned_items = self.version_store.batch_write(
            symbols, items, norm_metas, udms, prune_previous_version, validate_index, throw_on_error
        )
        write_results = []
        for result in cxx_versioned_items:
            if isinstance(result, DataError):
                write_results.append(result)
            else:
                write_results.append(self._convert_thin_cxx_item_to_python(result))
        return write_results

    def _batch_write_metadata_to_versioned_items(
        self, symbols: List[str], metadata_vector: List[Any], prune_previous_version, throw_on_error
    ):
        for symbol in symbols:
            self.check_symbol_validity(symbol)

        proto_cfg = self._lib_cfg.lib_desc.version.write_options
        prune_previous_version = self.resolve_defaults(
            "prune_previous_version", proto_cfg, global_default=False, existing_value=prune_previous_version
        )
        normalized_meta = [normalize_metadata(metadata_vector[idx]) for idx in range(len(symbols))]
        cxx_versioned_items = self.version_store.batch_write_metadata(
            symbols, normalized_meta, prune_previous_version, throw_on_error
        )
        write_metadata_results = []
        for result in cxx_versioned_items:
            if isinstance(result, DataError):
                write_metadata_results.append(result)
            else:
                write_metadata_results.append(self._convert_thin_cxx_item_to_python(result))
        return write_metadata_results

    def batch_write_metadata(
        self, symbols: List[str], metadata_vector: List[Any], prune_previous_version=None
    ) -> List[Union[VersionedItem, DataError]]:
        """
        Write metadata to multiple symbols in a batch fashion. This is more efficient than making multiple `write` calls
        in succession as some constant-time operations can be executed only once rather than once for each element of
        `symbols`.
        Note that this isn't an atomic operation - it's possible for the metadata for one symbol to be fully written and
        readable before another symbol.
        Parameters
        ----------
        symbols : `List[str]`
            List of symbols to be written in batch. Elements should not be duplicated.
        metadata_vector : `List[Any]`
            Metadata to be written.
            i-th entry corresponds to i-th element of `symbols`.
        prune_previous_version : `Optional[bool]`, default=None
            Remove previous versions from version list. Uses library default if left as None.

        Returns
        -------
        List[Union[VersionedItem, DataError]]
            List of versioned items. The data attribute will be None for each versioned item.
            i-th entry corresponds to i-th element of `symbols`. Each result correspond to
            a structure containing metadata and version number of the affected symbol in the store.
            If any internal exception is raised, a DataError object is returned, with symbol,
            error_code, error_category, and exception_string properties.
        """
        throw_on_error = True
        return self._batch_write_metadata_to_versioned_items(
            symbols, metadata_vector, prune_previous_version, throw_on_error
        )

    def batch_append(
        self,
        symbols: List[str],
        data_vector: List[TimeSeriesType],
        metadata_vector: Optional[List[Any]] = None,
        prune_previous_version=None,
        validate_index: bool = False,
        **kwargs,
    ) -> List[VersionedItem]:
        """
        Append data to multiple symbols in a batch fashion. This is more efficient than making multiple `append` calls in
        succession as some constant-time operations can be executed only once rather than once for each element of
        `symbols`.
        Note that this isn't an atomic operation - it's possible for one symbol to be fully written and readable before
        another symbol.
        Parameters
        ----------
        symbols : `List[str]`
            List of symbols to be appended-to in batch. Elements should not be duplicated.
        data_vector : `List[TimeSeriesType]`
            Data to be appended.
            i-th entry corresponds to i-th element of `symbols`.
        metadata_vector : `Optional[List[Any]]`, default=None
            Metadata to be written. Note that the metadata is not combined in any way with the metadata stored in the
            previous version.
            i-th entry corresponds to i-th element of `symbols`.
        prune_previous_version : `Optional[bool]`, default=None
            Remove previous versions from version list. Uses library default if left as None.
        validate_index: bool, default=False
            If set to True, it will verify for each entry in the batch whether the index of the data supports date range searches and update operations.
            This in effect tests that the data is sorted in ascending order. ArcticDB relies on Pandas to detect if data is sorted -
            you can call DataFrame.index.is_monotonic_increasing on your input DataFrame to see if Pandas believes the data to be sorted
        kwargs :
            passed through to the write handler

        Returns
        -------
        List
            List of versioned items. The data attribute will be None for each versioned item.
            i-th entry corresponds to i-th element of `symbols`.

        Raises
        ------
        UnsortedDataException
            If data is unsorted, when validate_index is set to True.
        """
        throw_on_error = True
        _check_batch_kwargs(NativeVersionStore.batch_append, NativeVersionStore.append, kwargs)
        return self._batch_append_to_versioned_items(
            symbols,
            data_vector,
            metadata_vector,
            prune_previous_version,
            validate_index,
            throw_on_error,
            **kwargs,
        )

    def _batch_append_to_versioned_items(
        self,
        symbols,
        data_vector,
        metadata_vector,
        prune_previous_version,
        validate_index,
        throw_on_error,
        **kwargs,
    ):
        for symbol in symbols:
            self.check_symbol_validity(symbol)

        proto_cfg = self._lib_cfg.lib_desc.version.write_options
        prune_previous_version = self.resolve_defaults(
            "prune_previous_version", proto_cfg, global_default=False, existing_value=prune_previous_version
        )
        dynamic_strings = self._resolve_dynamic_strings(kwargs)

        if metadata_vector is None:
            metadata_itr = itertools.repeat(None)
        else:
            metadata_itr = iter(metadata_vector)

        for idx in range(len(symbols)):
            _handle_categorical_columns(symbols[idx], data_vector[idx])

        normalized_infos = [
            self._try_normalize(
                symbols[idx],
                data_vector[idx],
                next(metadata_itr),
                dynamic_strings=dynamic_strings,
                pickle_on_failure=False,
                coerce_columns=None,
            )
            for idx in range(len(symbols))
        ]
        udms = [info[0] for info in normalized_infos]
        items = [info[1] for info in normalized_infos]
        norm_metas = [info[2] for info in normalized_infos]

        write_if_missing = kwargs.get("write_if_missing", True)

        cxx_versioned_items = self.version_store.batch_append(
            symbols,
            items,
            norm_metas,
            udms,
            prune_previous_version,
            validate_index,
            write_if_missing,
            throw_on_error,
        )
        append_results = []
        for result in cxx_versioned_items:
            if isinstance(result, DataError):
                append_results.append(result)
            else:
                append_results.append(self._convert_thin_cxx_item_to_python(result))
        return append_results

    def batch_restore_version(
        self, symbols: List[str], as_ofs: Optional[List[VersionQueryInput]] = None, **kwargs
    ) -> List[VersionedItem]:
        """
        Makes the latest version of the symbols equal to the as_of specified versions.

        Parameters
        ----------
        symbols : `List[str]`
            Symbol names.
        as_ofs : `Optional[List[VersionQueryInput]]`, default=None
            See documentation of `read` method for more details.
            i-th entry corresponds to i-th element of `symbols`.

        Returns
        -------
        List[VersionedItem]
            Includes the version number that was just written.
            i-th entry corresponds to i-th element of `symbols`.
        """
        _check_batch_kwargs(NativeVersionStore.batch_restore_version, NativeVersionStore.restore_version, kwargs)
        version_queries = self._get_version_queries(len(symbols), as_ofs, **kwargs)
        raw_results = self.version_store.batch_restore_version(symbols, version_queries)
        read_results = [ReadResult(*r) for r in raw_results]
        metadatas = [denormalize_user_metadata(read_result.udm, self._normalizer) for read_result in read_results]

        return [
            VersionedItem(
                symbol=result.version.symbol,
                library=self._library.library_path,
                data=None,
                version=result.version.version,
                metadata=meta,
                host=self.env,
            )
            for result, meta in zip(read_results, metadatas)
        ]

    def _get_version_query(self, as_of: VersionQueryInput, **kwargs):
        version_query = _PythonVersionStoreVersionQuery()
        version_query.set_skip_compat(_assume_true("skip_compat", kwargs))
        version_query.set_iterate_on_failure(_assume_false("iterate_on_failure", kwargs))

        if isinstance(as_of, six.string_types):
            version_query.set_snap_name(as_of)
        elif isinstance(as_of, int):
            version_query.set_version(as_of)
        elif isinstance(as_of, (datetime, Timestamp)):
            version_query.set_timestamp(Timestamp(as_of).value)
        elif as_of is not None:
            raise ArcticNativeException("Unexpected combination of read parameters")

        return version_query

    def _get_version_queries(self, num_symbols: int, as_ofs: Optional[List[VersionQueryInput]], **kwargs):
        if as_ofs is not None:
            check(
                len(as_ofs) == num_symbols,
                "Mismatched number of symbols ({}) and as_ofs ({}) supplied to batch read",
                num_symbols,
                len(as_ofs),
            )
            return [self._get_version_query(as_of, **kwargs) for as_of in as_ofs]
        else:
            return [self._get_version_query(None, **kwargs) for _ in range(num_symbols)]

    def _get_read_query(self, date_range: Optional[DateRangeInput], row_range, columns, query_builder):
        check(date_range is None or row_range is None, "Date range and row range both specified")
        read_query = _PythonVersionStoreReadQuery()

        if query_builder:
            read_query.add_clauses(query_builder.clauses)

        if date_range is not None:
            read_query.row_filter = _normalize_dt_range(date_range)

        if row_range is not None:
            read_query.row_range = row_range

        if columns is not None:
            read_query.columns = list(columns)

        return read_query

    def _get_read_queries(
        self,
        num_symbols: int,
        date_ranges: Optional[List[Optional[DateRangeInput]]],
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
            these_columns = None
            query = None
            if date_ranges is not None:
                date_range = date_ranges[idx]
            if columns is not None:
                these_columns = columns[idx]
            if query_builder is not None:
                query = query_builder if isinstance(query_builder, QueryBuilder) else query_builder[idx]
            read_queries.append(self._get_read_query(date_range, None, these_columns, query))

        return read_queries

    def _get_read_options(self, **kwargs):
        proto_cfg = self._lib_cfg.lib_desc.version.write_options
        read_options = _PythonVersionStoreReadOptions()
        read_options.set_force_strings_to_object(_assume_false("force_string_to_object", kwargs))
        read_options.set_optimise_string_memory(_assume_false("optimise_string_memory", kwargs))
        read_options.set_dynamic_schema(
            self.resolve_defaults("dynamic_schema", proto_cfg, global_default=False, **kwargs)
        )
        read_options.set_set_tz(self.resolve_defaults("set_tz", proto_cfg, global_default=False, **kwargs))
        read_options.set_allow_sparse(self.resolve_defaults("allow_sparse", proto_cfg, global_default=False, **kwargs))
        read_options.set_incompletes(self.resolve_defaults("incomplete", proto_cfg, global_default=False, **kwargs))
        return read_options

    def _get_queries(self, as_of, date_range, row_range, columns, query_builder, **kwargs):
        version_query = self._get_version_query(as_of, **kwargs)
        read_options = self._get_read_options(**kwargs)
        read_query = self._get_read_query(date_range, row_range, columns, query_builder)
        return version_query, read_options, read_query

    def _get_column_stats(self, column_stats):
        return None if column_stats is None else _ColumnStats(column_stats)

    def read(
        self,
        symbol: str,
        as_of: Optional[VersionQueryInput] = None,
        date_range: Optional[DateRangeInput] = None,
        row_range: Optional[Tuple[int, int]] = None,
        columns: Optional[List[str]] = None,
        query_builder: Optional[QueryBuilder] = None,
        **kwargs,
    ) -> VersionedItem:
        """
        Read data for the named symbol.

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        as_of : `Optional[VersionQueryInput]`, default=None
            Return the data as it was as_of the point in time. Defaults to getting the latest version.
            `int` : specific version number. Negative indexing is supported, with -1 representing the latest version, -2 the version before that, etc.
            `str` : snapshot name which contains the version
            `datetime.datetime` : the version of the data that existed as_of the requested point in time
        date_range: `Optional[DateRangeInput]`, default=None
            DateRange to read data for.  Applicable only for Pandas data with a DateTime index. Returns only the part
            of the data that falls within the given range. The same effect can be achieved by using the date_range
            clause of the QueryBuilder class, which will be slower, but return data with a smaller memory footprint.
            See the QueryBuilder.date_range docstring for more details.
        row_range: `Optional[Tuple[int, int]]`, default=None
            Row range to read data for. Inclusive of the lower bound, exclusive of the upper bound
            lib.read(symbol, row_range=(start, end)).data should behave the same as df.iloc[start:end], including in
            the handling of negative start/end values. Only one of date_range or row_range can be provided.
        columns: `Optional[List[str]]`, default=None
            Applicable only for Pandas data. Determines which columns to return data for.
        query_builder: 'Optional[QueryBuilder]', default=None
            A QueryBuilder object to apply to the dataframe before it is returned.
            For more information see the documentation for the QueryBuilder class.


        Returns
        -------
        VersionedItem
        """

        if row_range is not None:
            row_range = _SignedRowRange(row_range[0], row_range[1])
        if date_range is not None and query_builder is not None:
            q = QueryBuilder()
            query_builder = q.date_range(date_range).then(query_builder)
        version_query, read_options, read_query = self._get_queries(
            as_of, date_range, row_range, columns, query_builder, **kwargs
        )
        read_result = self._read_dataframe(symbol, version_query, read_query, read_options)
        return self._post_process_dataframe(read_result, read_query, query_builder)

    def head(
        self,
        symbol: str,
        n: Optional[int] = 5,
        as_of: Optional[VersionQueryInput] = None,
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> VersionedItem:
        """
        Read the first n rows of data for the named symbol.
        If n is negative, return all rows EXCEPT the last n rows.

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        n : 'Optional[int]', default=5
            number of rows to select
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.
        columns: `list`
            Which columns to return for a dataframe.

        Returns
        -------
        VersionedItem
        """

        q = QueryBuilder()
        q = q._head(n)
        version_query, read_options, read_query = self._get_queries(as_of, None, None, columns, q, **kwargs)
        read_result = self._read_dataframe(symbol, version_query, read_query, read_options)
        return self._post_process_dataframe(read_result, read_query, q)

    def tail(
        self, symbol: str, n: int = 5, as_of: VersionQueryInput = None, columns: Optional[List[str]] = None, **kwargs
    ) -> VersionedItem:
        """
        Read the last n rows of data for the named symbol.
        If n is negative, return all rows EXCEPT the first n rows.

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        n : 'Optional[int]', default=5
            number of rows to select
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.
        columns: `list`
            Which columns to return for a dataframe.

        Returns
        -------
        VersionedItem
        """

        q = QueryBuilder()
        q = q._tail(n)
        version_query, read_options, read_query = self._get_queries(as_of, None, None, columns, q, **kwargs)
        read_result = self._read_dataframe(symbol, version_query, read_query, read_options)
        return self._post_process_dataframe(read_result, read_query, q)

    def _read_dataframe(self, symbol, version_query, read_query, read_options):
        return ReadResult(*self.version_store.read_dataframe_version(symbol, version_query, read_query, read_options))

    def _post_process_dataframe(self, read_result, read_query, query_builder):
        if read_query.row_filter is not None and (query_builder is None or query_builder.needs_post_processing()):
            # post filter
            start_idx = end_idx = None
            if isinstance(read_query.row_filter, _RowRange):
                start_idx = read_query.row_filter.start - read_result.frame_data.offset
                end_idx = read_query.row_filter.end - read_result.frame_data.offset
            elif isinstance(read_query.row_filter, _IndexRange):
                ts_idx = read_result.frame_data.value.data[0]
                if len(ts_idx) != 0:
                    start_idx = ts_idx.searchsorted(datetime64(read_query.row_filter.start_ts, "ns"), side="left")
                    end_idx = ts_idx.searchsorted(datetime64(read_query.row_filter.end_ts, "ns"), side="right")
            else:
                raise ArcticNativeException("Unrecognised row_filter type: {}".format(type(read_query.row_filter)))
            data = []
            for c in read_result.frame_data.value.data:
                data.append(c[start_idx:end_idx])
            read_result.frame_data = FrameData(data, read_result.frame_data.names, read_result.frame_data.index_columns)

        vitem = self._adapt_read_res(read_result)

        # Handle custom normalized data
        if len(read_result.keys) > 0:
            meta_struct = denormalize_user_metadata(read_result.mmeta)

            key_map = {v.symbol: v.data for v in self._batch_read_keys(read_result.keys)}
            original_data = Flattener().create_original_obj_from_metastruct_new(meta_struct, key_map)

            return VersionedItem(
                symbol=vitem.symbol,
                library=vitem.library,
                data=original_data,
                version=vitem.version,
                metadata=vitem.metadata,
                host=vitem.host,
            )

        return vitem

    def _find_version(
        self, symbol: str, as_of: Optional[VersionQueryInput] = None, raise_on_missing: Optional[bool] = False, **kwargs
    ) -> Optional[VersionedItem]:
        version_query = self._get_version_query(as_of, **kwargs)
        read_options = self._get_read_options(**kwargs)
        version_handle = self.version_store.find_version(symbol, version_query, read_options)

        if version_handle is None and raise_on_missing:
            raise KeyError(f"Cannot find version for symbol={symbol},as_of={as_of}")

        return version_handle

    def read_index(self, symbol: str, as_of: Optional[VersionQueryInput] = None, **kwargs) -> pd.DataFrame:
        """
        Read the index key for the named symbol.

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        Pandas DataFrame representing the index key in a human-readable format.
        """
        version_query = self._get_version_query(as_of, **kwargs)
        data = denormalize_dataframe(self.version_store.read_index(symbol, version_query))
        return data

    def restore_version(self, symbol: str, as_of: Optional[VersionQueryInput] = None, **kwargs) -> VersionedItem:
        """
        Makes the latest version of the symbol equal to the as_of specified version.
        lib.restore_version(sym, as_of) is semantically equivalent o lib.write(sym, lib.read(sym, as_of).data)

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        VersionedItem
            Includes the version number that was just written.
        """
        version_query = self._get_version_query(as_of, **kwargs)
        read_result = ReadResult(*self.version_store.restore_version(symbol, version_query))
        meta = denormalize_user_metadata(read_result.udm, self._normalizer)

        return VersionedItem(
            symbol=read_result.version.symbol,
            library=self._library.library_path,
            data=None,
            version=read_result.version.version,
            metadata=meta,
            host=self.env,
        )

    def list_symbols_with_incomplete_data(self) -> List[str]:
        """
        List all symbols with previously written un-indexed chunks of data, produced by a tick collector or parallel
        writes/appends.

        Returns
        -------
        List[str]
            A list of the symbols with incomplete data segments.
        """
        return list(self.version_store.get_incomplete_symbols())

    def remove_incomplete(self, symbol: str):
        """
        Remove previously written un-indexed chunks of data, produced by a tick collector or parallel
        writes/appends.

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        """
        self.version_store.remove_incomplete(symbol)

    def compact_incomplete(
        self,
        symbol: str,
        append: bool,
        convert_int_to_float: bool,
        via_iteration: Optional[bool] = True,
        sparsify: Optional[bool] = False,
        metadata: Optional[Any] = None,
        prune_previous_version: Optional[bool] = None,
    ):
        """
        Compact previously written un-indexed chunks of data, produced by a tick collector or parallel
        writes/appends.

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        append : `bool`
            Whether the un-indexed chunks of data should be appended to the previous version, or treated like a `write`
            operation.
        convert_int_to_float : `bool`
            Convert integers to floating point type.
        via_iteration: `Optional[bool]`, default=True
            Tick collectors can always write data in the correct order, so don't
            need to iterate the keys, which is faster. Everything else needs to
            set this to True.
        sparsify : `Optional[bool]`, default=False
            Convert data to sparse format (for tick data only)
        metadata : `Optional[Any]`, default=None
            Add user-defined metadata in the same way as write etc
        prune_previous_version
            Removes previous (non-snapshotted) versions from the database.
        Returns
        -------
        VersionedItem
            The data attribute will be None.
        """
        prune_previous_version = self.resolve_defaults(
            "prune_previous_version", self._write_options(), global_default=False, existing_value=prune_previous_version
        )
        udm = normalize_metadata(metadata) if metadata is not None else None
        return self.version_store.compact_incomplete(
            symbol, append, convert_int_to_float, via_iteration, sparsify, udm, prune_previous_version
        )

    @staticmethod
    def _get_index_columns_from_descriptor(descriptor):
        norm_info = descriptor.normalization
        stream_descriptor = descriptor.stream_descriptor
        # For explicit integer indexes, the index is actually present in the first column even though the field_count
        # is 0.
        idx_type = norm_info.df.common.WhichOneof("index_type")
        if idx_type == "index":
            last_index_column_idx = 1 if norm_info.df.common.index.is_not_range_index else 0
        else:
            # The value of field_count is len(index) - 1
            last_index_column_idx = 1 + norm_info.df.common.multi_index.field_count

        index_columns = []
        for field_idx in range(last_index_column_idx):
            index_columns.append(stream_descriptor.fields[field_idx].name)

        return index_columns

    def _adapt_read_res(self, read_result: ReadResult) -> VersionedItem:
        frame_data = FrameData.from_cpp(read_result.frame_data)

        meta = denormalize_user_metadata(read_result.udm, self._normalizer)
        data = self._normalizer.denormalize(frame_data, read_result.norm)
        if read_result.norm.HasField("custom"):
            data = self._custom_normalizer.denormalize(data, read_result.norm.custom)

        return VersionedItem(
            symbol=read_result.version.symbol,
            library=self._library.library_path,
            data=data,
            version=read_result.version.version,
            metadata=meta,
            host=self.env,
        )

    def list_versions(
        self,
        symbol: Optional[str] = None,
        snapshot: Optional[str] = None,
        latest_only: Optional[bool] = False,
        iterate_on_failure: Optional[bool] = False,
        skip_snapshots: Optional[bool] = False,
    ) -> List[Dict]:
        """
        Return a list of versions filtered by the passed in parameters.

        Parameters
        ----------
        symbol : `Optional[str]`, default=None,
            Symbol to return versions for.  If None, returns versions for all of the symbols in the library.
        snapshot : `Optional[str]`, default=None,
            Return the versions contained in the named snapshot.
        latest_only : `bool`
            Only include the latest version for each returned symbol. Has no effect if `snapshot` argument is also
            specified.
        iterate_on_failure: `bool`
            Iterate the type in the storage if the top-level key isn't present.
        skip_snapshots: `bool`
            Don't populate version list with snapshot information.
            Can improve performance significantly if there are many snapshots.

        Examples
        --------

        >>> lib.list_versions()
        >>>
        [{'symbol': '100m',
          'version': 0,
          'date': Timestamp('2021-09-23 11:48:18.053074428+0000', tz='UTC'),
          'deleted': False,
          'snapshots': []},
         {'symbol': '50m',
          'version': 0,
          'date': Timestamp('2021-09-23 11:34:33.560910368+0000', tz='UTC'),
          'deleted': True,
          'snapshots': ["my_snapshot"]},
        ...

        Returns
        -------
        `List[Dict]`
            List of dictionaries describing the discovered versions in the library.
        """
        if latest_only and snapshot and not NativeVersionStore._warned_about_list_version_latest_only_and_snapshot:
            log.warning("latest_only has no effect when snapshot is specified")
            NativeVersionStore._warned_about_list_version_latest_only_and_snapshot = True

        result = self.version_store.list_versions(symbol, snapshot, latest_only, iterate_on_failure, skip_snapshots)
        return [
            {
                "symbol": version_result[0],
                "version": version_result[1],
                "date": to_datetime(version_result[2], unit="ns", utc=True),
                "deleted": version_result[4],
                "snapshots": version_result[3],
            }
            for version_result in result
        ]

    def list_symbols(
        self,
        all_symbols: Optional[bool] = False,
        snapshot: Optional[str] = None,
        regex: Optional[str] = None,
        prefix: Optional[str] = None,
        use_symbol_list: Optional[bool] = None,
    ) -> List[str]:
        """
        Return the symbols in this library.

        Parameters
        ----------
        all_symbols : `Optional[bool]`, default=False
            If True returns all symbols under all snapshots, even if the symbol has been deleted
            in the current version (i.e. it exists under a snapshot... Default: False
        snapshot : `Optional[str]`, default=None
            Return the symbols available under the specified snapshot.
        regex : `Optional[str]`, default=None
            filter symbols by the passed in regular expression
        prefix : `Optional[str]`, default=None
            filter symbols by the given prefix. Only supported by S3 backend for now.
        use_symbol_list: `Optional[bool]`, default=None
            If True, ignore the symbol list and get all symbols using iteration.
            ignored if all_symbols=True

        Examples
        --------

        >>> lib.list_symbols()
        ['test_symbol', 'symbol', '50m']

        Returns
        -------
        `List[str]`
            String list of symbols in the library.
        """
        if all_symbols:
            if use_symbol_list:
                log.warning("Cannot use symbol list with all_symbols=True as it only stores undeleted symbols")
            use_symbol_list = False
        return list(self.version_store.list_streams(snapshot, regex, prefix, use_symbol_list, all_symbols))

    def list_snapshots(self, load_metadata: Optional[bool] = True) -> Dict[str, Any]:
        """
        List the snapshots in the library.

        Parameters
        ----------
        load_metadata : `Optional[bool]`, default=True
            Load the snapshot metadata. May be slow so opt for false if you don't need it.

        Returns
        -------
        `Dict[str, Any]`
            Dictionary mapping snapshot names to their associated metadata, or to Nones if load_metadata is False.
        """
        snap_data = self.version_store.list_snapshots(load_metadata)
        denormalized_snap_data = {}
        for snap_name, normalized_metadata in snap_data:
            metadata = denormalize_user_metadata(normalized_metadata) if normalized_metadata else None
            denormalized_snap_data[snap_name] = metadata

        return denormalized_snap_data

    def snapshot(
        self,
        snap_name: str,
        metadata: Optional[Any] = None,
        skip_symbols: Optional[List[str]] = None,
        versions: Optional[Dict[str, int]] = None,
    ):
        """
        Create a named snapshot of the data within a library.

        By default, the latest version of each active symbol at the time of snapshot creation will be contained within
        the snapshot. The symbols and versions contained within the snapshot will persist regardless of new symbols
        and versions written to the library post snapshot creation.

        Parameters
        ----------
        snap_name : `str`
            Name of the snapshot.
        metadata : `Optional[Any]`, default=None
            Optional metadata to persist along with the snapshot.
        skip_symbols : `Optional[List[str]]`, default=None
            Optional list of symbols to be excluded from the snapshot.
        versions: `Optional[Dict[str, int]]`, default=None
            Optional dictionary of versions of the symbols to include in the snapshot.
        """
        if not skip_symbols:
            skip_symbols = []
        if not versions:
            versions = {}
        metadata = normalize_metadata(metadata) if metadata else None

        self.version_store.snapshot(snap_name, metadata, skip_symbols, versions)

    def delete_snapshot(self, snap_name: str):
        """
        Delete a named snapshot. This may be slow if the given snapshot is the last reference to the underlying
        symbol(s) as the associated data will have to be removed as well.

        Parameters
        ----------
        snap_name : `str`
            The snapshot name to delete.
        """
        self.version_store.delete_snapshot(snap_name)

    def add_to_snapshot(
        self, snap_name: str, symbols: List[str], as_ofs: Optional[List[VersionQueryInput]] = None, **kwargs
    ):
        """
        Add items to a snapshot. Will replace if the snapshot already contains an entry for a particular symbol.

        Parameters
        ----------
        snap_name : `str`
            The snapshot name to modify.
        symbols : `List[str]`
            The symbols to add/modify entries for.
        as_ofs : `Optional[List[VersionQueryInput]]`, default=None
            List of version queries. See documentation of `read` method for more details.
            i-th entry corresponds to i-th element of `symbols`.
        """
        version_queries = self._get_version_queries(len(symbols), as_ofs, **kwargs)
        self.version_store.add_to_snapshot(snap_name, symbols, version_queries)

    def remove_from_snapshot(self, snap_name: str, symbols: List[str], versions: List[int]):
        """
        Remove items from a snapshot

        Parameters
        ----------
        snap_name : `str`
            The snapshot name to modify.
        symbols : List[str]
            The symbols to remove entries for.
        versions : List[int]
            The versions to remove entries for.
            i-th entry corresponds to i-th element of `symbols`.
        """
        self.version_store.remove_from_snapshot(snap_name, symbols, versions)

    def delete(self, symbol: str, date_range: Optional[DateRangeInput] = None, **kwargs):
        """
        Delete all versions of the item from the library which aren't part of at least one snapshot.

        Note that this requires data to be removed from the underlying storage which can be slow if the underlying
        delete primitives offered by the storage are slow.

        Parameters
        ----------
        symbol : `str`
            Symbol name to delete
        date_range: `Optional[DateRangeInput]`, default=None
            If provided, deletes rows that fall within the given range from the latest live version. Note this is
            end-inclusive.
            If not provided then all versions of the symbol will be removed in their entirety (unless accessible via at
            least one snapshot).
        """
        if date_range is not None:
            dynamic_schema = self.resolve_defaults(
                "dynamic_schema", self._lib_cfg.lib_desc.version.write_options, False, **kwargs
            )
            update_query = _PythonVersionStoreUpdateQuery()
            update_query.row_filter = _normalize_dt_range(date_range)
            self.version_store.delete_range(symbol, update_query, dynamic_schema)

        else:
            self.version_store.delete(symbol)

    def delete_version(self, symbol: str, version_num: int, *_):
        """
        Delete the given version of this symbol.

        Parameters
        ----------
        symbol : `str`
            Symbol name.
        version_num : `int`
            Version to be deleted.
        """
        self.version_store.delete_version(symbol, version_num)

    def prune_previous_versions(self, symbol: str):
        """
        Removes all (non-snapshotted) versions from the database for the given symbol, except the latest.

        Parameters
        ----------
        symbol : `str`
            Symbol name to prune.
        """
        self.version_store.prune_previous_versions(symbol)

    def _prune_previous_versions(
        self, symbol: str, keep_mins: Optional[int] = 120, keep_version: Optional[int] = None, *_
    ):
        """
        Removes all (non-snapshotted) versions older than keep_mins from the database for the given symbol, except the
        latest.

        Parameters
        ----------
        symbol : `str`
            symbol name to prune
        keep_mins : 'Optional[int]', default=120
            Delete versions older than keep_mins
        keep_version:  `Optional[int]`, default=None
            Version number not to be deleted
        """

        versions = [ver for ver in self.list_versions(symbol) if ver["deleted"] is False]
        # We never prune the latest version
        if len(versions) < 2:
            return
        if not all(versions[i]["version"] > versions[i + 1]["version"] for i in range(len(versions) - 1)):
            log.error("Unexpected version list received, not sorted. Aborting prune previous")
            return
        versions = versions[1:]
        versions = filter(lambda v: v["version"] != keep_version, versions)
        keep_date = Timestamp.utcnow() - Timedelta(minutes=keep_mins)
        delete_versions = filter(lambda v: v["date"] < keep_date, versions)

        for v_info in delete_versions:
            log.info("Deleting version: {}".format(str(v_info["version"])))
            self.delete_version(symbol, v_info["version"])
            log.info("Done deleting version: {}".format(str(v_info["version"])))

    def has_symbol(
        self, symbol: str, as_of: Optional[VersionQueryInput] = None, iterate_on_failure: Optional[bool] = False
    ) -> bool:
        """
        Return True if the 'symbol' exists in this library AND the symbol isn't deleted in the specified as_of.
        It's possible for a deleted symbol to exist in snapshots.

        Parameters
        ----------
        symbol : `str`
            symbol name
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.
        iterate_on_failure: `Optional[bool]`, default=False
            Iterate the type in the storage if the top-level key isn;t present

        Returns
        -------
        `bool`
            True if the symbol exists as_of the specified revision, False otherwise.
        """

        return (
            self._find_version(symbol, as_of=as_of, raise_on_missing=False, iterate_on_failure=iterate_on_failure)
            is not None
        )

    def column_names(self, symbol: str, as_of: Optional[VersionQueryInput] = None) -> List[str]:
        """
        Get the column names of the data for a given symbol.

        Parameters
        ----------
        symbol : `str`
            Symbol name
        as_of : `Optional[VersionQueryInput]`, default=None,
            See documentation of `read` method for more details.

        Returns
        -------
        `List[str]`
            A list of column names associated with the symbol as of the specified revision.
        """
        return self.get_info(symbol, version=as_of)["col_names"]["columns"]

    def update_time(self, symbol: str, as_of: Optional[VersionQueryInput] = None) -> datetime64:
        """
        Get the time of the most recent update for a symbol in a given version.

        Parameters
        ----------
        symbol : `str`
            symbol name
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        `datetime64`
            The time that the specified revision of the symbol was updated.
        """
        version_query = self._get_version_query(as_of)
        return datetime64(self.version_store.get_update_time(symbol, version_query), "ns")

    def update_times(self, symbols: List[str], as_ofs: Optional[List[VersionQueryInput]] = None) -> List[datetime64]:
        """
        Get the time of the most recent update for a list of symbols in given versions.

        Parameters
        ----------
        symbols : `List[str]`
            symbol names
        as_ofs: `Optional[List[VersionQueryInput]]`, default=None
            See documentation of `read` method for more details.
            i-th entry corresponds to i-th element of `symbols`.

        Returns
        -------
        `List[datetime64]`
            List of the times that the specified revisions of the symbols were updated.
        """
        version_queries = self._get_version_queries(len(symbols), as_ofs)
        return [datetime64(t, "ns") for t in self.version_store.get_update_times(symbols, version_queries)]

    def read_metadata(self, symbol: str, as_of: Optional[VersionQueryInput] = None, **kwargs) -> VersionedItem:
        """
        Return the metadata saved for a symbol.  This method is fast as it doesn't actually load the data.

        Parameters
        ----------
        symbol : `str`
            symbol name
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        `VersionedItem`
            Structure including the metadata read from the store.
            The data attribute will not be populated.
        """
        version_query = self._get_version_query(as_of, **kwargs)
        read_options = self._get_read_options(**kwargs)
        version_item, udm = self.version_store.read_metadata(symbol, version_query, read_options)
        meta = denormalize_user_metadata(udm, self._normalizer) if udm else None

        return VersionedItem(
            symbol=symbol,
            library=self._library.library_path,
            data=None,
            version=version_item.version,
            metadata=meta,
            host=self.env,
        )

    def get_type(self) -> str:
        return self._lib_cfg.lib_desc.WhichOneof("store_type")

    def get_normalizer_for_item(self, item):
        """
        Useful util to get the normalizer that will be used for any given item.
        """
        return self._normalizer.get_normalizer_for_type(item)

    def will_item_be_pickled(self, item):
        try:
            _udm, _item, norm_meta = self._try_normalize(
                symbol="",
                dataframe=item,
                metadata=None,
                pickle_on_failure=False,
                dynamic_strings=False,  # TODO: Enable it when on by default.
                coerce_columns=None,
            )
        except Exception:
            # This will also log the exception inside composite normalizer's normalize
            return True

        return norm_meta.WhichOneof("input_type") == "msg_pack_frame"

    @staticmethod
    def get_arctic_style_type_info_for_norm(desc):
        # Arctic used to have a type field in get_info which had info about the underlying store being used.
        # This is to provide some sort of compatibility to that notation
        # eg: https://github.com/manahl/arctic/blob/master/arctic/store/_pandas_ndarray_store.py#L177
        input_type = desc.normalization.WhichOneof("input_type")
        if input_type == "df":
            return "pandasdf"
        elif input_type == "series":
            return "pandasseries"
        elif input_type == "np":
            return "ndarray"
        elif input_type == "msg_pack_frame":
            return "pickled"
        elif input_type == "ts":
            return "normalized_timeseries"
        else:
            return "missing_type_info"

    @staticmethod
    def is_pickled_descriptor(desc):
        return desc.normalization.WhichOneof("input_type") == "msg_pack_frame"

    def is_symbol_pickled(self, symbol: str, as_of: Optional[VersionQueryInput] = None, **kwargs) -> bool:
        """
        Query if the symbol as of the specified revision is pickled.

        Parameters
        ----------
        symbol : `str`
            symbol name
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        `bool`
            True if the symbol is pickled, False otherwise.
        """
        version_query = self._get_version_query(as_of, **kwargs)
        read_options = self._get_read_options(**kwargs)
        dit = self.version_store.read_descriptor(symbol, version_query, read_options)
        return self.is_pickled_descriptor(dit.timeseries_descriptor)

    def _get_time_range_from_ts(self, desc, min_ts, max_ts):
        if min_ts == None or max_ts == None:
            return datetime64("nat"), datetime64("nat")
        input_type = desc.normalization.WhichOneof("input_type")
        tz = None
        if input_type == "df":
            index_metadata = desc.normalization.df.common
            tz = get_timezone_from_metadata(index_metadata)
        if tz:
            # If tz is provided, it is stored in UTC - hence needs to be localized to UTC before
            # converting to the given tz
            return (
                _from_tz_timestamp(min_ts, "UTC").astimezone(pytz.timezone(tz)),
                _from_tz_timestamp(max_ts, "UTC").astimezone(pytz.timezone(tz)),
            )
        else:
            return _from_tz_timestamp(min_ts, None), _from_tz_timestamp(max_ts, None)

    def get_timerange_for_symbol(
        self, symbol: str, version: Optional[VersionQueryInput] = None, **kwargs
    ) -> Tuple[datetime, datetime]:
        """
        Query the earliest and latest timestamp in the index of the specified revision of the symbol.

        Parameters
        ----------
        symbol : `str`
            symbol name
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        `Tuple[datetime, datetime]`
            The earliest and latest timestamps in the index.
        """
        given_version = max([v["version"] for v in self.list_versions(symbol)]) if version is None else version
        version_query = self._get_version_query(given_version)
        read_options = self._get_read_options(**kwargs)

        i = self.version_store.read_index(symbol, version_query)
        frame_data = ReadResult(*i).frame_data
        index_data = FrameData.from_cpp(frame_data).data
        if len(index_data[0]) == 0:
            return datetime64("nat"), datetime64("nat")

        start_indices, end_indices = index_data[0], index_data[1]
        min_ts, max_ts = min(start_indices), max(end_indices)
        # to get timezone info
        dit = self.version_store.read_descriptor(symbol, version_query, read_options)
        return self._get_time_range_from_ts(dit.timeseries_descriptor, min_ts, max_ts)

    def name(self):
        return self._lib_cfg.lib_desc.name

    def get_num_rows(self, symbol: str, as_of: Optional[VersionQueryInput] = None, **kwargs) -> int:
        """
        Query the number of rows in the specified revision of the symbol.

        Parameters
        ----------
        symbol : `str`
            symbol name
        as_of : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        `int`
            The number of rows in the specified revision of the symbol.
        """
        read_options = self._get_read_options(**kwargs)
        version_query = self._get_version_query(as_of)
        dit = self.version_store.read_descriptor(symbol, version_query, read_options)
        return dit.timeseries_descriptor.total_rows

    def lib_cfg(self):
        return self._lib_cfg

    def open_mode(self):
        return self._open_mode

    def _process_info(self, symbol: str, dit, as_of: Optional[VersionQueryInput] = None) -> Dict[str, Any]:
        timeseries_descriptor = dit.timeseries_descriptor
        columns = [f.name for f in timeseries_descriptor.stream_descriptor.fields]
        dtypes = [f.type_desc for f in timeseries_descriptor.stream_descriptor.fields]
        index = []
        index_dtype = []
        input_type = timeseries_descriptor.normalization.WhichOneof("input_type")
        index_type = "NA"
        if input_type == "df":
            index_type = timeseries_descriptor.normalization.df.common.WhichOneof("index_type")
            if index_type == "index":
                index_metadata = timeseries_descriptor.normalization.df.common.index
            else:
                index_metadata = timeseries_descriptor.normalization.df.common.multi_index

            if index_type == "multi_index" or (index_type == "index" and index_metadata.is_not_range_index):
                index_name_from_store = columns.pop(0)
                has_fake_name = (
                    0 in index_metadata.fake_field_pos if index_type == "multi_index" else index_metadata.fake_name
                )
                if has_fake_name:
                    index = [None]
                elif index_name_from_store is not None:
                    index = [index_name_from_store]
                elif index_metadata.name:
                    index = [index_metadata.name]
                else:
                    index = [None]
                index_dtype = [dtypes.pop(0)]
            else:
                # RangeIndex
                index = [index_metadata.name if index_metadata.name else None]
                dtype_desc = TypeDescriptor()
                dtype_desc.value_type = TypeDescriptor.ValueType.INT
                index_dtype = [dtype_desc]

            if index_type == "multi_index":
                for i in range(1, index_metadata.field_count + 1):
                    index_name = columns.pop(0)
                    if i in index_metadata.fake_field_pos:
                        index_name = None
                    else:
                        index_name = index_name[_IDX_PREFIX_LEN:]
                    index.append(index_name)
                    index_dtype.append(dtypes.pop(0))
            if timeseries_descriptor.normalization.df.has_synthetic_columns:
                columns = pd.RangeIndex(0, len(columns))

        date_range = self._get_time_range_from_ts(timeseries_descriptor, dit.start_index, dit.end_index)
        last_update = datetime64(dit.creation_ts, "ns")
        return {
            "col_names": {"columns": columns, "index": index, "index_dtype": index_dtype},
            "dtype": dtypes,
            "rows": timeseries_descriptor.total_rows,
            "last_update": last_update,
            "input_type": input_type,
            "index_type": index_type,
            "normalization_metadata": timeseries_descriptor.normalization,
            "type": self.get_arctic_style_type_info_for_norm(timeseries_descriptor),
            "date_range": date_range,
            "sorted": SortedValue.Name(timeseries_descriptor.stream_descriptor.sorted),
        }

    def get_info(self, symbol: str, version: Optional[VersionQueryInput] = None) -> Dict[str, Any]:
        """
        Returns descriptive data for `symbol`.

        Parameters
        ----------
        symbol : `str`
            symbol name
        version : `Optional[VersionQueryInput]`, default=None
            See documentation of `read` method for more details.

        Returns
        -------
        `Dict[str, Any]`
            Dictionary containing the following fields:

            - col_names, `Dict`
            - dtype, `List`
            - rows, `int`
            - last_update, `datetime`
            - input_type, `str`
            - index_type, `index_type`
            - normalization_metadata,
            - type, `str`
            - date_range, `tuple`
        """
        version_query = self._get_version_query(version)
        read_options = _PythonVersionStoreReadOptions()
        dit = self.version_store.read_descriptor(symbol, version_query, read_options)
        return self._process_info(symbol, dit, version)

    def batch_get_info(
        self, symbols: List[str], as_ofs: Optional[List[VersionQueryInput]] = None
    ) -> List[Dict[str, Any]]:
        """
        Returns descriptive data for a list of `symbols`.

        Parameters
        ----------
        symbols : `str`
            symbols: `List[str]`
                List of symbols to read the descriptor info for.
            as_ofs: `Optional[List[VersionQueryInput]]`, default=None
                List of version queries. See documentation of `read` method for more details.
                i-th entry corresponds to i-th element of `symbols`.

        Returns
        -------
        `List[Dict[str, Any]]`
            List of Dictionaries containing the following fields:

            - col_names, `Dict`
            - dtype, `List`
            - rows, `int`
            - last_update, `datetime`
            - input_type, `str`
            - index_type, `index_type`
            - normalization_metadata,
            - type, `str`
            - date_range, `tuple`
        """
        throw_on_error = True
        return self._batch_read_descriptor(symbols, as_ofs, throw_on_error)

    def _batch_read_descriptor(self, symbols, as_ofs, throw_on_error):
        as_ofs_lists = []
        if as_ofs == None:
            as_ofs_lists = [None] * len(symbols)
        else:
            as_ofs_lists = as_ofs

        version_queries = []
        for as_of in as_ofs_lists:
            version_queries.append(self._get_version_query(as_of))

        read_options = _PythonVersionStoreReadOptions()
        read_options.set_batch_throw_on_error(throw_on_error)
        descriptions_or_errors = self.version_store.batch_read_descriptor(symbols, version_queries, read_options)
        args_list = list(zip(descriptions_or_errors, symbols, version_queries, as_ofs_lists))
        description_results = []
        for dit, symbol, version_query, as_of in args_list:
            if isinstance(dit, DataError):
                description_results.append(dit)
            else:
                description_results.append(self._process_info(symbol, dit, as_of))
        return description_results

    def write_metadata(
        self, symbol: str, metadata: Any, prune_previous_version: Optional[bool] = None
    ) -> VersionedItem:
        """
        Write 'metadata' under the specified 'symbol' name to this library.
        The data will remain unchanged. A new version will be created.
        If the symbol is missing, it causes a write with empty data (pickled, can't append) and the supplied metadata.
        Returns a VersionedItem object with only a metadata element.
        This operation is fast as there are zero data segment read/write operations.

        Parameters
        ----------
        symbol : `str`
            symbol name
        metadata : `Any`
            metadata to persist along with the symbol data
        prune_previous_version : `Optional[bool]`, default=None
            Removes previous (non-snapshotted) versions from the database.

        Returns
        -------
        `VersionedItem`
            VersionedItem containing the metadata of the written symbol's version in the store.
            The data attribute will not be populated.
        """
        self.check_symbol_validity(symbol)

        proto_cfg = self._lib_cfg.lib_desc.version.write_options

        prune_previous_version = self.resolve_defaults(
            "prune_previous_version", proto_cfg, global_default=False, existing_value=prune_previous_version
        )
        udm = normalize_metadata(metadata) if metadata is not None else None
        v = self.version_store.write_metadata(symbol, udm, prune_previous_version)
        return self._convert_thin_cxx_item_to_python(v)

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
        return self.version_store.is_symbol_fragmented(symbol, segment_size)

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
            It is for achieving smallest no. of segment after compaction. Please refer to below example for further explantion.

        Returns
        -------
        VersionedItem
            Structure containing metadata and version number of the defragmented symbol in the store.

        Raises
        ------
        1002 ErrorCategory.INTERNAL:E_ASSERTION_FAILURE
            If `is_symbol_fragmented` returns false.
        2001 ErrorCategory.NORMALIZATION:E_UNIMPLEMENTED_INPUT_TYPE
            If library option - "bucketize_dynamic" is ON.

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

        if self._lib_cfg.lib_desc.version.write_options.bucketize_dynamic:
            raise ArcticDbNotYetImplemented(f"Support for library with 'bucketize_dynamic' ON is not implemented yet")

        result = self.version_store.defragment_symbol_data(symbol, segment_size)
        return VersionedItem(
            symbol=result.symbol,
            library=self._library.library_path,
            version=result.version,
            metadata=None,
            data=None,
            host=self.env,
        )

    def library(self):
        return self._library

    def library_tool(self) -> LibraryTool:
        return LibraryTool(self.library())
