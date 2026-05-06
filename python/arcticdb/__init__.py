import logging as _logging
import os as _os

import arcticdb_ext as _ext
import sys as _sys

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions, OutputFormat, RuntimeOptions, ArrowOutputStringFormat
from arcticdb.version_store.processing import QueryBuilder, where
from arcticdb.version_store._store import VersionedItem
import arcticdb.version_store.library as library
from arcticdb.tools import set_config_from_env_vars
from arcticdb_ext.version_store import DataError, VersionRequestType
from arcticdb_ext.exceptions import ErrorCode, ErrorCategory
from arcticdb.version_store.library import (
    WritePayload,
    UpdatePayload,
    ReadInfoRequest,
    ReadRequest,
    DeleteRequest,
    col,
    LazyDataFrame,
    LazyDataFrameCollection,
    LazyDataFrameAfterJoin,
    concat,
    StagedDataFinalizeMethod,
    WriteMetadataPayload,
)
from arcticdb.version_store.admin_tools import KeyType, Size

set_config_from_env_vars(_os.environ)

if _sys.platform == "win32":
    # On Windows, os._exit → ExitProcess can deadlock when thread pool threads
    # hold CRT locks during DLL_PROCESS_DETACH (the "loader lock" deadlock).
    # Wrapping os._exit to stop thread pools first prevents the deadlock.
    _original_exit = _os._exit

    def _safe_exit(code):
        _ext.shutdown()
        _original_exit(code)

    _os._exit = _safe_exit

__version__ = "dev"
