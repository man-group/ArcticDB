import logging as _logging
import os as _os

# Since aws-sdk-cpp >= 1.11.486, it has turned on checksum integrity check `x-amz-checksum-mode: enabled`
# This feature is not supported in many s3 implementation and causes error
# Update environment variable before import arcticdb_ext to disable checksum validation
if _os.getenv("AWS_RESPONSE_CHECKSUM_VALIDATION") == "when_supported" or _os.getenv("AWS_REQUEST_CHECKSUM_CALCULATION") == "when_supported":
    _logging.getLogger(__name__).warning(
        "Checksum validation has been specfically enabled by user, which the endpoint may not support and causes error."
    )
else:
    _os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"
    _os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"

import arcticdb_ext as _ext
import sys as _sys

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.version_store._store import VersionedItem
import arcticdb.version_store.library as library
from arcticdb.tools import set_config_from_env_vars
from arcticdb_ext.version_store import DataError, VersionRequestType
from arcticdb_ext.exceptions import ErrorCode, ErrorCategory
from arcticdb.version_store.library import (
    WritePayload,
    ReadInfoRequest,
    ReadRequest,
    col,
    LazyDataFrame,
    LazyDataFrameCollection,
    StagedDataFinalizeMethod,
    WriteMetadataPayload
)
from arcticdb.version_store.admin_tools import KeyType, Size

set_config_from_env_vars(_os.environ)

__version__ = "dev"
