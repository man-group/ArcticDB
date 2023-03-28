import arcticdb_ext as _ext
import os
import sys as _sys
import google.protobuf as _protobuf

_proto_ver = _protobuf.__version__.split(".")[0]
if _proto_ver in "34":
    # Use the namespace package (https://peps.python.org/pep-0420) feature to select the pb2 files matching the protobuf
    _sys.path.append(os.path.join(os.path.dirname(__file__), "proto", _proto_ver))
else:
    raise NotImplementedError(f"We do not support protobuf {_protobuf.__version__}")

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.version_store.processing import QueryBuilder
import arcticdb.version_store.library as library
from arcticdb.options import LibraryOptions
from arcticdb.tools import set_config_from_env_vars

set_config_from_env_vars(os.environ)

del os
