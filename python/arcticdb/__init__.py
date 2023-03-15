import traceback
import os

try:
    import arcticdb_ext
except Exception as e:
    traceback.print_exc()

from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.version_store.processing import QueryBuilder
import arcticdb.version_store.library as library
from arcticdb.options import LibraryOptions
from arcticdb.tools import set_config_from_env_vars


set_config_from_env_vars(os.environ)
