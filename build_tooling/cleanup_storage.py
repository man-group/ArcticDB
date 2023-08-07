from arcticdb import Arctic
import sys

from storage_common import *

# TODO: Add support for other storages
uri = get_real_s3_uri()

ac = Arctic(uri)
branch_name = sys.argv[1]
for lib in LIBRARIES:
    lib_name = f"{branch_name}_{lib}"

    ac.delete_library(lib_name)
