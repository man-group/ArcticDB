from arcticdb import Arctic
import sys

from storage_common import *


def seed_library(lib_name):
    # Each branch should create its own seed and it should be fresh on each run
    # so delete the library, if it exists / wasn't cleaned on a previous run
    ac.delete_library(lib_name)
    ac.create_library(lib_name)

    library = ac[lib_name]

    one_df = test_df_3_cols()
    library.write("one", one_df)

    two_df = test_df_3_cols(1)
    library.write("two", two_df)

    two_df = test_df_3_cols(2)
    library.append("two", two_df)

    three_df = test_df_3_cols(3)
    library.append("three", three_df)


if __name__ == "__main__":
    # TODO: Add support for other storages
    uri = get_real_s3_uri()

    ac = Arctic(uri)
    lib_name = sys.argv[1]
    seed_library(lib_name)
