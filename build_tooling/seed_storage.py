from arcticdb import Arctic
import sys

from storage_common import *

# TODO: Add support for other storages
uri = get_real_s3_uri()

ac = Arctic(uri)
lib_name = sys.argv[1]

# TODO: Add some validation of the library, if it is there
if lib_name not in ac.list_libraries():
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
