import arcticdb
import numpy as np
import pandas as pd
from arcticdb.options import LibraryOptions
from arcticdb.version_store.library import Library
from arcticdb_ext.storage import KeyType


def count_key_types_for_symbol(lib, symbol):
    # Get the library tool
    lib_tool = lib._nvs.library_tool()

    # Define all possible key types to check
    key_types = [
        KeyType.VERSION,
        KeyType.TABLE_DATA,
        KeyType.TABLE_INDEX,
        KeyType.SNAPSHOT_REF,
        KeyType.MULTI_KEY,
        KeyType.APPEND_DATA,
        KeyType.VERSION_REF
    ]

    # Count keys for each type for the specific symbol
    symbol_key_counts = {}

    for key_type in key_types:
        keys = list(lib_tool.find_keys_for_symbol(key_type, symbol))
        symbol_key_counts[key_type.name] = len(keys)
    return symbol_key_counts


if __name__ == "__main__":
    library_name  = "test_lib"
    ac = arcticdb.Arctic("lmdb://test")
    lib_opts = LibraryOptions(rows_per_segment=2, dedup=True)
    if ac.has_library(library_name):
        ac.delete_library(library_name)
    lib = ac.get_library(library_name, library_options=lib_opts, create_if_missing=True)
    
    df1 = pd.DataFrame({"a": [1, 2, 3, 4, 5]}, index=pd.date_range(start="2023-01-01", periods=5, freq="D"))
    lib.write("test_df", df1)
    print(count_key_types_for_symbol(lib, "test_df"))
    df2 = pd.DataFrame({"a": [3, 4, 5]}, index=pd.date_range(start="2023-01-03", periods=3, freq="D"))
    lib.write("test_df", df2)
    print(count_key_types_for_symbol(lib, "test_df"))

    
    