from arcticdb import Arctic
import sys

from storage_common import *

# TODO: Add support for other storages
uri = get_real_s3_uri()

ac = Arctic(uri)
lib_name = sys.argv[1]

ac = Arctic(uri)
branch_name = sys.argv[1]
for lib in LIBRARIES:
    lib_name = f"{branch_name}_{lib}"
    lib = ac[lib_name]
    print(lib_name)

    symbols = lib.list_symbols()
    print(symbols)
    assert len(symbols) == 3
    for sym in ["one", "two", "three"]:
        assert sym in symbols
    for sym in symbols:
        df = lib.read(sym).data
        column_names = df.columns.values.tolist()
        assert column_names == ["x", "y", "z"]
