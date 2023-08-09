from arcticdb import Arctic
import sys

from storage_common import *


def verify_library(lib_name):
    lib = ac[lib_name]

    symbols = lib.list_symbols()
    assert len(symbols) == 3
    for sym in ["one", "two", "three"]:
        assert sym in symbols
    for sym in symbols:
        df = lib.read(sym).data
        column_names = df.columns.values.tolist()
        assert column_names == ["x", "y", "z"]


if __name__ == "__main__":
    # TODO: Add support for other storages
    uri = get_real_s3_uri()

    ac = Arctic(uri)
    branch_name = os.getenv("ARCTICDB_REAL_STORAGE_BRANCH_NAME")
    for lib in LIBRARIES:
        lib_name = f"test_{branch_name}_{lib}"
        lib_name = normalize_lib_name(lib_name)
        verify_library(lib_name)
