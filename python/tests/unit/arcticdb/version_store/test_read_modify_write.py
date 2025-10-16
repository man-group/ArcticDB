import pandas as pd
from arcticdb.version_store.processing import QueryBuilder


def test_read_modify_write(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    q = QueryBuilder()
    q = q[q["col"] < 5]
    lib.write("sym", pd.DataFrame({"col": [1, 10, 3]}))
    lib._read_modify_write("sym", q)
    lib.read("sym")
