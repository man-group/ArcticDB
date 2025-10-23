import pandas as pd
import pytest
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal


def test_read_modify_write_filter(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "sym"
    df1 = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
    lib.append(sym, df1)

    df2 = pd.DataFrame({"b": [4.0, 5.0, 6.0]})
    lib.append(sym, df2)

    df3 = pd.DataFrame({"a": [4.0, 5.0, 6.0]})
    lib.append(sym, df3)

    q = QueryBuilder()
    q = q[q["a"] > 2.0]
    lib._read_modify_write(sym, q, "sym2")
