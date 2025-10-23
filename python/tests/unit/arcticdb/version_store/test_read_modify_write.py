import pandas as pd
import pytest
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal


def test_read_modify_write_filter(version_store_factory):
    lmdb_version_store = version_store_factory(col_per_group=2, row_per_segment=2)
    lib = lmdb_version_store
    q = QueryBuilder()
    q = q[q["col"] < 5]
    lib.write(
        "sym",
        pd.DataFrame(
            {
                "col": [10, 10, 10, 10, 3, 5, 5, 5, 5, 1, 2],
                "col_2": [10, 10, 10, 10, 3, 5, 5, 5, 5, 1, 2],
                "col_3": [10, 10, 10, 10, 3, 5, 5, 5, 5, 1, 2],
            }
        ),
    )
    lib._read_modify_write("sym", q, "sym1")
    assert_frame_equal(lib.read("sym1").data, lib.read("sym", query_builder=q).data)
