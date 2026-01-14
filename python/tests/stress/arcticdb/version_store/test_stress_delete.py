"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from datetime import datetime
from pandas.testing import assert_frame_equal

from arcticdb.util.test import sample_dataframe
from arcticdb_ext.storage import KeyType

from arcticdb.exceptions import NoDataFoundException
import pytest


def py_enum_to_dict(enum):
    return list(enum.__members__.values())


def check_no_keys(library):
    lib_tool = library.library_tool()

    all_keys_types = py_enum_to_dict(KeyType)
    for key_type in all_keys_types:
        assert len(lib_tool.find_keys(key_type)) == 0


@pytest.mark.storage
def test_stress_delete(object_store_factory):
    store_factory = object_store_factory
    lib1 = store_factory(name=f"delete_me_{datetime.utcnow().isoformat()}")
    lib2 = store_factory(name=f"leave_me_{datetime.utcnow().isoformat()}")
    num_tests = 100
    dataframe_size = 1000

    syms = []
    written_dfs = []

    for x in range(num_tests):
        symbol = "symbol_{}".format(x)
        syms.append(symbol)
        df = sample_dataframe(dataframe_size, x)
        written_dfs.append(df)

    lib1.batch_write(syms, written_dfs)
    lib2.batch_write(syms, written_dfs)

    start_time = datetime.now()
    lib1.version_store.clear()
    print("Delete took {}".format(datetime.now() - start_time))

    # Make sure that the symbols are deleted
    for x in range(num_tests):
        with pytest.raises(NoDataFoundException) as e:
            lib1.read(f"symbol_{x}")

    res = lib2.batch_read(syms)
    for i, sym in enumerate(syms):
        assert_frame_equal(res[sym].data, written_dfs[i])

    check_no_keys(lib1)

    lib2.version_store.clear()
    check_no_keys(lib2)

    # Make sure that the symbols are deleted
    for x in range(num_tests):
        with pytest.raises(NoDataFoundException) as e:
            lib2.read(f"symbol_{x}")
