"""
Copyright 2024 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt. As of the
Change Date specified in that file, in accordance with the Business Source License, use of this software will be
governed by the Apache License, version 2.0.
"""

import pandas as pd
import arcticdb
import pytest
from functools import reduce

@pytest.fixture(
    scope="function",
    params=(
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ),
)
def lmdb_version_store_static_and_dynamic(request):
    """
    Designed to test all combinations between schema and encoding version for LMDB
    """
    yield request.getfixturevalue(request.param)

@pytest.mark.skip(reason="Not implemented")
class TestReadSingleIndex:
    @pytest.mark.parametrize("index", [
        pd.RangeIndex(start=0, stop=10),
        pd.RangeIndex(start=0, stop=10, step=2),
        pd.date_range(start="01/01/2024",end="01/10/2024"),
        pd.MultiIndex.from_arrays(
            [pd.date_range(start="01/01/2024", end="01/10/2024"), pd.RangeIndex(start=0, stop=10)],
            names=["datetime", "level"]
        )
    ])
    def test_rowrange(self, lmdb_version_store_static_and_dynamic, index):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": range(0, len(index))}, index=index))
        result = lmdb_version_store_static_and_dynamic.read_index("sym")
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.equals(index)
        
    @pytest.mark.parametrize("indexes", [
        [
            pd.date_range(start="01/01/2024",end="01/10/2024"),
            pd.date_range(start="01/11/2024",end="01/15/2024"),
            pd.date_range(start="01/22/2024",end="01/30/2024")
        ],
        [
            pd.RangeIndex(start=0, stop=10),
            pd.RangeIndex(start=10, stop=15),
            pd.RangeIndex(start=15, stop=22)
        ],
        [
            pd.MultiIndex.from_arrays(
                [pd.date_range(start="01/01/2024", end="01/10/2024"), pd.RangeIndex(start=0, stop=10)],
                names=["datetime", "level"]
            ),
            pd.MultiIndex.from_arrays(
                [pd.date_range(start="01/11/2024", end="01/21/2024"), pd.RangeIndex(start=10, stop=21)],
                names=["datetime", "level"]
            )
        ]
    ])
    def test_as_of(self, lmdb_version_store_static_and_dynamic, indexes):
        data = [list(range(0, len(index))) for index in indexes]
        
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": data[0]}, index=indexes[0]))

        for i in range(1, len(indexes)):
            lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": data[i]}, index=indexes[i]))
        
        for i in range(0, len(indexes)):
            read_index_result = lmdb_version_store_static_and_dynamic.read_index("sym", as_of=i)
            assert isinstance(read_index_result, arcticdb.VersionedItem)
            assert read_index_result.symbol == "sym"
            assert read_index_result.version == i
            assert read_index_result.data.equals(reduce(lambda current, new: current.append(new), indexes[:i+1]))