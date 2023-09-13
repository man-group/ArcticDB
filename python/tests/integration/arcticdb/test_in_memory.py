import pytest
from arcticdb import Arctic
import pandas as pd

from arcticdb.util.test import assert_frame_equal

def test_in_memory():
    ac = Arctic("mem://memory_db")
    ac.create_library("test_lib")
    lib = ac["test_lib"]
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    lib.write("symbol", df)
    assert_frame_equal(lib.read("symbol").data, df)

## TODO: Some sort of cache size test
#def test_in_memory_cache_size():
#    # Given - tiny map size
#    ac = Arctic(f"lmdb://{tmpdir}?map_size=1KB")
#
#    # When
#    with pytest.raises(InternalException) as e:
#        ac.create_library("test")
#    # Then - even library creation fails so map size having an effect
#    assert "MDB_MAP_FULL" in str(e.value)
