import pytest
from arcticdb import Arctic
import pandas as pd

from arcticdb.util.test import assert_frame_equal


def test_in_memory():
    ac = Arctic("mem://")
    ac.create_library("test_lib")
    lib = ac["test_lib"]
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    lib.write("symbol", df)
    assert_frame_equal(lib.read("symbol").data, df)
