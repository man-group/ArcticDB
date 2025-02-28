import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np

from arcticdb import Arctic
import arcticdb


ver = arcticdb.__version__

ac = Arctic(f"lmdb:///tmp/{ver}")
assert ac.list_libraries() == []

df = pd.DataFrame(np.random.randint(0,100,size=(1000000, 4)), columns=list('ABCD'))
lib = ac.create_library("test")
assert ac.list_libraries() == ["test"]

lib.write("s", df)
assert_frame_equal(lib.read("s").data, df)

ac.delete_library("test")
assert ac.list_libraries() == []

