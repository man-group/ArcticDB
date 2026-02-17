import pandas as pd
import arcticdb
from arcticdb.options import LibraryOptions

ac = arcticdb.Arctic("lmdb://test")
opts = LibraryOptions(rows_per_segment=2)
df = pd.DataFrame({"a": [0] * 4}, index=pd.DatetimeIndex([pd.Timestamp(0)] * 4))

REPEAT = 10_000

for i in range(REPEAT):
    lib = ac.get_library("test", library_options=opts, create_if_missing=True)
    lib.write("test", df)
    ac.delete_library("test")
