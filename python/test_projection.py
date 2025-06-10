import arcticdb as adb
import numpy as np
import pandas as pd

ac = adb.Arctic("lmdb://test")
lib = ac.get_library("test", create_if_missing=True)
lib.write("test", pd.DataFrame({"g": [0], "ag": [0]}))

q = adb.QueryBuilder()
q = q.apply("new", q["g"] + 5)
print(lib.read("test", query_builder=q).data)
print("\n====================\n")
print(lib.read("test", query_builder=q, columns=["g", "ag", "new"]).data)
