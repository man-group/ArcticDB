import arcticdb
import pandas as pd
import numpy as np

ac = arcticdb.Arctic("lmdb://test")
opts = arcticdb.LibraryOptions(columns_per_segment=2, rows_per_segment=2)
if ac.has_library("test"):
    ac.delete_library("test")
lib = ac.get_library("test", library_options=opts, create_if_missing=True)

arrays = [
    [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(2), pd.Timestamp(2), pd.Timestamp(3)],
    [10, 20, 10, 20, 10],
]  # First index: integers  # Second index: integers
index = pd.MultiIndex.from_arrays(arrays, names=("index1", "index2"))

data = np.random.randn(5, 5)
columns = ["col1", "col2", "col3", "col4", "col5"]

df = pd.DataFrame(data, index=index, columns=columns)

ts_df = pd.DataFrame(data, columns=columns, index=pd.date_range("2023-01-01", periods=5, freq="h"))

qb = arcticdb.QueryBuilder()
qb = qb[qb["col1"] > 0]

lib.write("test", ts_df)
lib.read("test", query_builder=qb)
