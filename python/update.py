import arcticdb
import numpy as np
import pandas as pd

ac = arcticdb.Arctic("lmdb://test")
lib = ac.get_library("test")

df = pd.DataFrame({"a": [1, 2, 3, 4]}, index=pd.date_range("2024-01-01", "2024-01-04"))
print(df)
lib.write("sym", df)

update = pd.DataFrame({"a": [20, 20]}, index=pd.date_range("2024-01-02", "2024-01-03"))
print(update)
lib.update("sym", update)
