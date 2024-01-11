from arcticdb import Arctic
import pandas as pd
import numpy as np
ac = Arctic("lmdb://test")
l = ac.get_library("test", create_if_missing=True)
df = pd.DataFrame({"col": [1,2,3]})
l.write("test", df)