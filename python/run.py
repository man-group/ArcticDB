from arcticdb_ext.version_store import RefKey

from arcticdb import Arctic
from arcticdb.toolbox.library_tool import LibraryTool, KeyType
import pandas as pd
# 50 columns, 25 rows, random data, datetime indexed.
import pandas as pd
import numpy as np
from datetime import datetime
cols = ['COL_%d' % i for i in range(6)]
data = np.arange(0, 36).reshape(6, 6).T
data = np.array([["DATA No_%d" % cell for cell in row] for row in data])
df = pd.DataFrame(data, columns=cols)
# df.index = pd.date_range(datetime(2000, 1, 1, 5), periods=6, freq="h")
ac = Arctic("s3://http://10.255.255.254:9000:test-arctic-bucket2")
lib = ac.get_library("library_intro")
lib.list_symbols()