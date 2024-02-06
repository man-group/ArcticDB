import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

from arcticdb.version_store._store import write_file, read_file
import time

def test_simple_file(tmpdir):
    path = "{}/{}".format(tmpdir, "file.arc")

    df = pd.DataFrame({f'col_{i}': np.arange(1000000) for i in range(500)})
    start = time.time()
    write_file(path, df)
    elapsed = time.time() - start
    print("arctic write time: " + str(elapsed))

    start = time.time()
    vit = read_file(path)
    elapsed = time.time() - start
    print("arctic read time: " + str(elapsed))
    assert_frame_equal(df, vit.data)

    # Write DataFrame to Parquet
    start_write = time.time()
    df.to_parquet('dataframe.parquet', engine='pyarrow')
    elapsed = time.time() - start_write
    print("parquet write time: " + str(elapsed))

    # Read DataFrame from Parquet
    start_read = time.time()
    df_read = pd.read_parquet('dataframe.parquet', engine='pyarrow')
    elapsed = time.time() - start_read
    print("parquet read time: " + str(elapsed))