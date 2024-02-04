import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

from arcticdb.version_store._store import write_file, read_file
from arcticdb.config import set_log_level

set_log_level("DEBUG")
def test_simple_file(tmpdir):
    path = "{}/{}".format(tmpdir, "file.arc")
    df = pd.DataFrame({"x": np.arange(1000000)})
    write_file(path, df)
    vit = read_file(path)
    assert_frame_equal(df, vit.data)
