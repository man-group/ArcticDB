import tempfile

import pandas as pd
import numpy as np

from arcticdb import Arctic


def test_write_pyarrow_backed_dataframes():
    # ArcticDB must support writing pyarrow-backed pandas.DataFrames.

    NUM_COLUMNS=10
    NUM_ROWS=1

    with tempfile.TemporaryDirectory() as tmpdir:

        ac = Arctic(f"lmdb://{tmpdir}/my.db")

        ac.create_library('travel_data')
        ac.list_libraries()

        df = pd.DataFrame(
            np.random.randint(0, 100, size=(NUM_ROWS, NUM_COLUMNS)),
            columns=[f"COL_{i}" for i in range(NUM_COLUMNS)],
            index=pd.date_range('2000', periods=NUM_ROWS, freq='h'),
            dtype="float32[pyarrow]",
        )

        lib = ac['travel_data']
        lib.write("my_data", df)
