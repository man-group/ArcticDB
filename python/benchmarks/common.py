"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
import numpy as np


def generate_pseudo_random_dataframe(n, freq="s", end_timestamp="1/1/2023"):
    """
    Generates a Data Frame with 2 columns (timestamp and value) and N rows
    - timestamp contains timestamps with a given frequency that end at end_timestamp
    - value contains random floats that sum up to approximately N, for easier testing/verifying
    """
    # Generate random values such that their sum is equal to N
    values = np.random.uniform(0, 2, size=n)
    # Generate timestamps
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq=freq)
    # Create dataframe
    df = pd.DataFrame({"value": values})
    df.index = timestamps
    return df


def generate_random_floats_dataframe(num_rows, num_cols):
    """
    Generates a dataframe of random 64 bit floats with num_rows rows and num_cols columns.
    Columns are named "col_n" for n in range(num_cols).
    Row range indexed.
    """
    columns = [f"col_{n}" for n in range(num_cols)]
    rng = np.random.default_rng()
    data = rng.random((num_rows, num_cols), dtype=np.float64)
    return pd.DataFrame(data, columns=columns)


def generate_benchmark_df(n, freq="min", end_timestamp="1/1/2023"):
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq=freq)
    k = n // 10
    # Based on https://github.com/duckdblabs/db-benchmark/blob/master/_data/groupby-datagen.R#L19
    dt = pd.DataFrame()
    dt["id1"] = np.random.choice([f"id{str(i).zfill(3)}" for i in range(1, k + 1)], n)
    dt["id2"] = np.random.choice([f"id{str(i).zfill(3)}" for i in range(1, k + 1)], n)
    dt["id3"] = np.random.choice([f"id{str(i).zfill(10)}" for i in range(1, n // k + 1)], n)
    dt["id4"] = np.random.choice(range(1, k + 1), n)
    dt["id5"] = np.random.choice(range(1, k + 1), n)
    dt["id6"] = np.random.choice(range(1, n // k + 1), n)
    dt["v1"] = np.random.choice(range(1, 6), n)
    dt["v2"] = np.random.choice(range(1, 16), n)
    dt["v3"] = np.round(np.random.uniform(0, 100, n), 6)

    assert len(timestamps) == len(dt)

    dt.index = timestamps

    return dt


def get_prewritten_lib_name(rows):
    return f"prewritten_{rows}"
