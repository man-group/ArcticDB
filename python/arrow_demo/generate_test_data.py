import os
import random
import numpy as np
import pandas as pd

import arcticdb as adb

def generate_demo_df(symbol, num_rows):
    data = {}
    data["price"] = np.random.randint(size=num_rows, low=100*100, high=200*100) / 100.0
    data["size"] = np.random.randint(size=num_rows, low=10, high=100)
    data["direction"] = ["Buy" if random.randint(0, 1) else "Sell" for _ in range(num_rows)]
    data["trade_id"] = [f"trade_{symbol}_{i}" for i in range(num_rows)]
    time = pd.date_range(start=pd.Timestamp(2025, 1, 1, 9), periods=num_rows, freq="s")
    return pd.DataFrame(data, index=time)

def populate_arctic_and_parquet():
    data_path = os.getenv("DATA_PATH", "/tmp/arrow-test")
    arctic_path = os.path.join(data_path, "arctic")
    parquet_path = os.path.join(data_path, "parquet")
    if not os.path.exists(parquet_path):
        os.makedirs(parquet_path)

    ac = adb.Arctic(f"lmdb://{arctic_path}")
    lib = ac.get_library("lib", create_if_missing=True)

    symbols = [
        ("AAPL", generate_demo_df("AAPL", 5_000_000)),
        ("TSLA", generate_demo_df("TSLA", 10_000_000))
    ]

    for sym, df in symbols:
        lib.write(sym, df)
        df.to_parquet(os.path.join(parquet_path, f"{sym}.parquet"))

populate_arctic_and_parquet()
