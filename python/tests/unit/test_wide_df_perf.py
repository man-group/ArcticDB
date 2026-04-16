from arcticdb import Arctic, LibraryOptions
import numpy as np
import pandas as pd
import time

rng = np.random.default_rng()
URI = "lmdb:///tmp/script-test"
NUM_SYMBOLS = 20
NUM_ROWS = 320
NUM_COLUMNS = 35_000
ITERATIONS = 1


def generate_sample_data():
    return pd.DataFrame({f"col_{idx}": rng.random(NUM_ROWS) for idx in range(NUM_COLUMNS)})


def test_write_data():
    ac = Arctic(URI)
    # To make sure results are clean
    ac.delete_library("test")
    lib = ac.get_library("test", create_if_missing=True, library_options=LibraryOptions(dynamic_schema=True))
    symbols = [f"sym_{idx}" for idx in range(NUM_SYMBOLS)]
    for sym in symbols:
        df = generate_sample_data()
        lib.write(sym, df)


def test_read_serial():
    ac = Arctic(URI)
    lib = ac.get_library("test")
    symbols = [f"sym_{idx}" for idx in range(NUM_SYMBOLS)]
    start_serial = time.time()
    for _ in range(ITERATIONS):
        for sym in symbols:
            lib.read(sym)
    end_serial = time.time()
    print(f"Average serial read time: {(end_serial - start_serial) / ITERATIONS:.2f}s")


def test_read_batch():
    ac = Arctic(URI)
    lib = ac.get_library("test")
    symbols = [f"sym_{idx}" for idx in range(NUM_SYMBOLS)]
    start_batch = time.time()
    for _ in range(ITERATIONS):
        lib.read_batch(symbols)
    end_batch = time.time()
    print(f"Average batch read time: {(end_batch - start_batch) / ITERATIONS:.2f}s")
