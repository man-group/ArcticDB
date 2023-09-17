# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
from arcticdb import Arctic
from arcticdb.version_store.library import (
    WritePayload,
    ReadRequest
)

import pandas as pd
import numpy as np

def generate_pseudo_random_dataframe(n, freq="S", end_timestamp="1/1/2023"):
    """
    Generates a Data Frame with 2 columns (timestamp and value) and N rows
    - timestamp contains timestamps with a given frequency that end at end_timestamp
    - value contains random floats that sum up to approximately N, for easier testing/verifying
    """
    # Generate random values such that their sum is equal to N
    values = np.random.dirichlet(np.ones(n), size=1) * n
    # Generate timestamps
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq="S")
    # Create dataframe
    df = pd.DataFrame({"timestamp": timestamps, "value": values[0]})
    return df

def get_prewritten_lib_name(rows):
    return f"prewritten_{rows}"

class TimeSuite:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    number = 5
    params = ([1_000, 100_000], [500, 1000])
    param_names = ['rows', 'num_symbols']

    def __init__(self):
        self.ac = Arctic("lmdb://test")

        rows, num_symbols = TimeSuite.params
        for num_row in rows:
            lib = get_prewritten_lib_name(num_row)
            self.ac.delete_library(lib)
            self.ac.create_library(lib)
            lib = self.ac[lib]
            for sym in range(num_symbols[-1]):
                lib.write(f"{sym}_sym", generate_pseudo_random_dataframe(num_row))

    def setup(self, rows, num_symbols):
        pass

    def get_fresh_lib(self):
        self.ac.delete_library("fresh_lib")
        self.ac.create_library("fresh_lib")
        return self.ac["fresh_lib"]

    def time_write(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        for sym in range(num_symbols):
            lib.write(f"{sym}_sym", generate_pseudo_random_dataframe(rows))

    def peakmem_write(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        for sym in range(num_symbols):
            lib.write(f"{sym}_sym", generate_pseudo_random_dataframe(rows))

    def time_write_batch(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        df = generate_pseudo_random_dataframe(rows)
        payloads = [WritePayload(f"{sym}_sym", df) for sym in range(num_symbols)]
        lib.write_batch(payloads)

    def peakmem_write_batch(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        df = generate_pseudo_random_dataframe(rows)
        payloads = [WritePayload(f"{sym}_sym", df) for sym in range(num_symbols)]
        lib.write_batch(payloads)

    def time_read(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        COLS = ['value']
        _ = [lib.read(f"{sym}_sym", columns=COLS).data for sym in range(num_symbols)]

    def peakmem_read(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        COLS = ['value']
        _ = [lib.read(f"{sym}_sym", columns=COLS).data for sym in range(num_symbols)]

    def time_read_batch(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        COLS = ['value']
        read_reqs = [ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)]
        _ =lib .read_batch(read_reqs)

    def peakmem_read_batch(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        COLS = ['value']
        read_reqs = [ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)]
        _ =lib .read_batch(read_reqs)
    
