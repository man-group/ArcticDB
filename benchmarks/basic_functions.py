from arcticdb import Arctic
from arcticdb.version_store.library import (
    WritePayload,
    ReadRequest
)

from common import *

class BasicFunctions:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    number = 5
    timeout = 6000

    params = ([1000, 2000], [500, 1000])
    param_names = ['rows', 'num_symbols']

    def __init__(self):
        self.ac = Arctic("lmdb://basic_functions")

        rows, num_symbols = BasicFunctions.params
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

    def time_write_staged(self, rows, _):
        lib = self.get_fresh_lib()
        lib.write("staged_sym", generate_pseudo_random_dataframe(rows))

    def peakmem_write_staged(self, rows, _):
        lib = self.get_fresh_lib()
        lib.write("staged_sym", generate_pseudo_random_dataframe(rows))

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
    
