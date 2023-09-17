# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
from arcticdb import Arctic
import pandas as pd

class TimeSuite:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    def setup(self):
        self.ac = Arctic("lmdb://test")
        self.ac.create_library("test")

    def time_write(self):
        self.ac['test'].write('test', pd.DataFrame())
