"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from logging import Logger
import pandas as pd

from arcticdb.util.environment_setup import (
    TestLibraryManager,
    LibraryPopulationPolicy,
    LibraryType,
    Storage,
)
from arcticdb.util.logger import get_logger as _get_logger
from benchmarks.common import AsvBase


class AWSNestedDictRead(AsvBase):

    rounds = 1
    number = 3
    repeat = 1
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    param_names = ["num_dict_entries"]
    params = [1000]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="NESTED_DICT_READ")

    def get_logger(self) -> Logger:
        return _get_logger(self)

    def get_library_manager(self) -> TestLibraryManager:
        return AWSNestedDictRead.library_manager

    def get_population_policy(self) -> LibraryPopulationPolicy:
        return LibraryPopulationPolicy(self.get_logger())

    def setup_cache(self):
        manager = self.get_library_manager()
        lib = manager.get_library(LibraryType.PERSISTENT)
        
        for num_entries in self.params:
            symbol_name = self.get_symbol_name(num_entries)
            if symbol_name not in lib.list_symbols():
                data = {
                    f"{i}": {
                        "a": pd.DataFrame({"a": [1, 2, 3]}),
                        "b": pd.DataFrame({"b": [1, 2, 3, 4, 5, 6]})
                    } for i in range(num_entries)
                }
                lib._nvs.write(symbol_name, data, recursive_normalizers=True)
        
        manager.log_info()

    def get_symbol_name(self, num_dict_entries):
        return f"nested_dict_{num_dict_entries}"

    def setup(self, num_dict_entries):
        self.read_lib = self.get_library_manager().get_library(LibraryType.PERSISTENT)
        self.symbol = self.get_symbol_name(num_dict_entries)

    def teardown(self, num_dict_entries):
        pass

    def time_read_nested_dict(self, num_dict_entries):
        self.read_lib.read(self.symbol)

    def peakmem_read_nested_dict(self, num_dict_entries):
        self.read_lib.read(self.symbol)
