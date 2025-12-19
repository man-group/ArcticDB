"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from logging import Logger
import pandas as pd
from functools import lru_cache

from arcticdb.util.environment_setup import (
    TestLibraryManager,
    LibraryPopulationPolicy,
    LibraryType,
    Storage,
)
from arcticdb.util.logger import get_logger as _get_logger
from benchmarks.common import AsvBase


class LMDBRecursiveNormalizer(AsvBase):

    rounds = 4

    param_names = ["num_dict_entries", "num_symbols"]
    params = [[1000], [5]]

    library_manager = TestLibraryManager(storage=Storage.LMDB, name_benchmark="NESTED_DICT_READ")

    def get_logger(self) -> Logger:
        return _get_logger(self)

    def get_library_manager(self) -> TestLibraryManager:
        return LMDBRecursiveNormalizer.library_manager

    def get_population_policy(self) -> LibraryPopulationPolicy:
        return LibraryPopulationPolicy(self.get_logger())

    def setup_cache(self):
        manager = self.get_library_manager()
        lib = manager.get_library(LibraryType.PERSISTENT)

        max_num_symbols = max(self.params[1])
        for num_dict_entry in self.params[0]:
            for symbol_idx in range(max_num_symbols):
                symbol_name = self.get_symbol_name(num_dict_entry, symbol_idx)
                lib._nvs.write(symbol_name, self.get_data(num_dict_entry), recursive_normalizers=True)

        manager.log_info()

    @lru_cache(maxsize=None)
    def get_data(self, num_dict_entry):
        return {
            f"{i}": {"a": pd.DataFrame({"a": [1, 2, 3]}), "b": pd.DataFrame({"b": [1, 2, 3, 4, 5, 6]})}
            for i in range(num_dict_entry)
        }

    def get_symbol_name(self, dict_entry, symbol_idx):
        return f"nested_dict_sym_{dict_entry}_{symbol_idx}"

    def setup(self, num_dict_entry, num_symbols):
        self.read_lib = self.get_library_manager().get_library(LibraryType.PERSISTENT)

    def teardown(self, num_dict_entry, num_symbols):
        pass

    def time_read_nested_dict(self, num_dict_entry, num_symbols):
        self.read_lib.read(self.get_symbol_name(num_dict_entry, 0))

    def peakmem_read_nested_dict(self, num_dict_entry, num_symbols):
        self.read_lib.read(self.get_symbol_name(num_dict_entry, 0))

    def time_read_batch_nested_dict(self, num_dict_entry, num_symbols):
        self.read_lib.read_batch([self.get_symbol_name(num_dict_entry, i) for i in range(num_symbols)])

    def peakmem_read_batch_nested_dict(self, num_dict_entry, num_symbols):
        self.read_lib.read_batch([self.get_symbol_name(num_dict_entry, i) for i in range(num_symbols)])

    def time_write_nested_dict(self, num_dict_entry, num_symbols):
        self.read_lib._nvs.write(
            f"nested_dict_time_write_nested_dict_{num_dict_entry}",
            self.get_data(num_dict_entry),
            recursive_normalizers=True,
        )

    def peakmem_write_nested_dict(self, num_dict_entry, num_symbols):
        self.read_lib._nvs.write(
            f"nested_dict_peakmem_write_nested_dict_{num_dict_entry}",
            self.get_data(num_dict_entry),
            recursive_normalizers=True,
        )
