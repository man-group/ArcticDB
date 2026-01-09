"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
from .environment_setup import (
    TestLibraryManager,
    LibraryPopulationPolicy,
    LibraryType,
    Storage,
    populate_library,
)
from benchmarks.common import AsvBase


class AWSVersionSymbols(AsvBase):

    rounds = 1
    number = 3  # invoke X times the test runs between each setup-teardown
    repeat = 1  # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="LIST_VERSIONS")
    library_type = LibraryType.PERSISTENT

    # NOTE: If you plan to make changes to parameters, consider that a library with previous definition
    #       may already exist. This means that symbols there will be having having different number
    #       of rows than what you defined in the test. To resolve this problem check with documentation:
    #           https://github.com/man-group/ArcticDB/wiki/ASV-Benchmarks:-Real-storage-tests
    params = [25, 50]
    param_names = ["num_syms"]

    number_columns = 2
    number_rows = 2

    mean_number_versions_per_symbol = 5

    def get_library_manager(self) -> TestLibraryManager:
        return AWSVersionSymbols.library_manager

    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(None)  # Tone down creation of structure
        # parameters will be set on demand during iterations
        lpp.use_auto_increment_index()
        lpp.generate_versions(
            versions_max=int(1.5 * AWSVersionSymbols.mean_number_versions_per_symbol),
            mean=AWSVersionSymbols.mean_number_versions_per_symbol,
        )
        lpp.generate_metadata().generate_snapshots()
        return lpp

    def setup_cache(self):
        num_rows = AWSListSymbols.number_rows
        manager = self.get_library_manager()
        policy = self.get_population_policy()
        last_snapshot_names_dict = {}
        for number_symbols in AWSVersionSymbols.params:
            start = time.time()
            policy.set_parameters([num_rows] * number_symbols, AWSVersionSymbols.number_columns)
            if not manager.has_library(AWSListSymbols.library_type, number_symbols):
                populate_library(manager, policy, AWSVersionSymbols.library_type, number_symbols)
                self.get_logger().info(f"Generated {number_symbols} with {num_rows} each for {time.time()- start}")
            else:
                self.get_logger().info(f"Library already exists, population skipped")
            # Getting one snapshot - the last
            lib = self.get_library_manager().get_library(AWSVersionSymbols.library_type, number_symbols)
            snapshot_name = lib.list_snapshots(load_metadata=False)[-1]
            last_snapshot_names_dict[number_symbols] = snapshot_name
        manager.log_info()  # Always log the ArcticURIs
        return last_snapshot_names_dict

    def setup(self, last_snapshot_names_dict, num_syms):
        self.population_policy = self.get_population_policy()
        self.lib = self.get_library_manager().get_library(AWSVersionSymbols.library_type, num_syms)
        self.test_counter = 1
        expected_num_versions = AWSVersionSymbols.mean_number_versions_per_symbol * num_syms
        self.get_logger().info(f"Library {str(self.lib)}")
        symbols_list = self.lib.list_symbols()
        assert num_syms == len(symbols_list), f"The library contains expected number of symbols {symbols_list}"
        mes = f"There are sufficient versions (at least {expected_num_versions - 1}, num symbols {len(symbols_list)})"
        assert (expected_num_versions - 1) >= len(symbols_list), mes
        assert last_snapshot_names_dict[num_syms] is not None

    def time_list_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots()

    def time_list_snapshots_without_metadata(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots(load_metadata=False)

    def peakmem_list_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots()

    def peakmem_list_snapshots_without_metadata(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots(load_metadata=False)
