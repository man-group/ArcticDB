"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
from arcticdb.util.environment_setup import LibraryManager, LibraryPopulationPolicy, LibraryType, Storage, populate_library_if_missing
from benchmarks.common import AsvBase


class AWSListSymbols(AsvBase):

    rounds = 1
    number = 1 # invoke X times the test runs between each setup-teardown 
    repeat = 3 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200
    
    library_manager = LibraryManager(storage=Storage.AMAZON, name_benchmark="LIST_SYMBOLS")
    library_type = LibraryType.PERSISTENT

    # NOTE: Change of parameters will trigger failure as original library must also be deleted manually.
    #       Therefore if you plan changes to those numbers make sure to delete old library manually 
    params = [500, 1000]
    param_names = ["num_syms"]

    number_columns = 2
    number_rows = 2

    def get_library_manager(self) -> LibraryManager:
        return AWSListSymbols.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(None, None) # Tone down creation of structure
        lpp.set_columns(AWSListSymbols.number_columns)
        lpp.use_auto_increment_index()
        return lpp

    def setup_cache(self):
        num_rows = AWSListSymbols.number_rows
        manager = self.get_library_manager()
        assert AWSListSymbols.number == 1, "There must be always one test between setup and tear down"
        policy = self.get_population_policy()
        for number_symbols in AWSListSymbols.params:
            start = time.time()
            policy.set_parameters(num_rows* number_symbols)
            populate_library_if_missing(manager, policy, AWSListSymbols.library_type, number_symbols)
            self.get_logger().info(f"Generated {number_symbols} with {num_rows} each for {time.time()- start}")
        manager.log_info() # Always log the ArcticURIs 
    
    def setup(self, num_syms):
        self.population_policy = self.get_population_policy()
        self.lib = self.get_library_manager().get_library(AWSListSymbols.library_type, num_syms)
        self.test_counter = 1
        assert num_syms == len(self.lib.list_symbols()), "The library contains expected number of symbols"
        self.lib._nvs.version_store._clear_symbol_list_keys() # clear cache

    def time_list_symbols(self, num_syms):
        assert self.test_counter == 1, "Test executed only once in setup-teardown cycle" 
        self.lib.list_symbols()
        self.test_counter += 1

    def time_has_symbol_nonexisting(self, num_syms):
        assert self.test_counter == 1, "Test executed only once in setup-teardown cycle" 
        self.lib.has_symbol("250_sym")        
        self.test_counter += 1

    def peakmem_list_symbols(self, num_syms):
        assert self.test_counter == 1, "Test executed only once in setup-teardown cycle" 
        self.lib.list_symbols()
        self.test_counter += 1


class AWSVersionSymbols(AsvBase):

    rounds = 1
    number = 3 # invoke X times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    library_manager = LibraryManager(storage=Storage.AMAZON, name_benchmark="LIST_SYMBOLS")
    library_type = LibraryType.PERSISTENT

    # NOTE: Change of parameters will trigger failure as original library must also be deleted manually.
    #       Therefore if you plan changes to those numbers make sure to delete old library manually 
    params = [25, 50]
    param_names = ["num_syms"]

    number_columns = 2
    number_rows = 2

    mean_number_versions_per_symbol = 5

    def get_library_manager(self) -> LibraryManager:
        return AWSVersionSymbols.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(None, None) # Tone down creation of structure
        lpp.set_columns(AWSVersionSymbols.number_columns)
        lpp.use_auto_increment_index()
        lpp.generate_versions(versions_max=int(1.5 * AWSVersionSymbols.mean_number_versions_per_symbol), 
                              mean=AWSVersionSymbols.mean_number_versions_per_symbol)
        lpp.generate_metadata().generate_snapshots()
        return lpp

    def setup_cache(self):
        num_rows = AWSListSymbols.number_rows
        manager = self.get_library_manager()
        policy = self.get_population_policy()
        last_snapshot_names_dict = {}
        for number_symbols in AWSVersionSymbols.params:
            start = time.time()
            policy.set_parameters([num_rows] * number_symbols)
            populate_library_if_missing(manager, policy, AWSVersionSymbols.library_type, number_symbols)
            lib = self.get_library_manager().get_library(AWSVersionSymbols.library_type, number_symbols)
            snapshot_name = lib.list_snapshots(load_metadata=False)[-1]
            last_snapshot_names_dict[number_symbols] = snapshot_name
            self.get_logger().info(f"Generated {number_symbols} with {num_rows} each for {time.time()- start}")
        manager.log_info() # Always log the ArcticURIs 
        return last_snapshot_names_dict
    
    def setup(self, last_snapshot_names_dict, num_syms):
        self.population_policy = self.get_population_policy()
        self.lib = self.get_library_manager().get_library(AWSVersionSymbols.library_type, num_syms)
        self.test_counter = 1
        expected_num_versions = AWSVersionSymbols.mean_number_versions_per_symbol * num_syms
        assert num_syms == len(self.lib.list_symbols()), "The library contains expected number of symbols"
        assert expected_num_versions - 1 <= len(self.lib.list_versions()), "There are sufficient versions"
        assert expected_num_versions - 1 <= len(self.lib.list_snapshots()), "There are sufficient snapshots"
        assert last_snapshot_names_dict[num_syms] is not None

    def time_list_versions(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions()

    def time_list_versions_latest_only(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions(latest_only=True)        

    def time_list_versions_skip_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions(skip_snapshots=True)        

    def time_list_versions_latest_only_and_skip_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions(latest_only=True, skip_snapshots=True)        

    def time_list_versions_snapshot(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions(snapshot=last_snapshot_names_dict[num_syms])        

    def peakmem_list_versions(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions()

    def time_list_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots()
    
    def time_list_snapshots_without_metadata(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots(load_metadata=False)

    def peakmem_list_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots()
    
    def peakmem_list_snapshots_without_metadata(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots(load_metadata=False)

