"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb.util.environment_setup import GeneralSetupSymbolsVersionsSnapshots, Storage

class AWSListSymbols:

    rounds = 1
    number = 3 # invoke X times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    
    SETUP_CLASS = (GeneralSetupSymbolsVersionsSnapshots(storage=Storage.AMAZON, prefix="LIST_SYMBOLS")
        .set_with_metadata_for_each_version()
        .set_with_snapshot_for_each_version()
        .set_params([500, 1000]))
        #.set_params([10, 20])) # For test purposes 

    params = SETUP_CLASS.get_parameter_list()
    param_names = ["num_syms"]

    def setup_cache(self):
        aws_setup = AWSListSymbols.SETUP_CLASS
        if aws_setup.check_ok():
            aws_setup.clear_symbols_cache()
        else:
            aws_setup.setup_all()
        info = aws_setup.get_storage_info()
        # NOTE: use only logger defined by setup class
        aws_setup.logger().info(f"storage info object: {info}")
        return info

    def setup(self, storage_info, num_syms):
        self.aws = GeneralSetupSymbolsVersionsSnapshots.from_storage_info(storage_info)
        self.lib = self.aws.get_library(num_syms)

    def time_list_symbols(self, storage_info, num_syms):
        self.lib.list_symbols()

    def time_has_symbol_nonexisting(self, storage_info, num_syms):
        self.lib.has_symbol("250_sym")        

    def peakmem_list_symbols(self, storage_info, num_syms):
        self.lib.list_symbols()

    def time_list_symbols_first_snapshot(self, storage_info, num_syms):
        self.lib.list_symbols(self.aws.first_snapshot)

    def time_list_symbols_last_snapshot(self, storage_info, num_syms):
        self.lib.list_symbols(self.aws.last_snapshot)

class AWSVersionSymbols:

    rounds = 1
    number = 3 # invoke X times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    SETUP_CLASS = (GeneralSetupSymbolsVersionsSnapshots(storage=Storage.AMAZON, prefix="LIST_VERSIONS")
        .set_mean_number_versions_per_sym(35) # change to lower for testing
        .set_max_number_versions(50) # number versions is approx = num_syms * mean_number_versions
        .set_with_metadata_for_each_version()
        .set_with_snapshot_for_each_version()
        .set_params([25, 50])) # for test purposes: .set_params([5, 6]))

    params = SETUP_CLASS.get_parameter_list()
    param_names = ["num_syms"]

    def setup_cache(self):
        aws = AWSVersionSymbols.SETUP_CLASS.setup_environment() 
        info = aws.get_storage_info()
        # NOTE: use only logger defined by setup class
        aws.logger().info(f"storage info object: {info}")
        return info

    def setup(self, storage_info, num_syms):
        self.aws = GeneralSetupSymbolsVersionsSnapshots.from_storage_info(storage_info)
        self.lib = self.aws.get_library(num_syms)

    def time_list_versions(self, storage_info, num_syms):
        self.lib.list_versions()

    def time_list_versions_latest_only(self, storage_info, num_syms):
        self.lib.list_versions(latest_only=True)        

    def time_list_versions_skip_snapshots(self, storage_info, num_syms):
        self.lib.list_versions(skip_snapshots=self.aws.last_snapshot)        

    def time_list_versions_snapshot(self, storage_info, num_syms):
        self.lib.list_versions(snapshot=self.aws.last_snapshot)        

    def peakmem_list_versions(self, storage_info, num_syms):
        self.lib.list_versions()

