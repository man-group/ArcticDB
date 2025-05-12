"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from logging import Logger
import pandas as pd
from arcticdb.util.environment_setup import DataFrameGenerator, TestLibraryManager, LibraryPopulationPolicy, LibraryType, Storage, get_console_logger, populate_library_if_missing
from arcticdb.version_store.library import Library
from arcticdb.version_store.processing import QueryBuilder

from benchmarks.common import  AsvBase, generate_benchmark_df
from benchmarks.local_query_builder import PARAMS_QUERY_BUILDER


#region Setup classes

class QueryBuilderGenerator(DataFrameGenerator):

    def get_dataframe(self, number_rows, number_columns) -> pd.DataFrame:
        """
        Dataframe that will be used in read and write tests
        """
        return generate_benchmark_df(number_rows)
    
#endregion

class AWSQueryBuilderFunctions(AsvBase):
    """
    This is same test as :LocalQueryBuilderFunctions:`LocalQueryBuilderFunctions`

    """
    
    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    # NOTE: If you plan to make changes to parameters, consider that a library with previous definition 
    #       may already exist. This means that symbols there will be having having different number
    #       of rows than what you defined in the test. To resolve this problem check with documentation:
    #           https://github.com/man-group/ArcticDB/wiki/ASV-Benchmarks:-Real-storage-tests
    params = PARAMS_QUERY_BUILDER
    param_names = ["num_rows"]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="QUERY_BUILDER")

    def get_logger(self) -> Logger:
        return get_console_logger(self)

    def get_library_manager(self) -> TestLibraryManager:
        return AWSQueryBuilderFunctions.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(self.get_logger(), QueryBuilderGenerator())
        lpp.set_parameters(AWSQueryBuilderFunctions.params)
        return lpp
    
    def setup_cache(self):
        '''
        In setup_cache we only populate the persistent libraries if they are missing.
        '''
        manager = self.get_library_manager()
        policy = self.get_population_policy()
        populate_library_if_missing(manager, policy, LibraryType.PERSISTENT)
        manager.log_info() # Logs info about ArcticURI - do always use last

    def teardown(self, num_rows):
        pass

    def setup(self, num_rows):
        ## Construct back from arctic url the object
        self.lib: Library = self.get_library_manager().get_library(LibraryType.PERSISTENT)
        self.policy = self.get_population_policy()
        self.symbol =  self.policy.get_symbol_name(num_rows)

    # Omit string columns in filtering/projection benchmarks to avoid time/memory being dominated by Python string
    # allocation
    def time_filtering_numeric(self, num_rows):
        q = QueryBuilder()
        # v3 is random floats between 0 and 100
        q = q[q["v3"] < 1.0]
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["v3"], query_builder=q).data

    def peakmem_filtering_numeric(self, num_rows):
        q = QueryBuilder()
        # v3 is random floats between 0 and 100
        q = q[q["v3"] < 10.0]
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["v3"], query_builder=q).data
        assert data.shape[0] > 1

    def time_filtering_string_isin(self, num_rows):
        # Selects about 1% of the rows
        k = num_rows // 1000
        string_set = [f"id{str(i).zfill(3)}" for i in range(1, k + 1)]
        q = QueryBuilder()
        q = q[q["id1"].isin(string_set)]
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["v3"], query_builder=q).data

    def peakmem_filtering_string_isin(self, num_rows):
        # Selects about 1% of the rows
        k = num_rows // 1000
        string_set = [f"id{str(i).zfill(3)}" for i in range(1, k + 1)]
        q = QueryBuilder()
        q = q[q["id1"].isin(string_set)]
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["v3"], query_builder=q).data
        assert data.shape[0] > 1

    def time_projection(self, num_rows):
        q = QueryBuilder()
        q = q.apply("new_col", q["v2"] * q["v3"])
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["new_col"], query_builder=q).data

    def peakmem_projection(self, num_rows):
        q = QueryBuilder()
        q = q.apply("new_col", q["v2"] * q["v3"])
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["new_col"], query_builder=q).data
        assert data.shape[0] > 1

    # The names are based on the queries used here: https://duckdblabs.github.io/db-benchmark/
    # Don't rename to distinguish from other query tests as renaming makes it a new benchmark, losing historic results
    def time_query_1(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data

    def peakmem_query_1(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data
        assert data.shape[0] > 1

    def time_query_3(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data

    def peakmem_query_3(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data
        assert data.shape[0] > 1

    def time_query_4(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data

    def peakmem_query_4(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data
        assert data.shape[0] > 1

    def time_query_adv_query_2(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data

    def peakmem_query_adv_query_2(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data
        assert data.shape[0] > 1
