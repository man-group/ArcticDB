"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
import pandas as pd
from arcticdb.util.environment_setup import GeneralSetupLibraryWithSymbols, Storage
from arcticdb.version_store.library import Library
from arcticdb.version_store.processing import QueryBuilder

from benchmarks.common import  generate_benchmark_df
from benchmarks.local_query_builder import PARAMS_QUERY_BUILDER


#region Setup classes

class QueryBuilderFunctionsSettings(GeneralSetupLibraryWithSymbols):
    """
    Responsible for setup special read only symbols generated based
    on special dataframe for benchmarking queries
    """

    def generate_dataframe(self, rows:int, columns: int) -> pd.DataFrame:
        """
        Dataframe that will be used in read and write tests
        """
        st = time.time()
        # NOTE: Use only setup environment logger!
        self.logger().info("Dataframe generation started.")
        df = generate_benchmark_df(rows)
        self.logger().info(f"Dataframe {rows} rows generated for {time.time() - st} sec")
        return df

#endregion

class AWSQueryBuilderFunctions:
    """
    This is same test as :LocalQueryBuilderFunctions:`LocalQueryBuilderFunctions`

    
    """
    
    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    SETUP_CLASS = (QueryBuilderFunctionsSettings(storage=Storage.AMAZON, 
                                                 prefix="BASIC_QUERY")
                                                .set_params(PARAMS_QUERY_BUILDER))

    params = SETUP_CLASS.get_parameter_list()
    param_names = ["num_rows"]

    def setup_cache(self):
        set_env = AWSQueryBuilderFunctions.SETUP_CLASS
        set_env.setup_environment()
        info = set_env.get_storage_info()
        # NOTE: use only logger defined by setup class
        set_env.logger().info(f"storage info object: {info}")
        return info

    def teardown(self,storage_info, num_rows):
        pass

    def setup(self, storage_info, num_rows):
        ## Construct back from arctic url the object
        self.setup_env = QueryBuilderFunctionsSettings.from_storage_info(storage_info)
        self.lib: Library = self.setup_env.get_library()
        self.symbol = self.setup_env.get_symbol_name(num_rows, None)

    # Omit string columns in filtering/projection benchmarks to avoid time/memory being dominated by Python string
    # allocation
    def time_filtering_numeric(self, storage_info, num_rows):
        q = QueryBuilder()
        # v3 is random floats between 0 and 100
        q = q[q["v3"] < 1.0]
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["v3"], query_builder=q).data

    def peakmem_filtering_numeric(self, storage_info, num_rows):
        q = QueryBuilder()
        # v3 is random floats between 0 and 100
        q = q[q["v3"] < 10.0]
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["v3"], query_builder=q).data
        assert data.shape[0] > 1

    def time_filtering_string_isin(self, storage_info, num_rows):
        # Selects about 1% of the rows
        k = num_rows // 1000
        string_set = [f"id{str(i).zfill(3)}" for i in range(1, k + 1)]
        q = QueryBuilder()
        q = q[q["id1"].isin(string_set)]
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["v3"], query_builder=q).data

    def peakmem_filtering_string_isin(self,storage_info, num_rows):
        # Selects about 1% of the rows
        k = num_rows // 1000
        string_set = [f"id{str(i).zfill(3)}" for i in range(1, k + 1)]
        q = QueryBuilder()
        q = q[q["id1"].isin(string_set)]
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["v3"], query_builder=q).data
        assert data.shape[0] > 1

    def time_projection(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.apply("new_col", q["v2"] * q["v3"])
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["new_col"], query_builder=q).data

    def peakmem_projection(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.apply("new_col", q["v2"] * q["v3"])
        data: pd.DataFrame = self.lib.read(self.symbol, columns=["new_col"], query_builder=q).data
        assert data.shape[0] > 1

    # The names are based on the queries used here: https://duckdblabs.github.io/db-benchmark/
    # Don't rename to distinguish from other query tests as renaming makes it a new benchmark, losing historic results
    def time_query_1(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data

    def peakmem_query_1(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data
        assert data.shape[0] > 1

    def time_query_3(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data

    def peakmem_query_3(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data
        assert data.shape[0] > 1

    def time_query_4(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data

    def peakmem_query_4(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data
        assert data.shape[0] > 1

    def time_query_adv_query_2(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data

    def peakmem_query_adv_query_2(self,storage_info, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        data: pd.DataFrame = self.lib.read(self.symbol, query_builder=q).data
        assert data.shape[0] > 1
