"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder

from .common import *


class LocalQueryBuilderFunctions:
    number = 5
    timeout = 6000
    LIB_NAME = "query_builder"
    CONNECTION_STRING = "lmdb://query_builder?map_size=5GB"

    params = [1_000_000, 10_000_000]
    param_names = ["num_rows"]

    def setup_cache(self):
        self.ac = Arctic(LocalQueryBuilderFunctions.CONNECTION_STRING)

        num_rows = LocalQueryBuilderFunctions.params
        self.lib_name = LocalQueryBuilderFunctions.LIB_NAME
        self.ac.delete_library(self.lib_name)
        lib = self.ac.create_library(self.lib_name)
        for rows in num_rows:
            lib.write(f"{rows}_rows", generate_benchmark_df(rows))

    def teardown(self, num_rows):
        del self.lib
        del self.ac

    def setup(self, num_rows):
        self.ac = Arctic(LocalQueryBuilderFunctions.CONNECTION_STRING)
        self.lib = self.ac[LocalQueryBuilderFunctions.LIB_NAME]

    # Omit string columns in filtering/projection benchmarks to avoid time/memory being dominated by Python string
    # allocation
    def time_filtering_numeric(self, num_rows):
        q = QueryBuilder()
        # v3 is random floats between 0 and 100
        q = q[q["v3"] < 1.0]
        self.lib.read(f"{num_rows}_rows", columns=["v3"], query_builder=q)

    def peakmem_filtering_numeric(self, num_rows):
        q = QueryBuilder()
        # v3 is random floats between 0 and 100
        q = q[q["v3"] < 10.0]
        self.lib.read(f"{num_rows}_rows", columns=["v3"], query_builder=q)

    def time_filtering_string_isin(self, num_rows):
        # Selects about 1% of the rows
        k = num_rows // 1000
        string_set = [f"id{str(i).zfill(3)}" for i in range(1, k + 1)]
        q = QueryBuilder()
        q = q[q["id1"].isin(string_set)]
        self.lib.read(f"{num_rows}_rows", columns=["v3"], query_builder=q)

    def peakmem_filtering_string_isin(self, num_rows):
        # Selects about 1% of the rows
        k = num_rows // 1000
        string_set = [f"id{str(i).zfill(3)}" for i in range(1, k + 1)]
        q = QueryBuilder()
        q = q[q["id1"].isin(string_set)]
        self.lib.read(f"{num_rows}_rows", columns=["v3"], query_builder=q)

    def time_projection(self, num_rows):
        q = QueryBuilder()
        q = q.apply("new_col", q["v2"] * q["v3"])
        self.lib.read(f"{num_rows}_rows", columns=["new_col"], query_builder=q)

    def peakmem_projection(self, num_rows):
        q = QueryBuilder()
        q = q.apply("new_col", q["v2"] * q["v3"])
        self.lib.read(f"{num_rows}_rows", columns=["new_col"], query_builder=q)

    # The names are based on the queries used here: https://duckdblabs.github.io/db-benchmark/
    # Don't rename to distinguish from other query tests as renaming makes it a new benchmark, losing historic results
    def time_query_1(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def peakmem_query_1(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_3(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def peakmem_query_3(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_4(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def peakmem_query_4(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_adv_query_2(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)

    def peakmem_query_adv_query_2(self, num_rows):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        self.lib.read(f"{num_rows}_rows", query_builder=q)
