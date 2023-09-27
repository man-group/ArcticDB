"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb import Arctic

from .common import *

class QueryBuilder:
    number = 5
    timeout = 6000

    params = ([10_000_000, 100_000_000])
    param_names = ['num_rows']

    def __init__(self):
        self.ac = Arctic("lmdb://query_builder?map_size=10GB")

        num_rows = QueryBuilder.params
        lib = "query_builder"
        self.ac.delete_library(lib)
        self.ac.create_library(lib)
        lib = self.ac[lib]
        for rows in range(num_rows):
            lib.write(f"{rows}_rows", generate_benchmark_df(rows))

    def setup(self, num_rows):
        pass

    # The names are based on the queries used here: https://duckdblabs.github.io/db-benchmark/
    def time_query_1(self, num_rows):
        lib = self.ac["query_builder"]
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        _ = lib.read(f"{num_rows}_rows", query_builder=q)

    def peakmem_query_1(self, num_rows):
        lib = self.ac["query_builder"]
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        _ = lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_3(self, num_rows):
        lib = self.ac["query_builder"]
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        _ = lib.read(f"{num_rows}_rows", query_builder=q)

    def peakmem_query_3(self, num_rows):
        lib = self.ac["query_builder"]
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        _ = lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_4(self, num_rows):
        lib = self.ac["query_builder"]
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        _ = lib.read(f"{num_rows}_rows", query_builder=q)

    def peakmem_query_4(self, num_rows):
        lib = self.ac["query_builder"]
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        _ = lib.read(f"{num_rows}_rows", query_builder=q)

    def time_query_adv_query_2(self, num_rows):
        lib = self.ac["query_builder"]
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        _ = lib.read(f"{num_rows}_rows", query_builder=q)

    def peakmem_query_adv_query_2(self, num_rows):
        lib = self.ac["query_builder"]
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        _ = lib.read(f"{num_rows}_rows", query_builder=q)

