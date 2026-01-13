"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
from arcticdb.version_store.processing import QueryBuilder
from asv_runner.benchmarks.mark import SkipNotImplemented

from .common import *
from .environment_setup import create_libraries_across_storages, is_storage_enabled, Storage

PARAMS_QUERY_BUILDER = [1_000_000, 10_000_000]


def _symbol_name(rows):
    return f"sym-{rows}"


class QueryBuilderFunctions:
    sample_time = 2
    rounds = 2
    repeat = (1, 10, 20.0)
    warmup_time = 0.2
    timeout = 600

    num_rows = [1_000_000, 10_000_000]
    storages = [Storage.LMDB, Storage.AMAZON]

    params = [num_rows, storages]
    param_names = ["num_rows", "storages"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.symbol = None

    def setup_cache(self):
        start = time.time()
        lib_for_storage = create_libraries_across_storages(self.storages)

        for rows in QueryBuilderFunctions.num_rows:
            df = generate_benchmark_df(rows)
            sym = _symbol_name(rows)
            for storage in QueryBuilderFunctions.storages:
                if not is_storage_enabled(storage):
                    continue
                lib = lib_for_storage[storage]
                self.logger.info(f"writing {df.shape} under {sym}")
                lib.write(sym, df)

        self.logger.info(f"setup_cache time: {time.time() - start}")
        return lib_for_storage

    def setup(self, lib_for_storage, num_rows, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented
        self.symbol = _symbol_name(num_rows)

    # Omit string columns in filtering/projection benchmarks to avoid time/memory being dominated by Python string
    # allocation
    def time_filtering_numeric(self, *args):
        q = QueryBuilder()
        # v3 is random floats between 0 and 100
        q = q[q["v3"] < 1.0]
        self.lib.read(self.symbol, columns=["v3"], query_builder=q)

    def peakmem_filtering_numeric(self, *args):
        q = QueryBuilder()
        # v3 is random floats between 0 and 100
        q = q[q["v3"] < 10.0]
        self.lib.read(self.symbol, columns=["v3"], query_builder=q)

    def time_filtering_string_isin(self, lib_for_storage, num_rows, storage):
        # Selects about 1% of the rows
        k = num_rows // 1000
        string_set = [f"id{str(i).zfill(3)}" for i in range(1, k + 1)]
        q = QueryBuilder()
        q = q[q["id1"].isin(string_set)]
        self.lib.read(self.symbol, columns=["v3"], query_builder=q)

    def peakmem_filtering_string_isin(self, lib_for_storage, num_rows, storage):
        # Selects about 1% of the rows
        k = num_rows // 1000
        string_set = [f"id{str(i).zfill(3)}" for i in range(1, k + 1)]
        q = QueryBuilder()
        q = q[q["id1"].isin(string_set)]
        self.lib.read(self.symbol, columns=["v3"], query_builder=q)

    def time_filtering_string_regex_match(self, *args):
        pattern = f"^id\d\d\d$"
        q = QueryBuilder()
        q = q[q["id1"].regex_match(pattern)]
        self.lib.read(self.symbol, columns=["v3"], query_builder=q)

    def time_projection(self, *args):
        q = QueryBuilder()
        q = q.apply("new_col", q["v2"] * q["v3"])
        self.lib.read(self.symbol, columns=["new_col"], query_builder=q)

    def peakmem_projection(self, *args):
        q = QueryBuilder()
        q = q.apply("new_col", q["v2"] * q["v3"])
        self.lib.read(self.symbol, columns=["new_col"], query_builder=q)

    # The names are based on the queries used here: https://duckdblabs.github.io/db-benchmark/
    # Don't rename to distinguish from other query tests as renaming makes it a new benchmark, losing historic results
    def time_query_1(self, *args):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        self.lib.read(self.symbol, query_builder=q)

    def peakmem_query_1(self, *args):
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        self.lib.read(self.symbol, query_builder=q)

    def time_query_3(self, *args):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        self.lib.read(self.symbol, query_builder=q)

    def peakmem_query_3(self, *args):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "sum", "v3": "sum"})
        self.lib.read(self.symbol, query_builder=q)

    def time_query_4(self, *args):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        self.lib.read(self.symbol, query_builder=q)

    def peakmem_query_4(self, *args):
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum", "v2": "sum"})
        self.lib.read(self.symbol, query_builder=q)

    def time_query_adv_query_2(self, *args):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        self.lib.read(self.symbol, query_builder=q)

    def peakmem_query_adv_query_2(self, *args):
        q = QueryBuilder()
        q = q.groupby("id3").agg({"v1": "max", "v2": "min"})
        self.lib.read(self.symbol, query_builder=q)
