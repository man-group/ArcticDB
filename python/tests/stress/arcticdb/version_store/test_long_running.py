"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from __future__ import print_function
import random
import gc

from arcticdb.util.memory import format_bytes, current_mem

from arcticdb.util.tasks import (
    write_and_append_simple_df,
    write_large_mixed_df,
    write_pickle,
    delete_random_symbols,
    read_write_sample,
    write_large_mixed_df_prune,
    read_random_symbol_version,
    write_and_prune_simple_df,
    run_scenario,
)


LARGE_DF_SIZE = 100000
ITERATIONS = 10
SLEEP = 1
MAX_MEM_USAGE = 100000

SCENARIOS = [
    write_and_append_simple_df,
    write_large_mixed_df,
    write_pickle,
    delete_random_symbols,
    read_write_sample,
    write_large_mixed_df_prune,
    read_random_symbol_version,
    write_and_prune_simple_df,
    # Add more scenarios here.
]


def test_random_scenario(basic_store_small_segment):
    basic_store_small_segment.version_store._set_validate_version_map()
    for iteration in range(ITERATIONS):
        print("Iteration {}/{}".format(iteration, ITERATIONS))
        run_scenario(random.choice(SCENARIOS), basic_store_small_segment, True, True)
        # assert current_mem() < MAX_MEM_USAGE
        gc.collect()
        print(format_bytes(current_mem()))
