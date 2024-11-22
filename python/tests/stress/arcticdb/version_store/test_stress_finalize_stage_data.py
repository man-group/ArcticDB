"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import copy
import gc
import time
import numpy as np
import pandas as pd
from typing import Union, List
from arcticdb.version_store.library import Library, StagedDataFinalizeMethod
from arcticdb.config import Defaults, set_log_level
from arcticdb.util.utils import CachedDFGenerator, TimestampNumber, stage_chunks

from typing import Union
import numpy as np
import pandas as pd

# Uncomment for logging
# set_log_level(default_level="DEBUG", console_output=False, file_output_path="/tmp/arcticdb.log")

def generate_chunk_sizes(number_chunks:np.uint32, min_rows:np.uint32=100, max_rows:np.uint32=10000) -> List[np.uint32]:
    return np.random.randint(min_rows, max_rows, number_chunks, dtype=np.uint32)

def test_finalize_monotonic_unique_chunks(arctic_library_lmdb):

    options = [
        {"chunks_descending" : False, "finalization_mode" : StagedDataFinalizeMethod.APPEND},
        {"chunks_descending" : True, "finalization_mode" : StagedDataFinalizeMethod.APPEND},
        {"chunks_descending" : False, "finalization_mode" : StagedDataFinalizeMethod.WRITE},
        {"chunks_descending" : True, "finalization_mode" : StagedDataFinalizeMethod.WRITE},
        ]

    # Will hold the results after each iteration:
    #  - iteration chunks
    #  - chunks staged for finalization
    #  - rows finalized for iteration 
    #  - time for finalization
    results = []

    lib : Library = arctic_library_lmdb

    # We would need to generate as fast as possible kind of random
    # dataframes. To do that we build a large cache and will 
    # sample rows from there as we need to run as fast as we can
    cachedDF = CachedDFGenerator(250000)

    total_number_rows_all_iterations: int = 0

    # This will serve us as a counter and at the same time it provides unique index for each row
    total_number_rows: TimestampNumber = TimestampNumber(0, cachedDF.TIME_UNIT) # Synchronize index frequency
    INITIAL_TIMESTAMP: TimestampNumber = TimestampNumber(0, cachedDF.TIME_UNIT) # Synchronize index frequency
    symbol="staged"

    num_rows_initially = 99999
    print(f"Writing to symbol initially {num_rows_initially} rows")
    df = cachedDF.generate_dataframe_timestamp_indexed(num_rows_initially, total_number_rows, cachedDF.TIME_UNIT)

    cnt = 0
    res = {}
    for iter in [1000, 1000, 1000, 1000 ,5000, 5000, 5000, 5000, 10000, 10000, 10000, 10000] :

        total_number_rows = INITIAL_TIMESTAMP + num_rows_initially
        lib.write(symbol, data=df, prune_previous_versions=True)

        print(f"Start staging chunks .... for iter {cnt} with {iter} chunks")
        print(f"Using options {options[cnt % 4]}")
        chunk_list = generate_chunk_sizes(iter, 9000, 11000)
        gc.collect()
        print(f"Chunks to stage {len(chunk_list)} ")
        stage_chunks(lib, symbol, cachedDF, total_number_rows, chunk_list, options[cnt % 4]["chunks_descending"])

        if (options[cnt % 4]["finalization_mode"] == StagedDataFinalizeMethod.APPEND):
            total_number_rows = total_number_rows + sum(chunk_list)
        else:
            total_number_rows = INITIAL_TIMESTAMP + sum(chunk_list)


        print("--" * 50)
        print(f"STAGED ROWS {total_number_rows.get_value()} after iteration {cnt}")
        print(f"SYMBOL ACTUAL ROWS before finalization - {lib._nvs.get_num_rows(symbol)} ")
        start_time = time.time()
        lib.finalize_staged_data(symbol=symbol, mode=options[cnt % 4]["finalization_mode"])
        finalization_time = time.time() - start_time
        gc.collect()
        print(f"SYMBOL ACTUAL ROWS after finalization {lib._nvs.get_num_rows(symbol)} ")
        print("--" * 50)

        assert total_number_rows == lib._nvs.get_num_rows(symbol)
        cnt = cnt + 1

        total_number_rows_all_iterations = total_number_rows_all_iterations + total_number_rows
        print(f"TOTAL ROWS INSERTED IN ALL ITERATIONS: {total_number_rows_all_iterations}")

        res["options"] = options[cnt % 4]
        res["iteration"] = cnt
        res["number_staged_chunks"] = iter
        res["total_rows_finalized"] = total_number_rows_all_iterations
        res["finalization time"] = finalization_time

        results.append(res)

        total_number_rows.to_zero() # next iteration start from 0

    for res in results:
        print("_" * 100)
        print(res)
