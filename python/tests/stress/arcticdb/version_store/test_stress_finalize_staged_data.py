"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import itertools
import multiprocessing
import sys
from typing import List
import numpy as np
import pytest
from arcticdb import Arctic

from arcticdb.util.utils import CachedDFGenerator, TimestampNumber


from tests.util.mark import SKIP_CONDA_MARK, SLOW_TESTS_MARK

# Uncomment for logging
# set_log_level(default_level="DEBUG", console_output=False, file_output_path="/tmp/arcticdb.log")

def generate_chunk_sizes(number_chunks:np.uint32, min_rows:np.uint32=100, max_rows:np.uint32=10000) -> List[np.uint32]:
    return np.random.randint(min_rows, max_rows, number_chunks, dtype=np.uint32)

class Results:

    def __init__(self):
        self.options = None
        self.iteration = None
        self.number_staged_chunks = 0
        self.total_rows_finalized = 0
        self.finalization_time = None

    def __str__(self): 
        return f"Options: {self.options}\nIteration: {self.iteration}\n# staged chunks: {self.number_staged_chunks}\ntotal rows finalized: {self.total_rows_finalized}\ntime for finalization (s): {self.finalization_time}"


num_rows_initially = int(1e6)
num_chunks = 10
num_rows_per_chunk = int(1e6)
num_symbols = 30
cachedDF = CachedDFGenerator(num_rows_initially + 1, size_string_flds_array=[10])


stages_counter = multiprocessing.Value("i", 0)


def stage_chunk(lib, symbol, chunk):
    global stages_counter
    with stages_counter.get_lock():
        stages_counter.value += 1
        if stages_counter.value % 100 == 0:
            print(f"Staging chunk {stages_counter.value}")
    df = chunk
    lib.write(symbol, data=df, validate_index=True, staged=True)


def get_chunks(chunk_sizes, start_times):
    res = []
    for chunk_size, start_time in zip(chunk_sizes, start_times):
        df = cachedDF.generate_dataframe_timestamp_indexed(chunk_size, start_time, cachedDF.TIME_UNIT)
        res.append(df)
    return res


def stage_chunks(lib, symbol, chunk_sizes, start_times):
    chunks = get_chunks(chunk_sizes, start_times)
    inp = zip(itertools.repeat(lib), itertools.repeat(symbol), chunks)
    with multiprocessing.Pool(10) as p:
        p.starmap(stage_chunk, inp)


#ac = Arctic("lmdb:///home/alex/big_disk/part/lmdb_one?map_size=100GB")
ac = Arctic("s3://172.17.0.2:9000:aseaton?access=3SePAqKdc1O7JgeDIJob&secret=zhtHzQtQt7UZJVUHk3QtpShSeRYZozwEl0pVeq8A")
lib = ac.get_library("tst", create_if_missing=True)
lib._nvs.version_store.clear()


@SLOW_TESTS_MARK
@SKIP_CONDA_MARK # Conda CI runner doesn't have enough storage to perform these stress tests
@pytest.mark.skipif(sys.platform == "win32", reason="Not enough storage on Windows runners")
def test_finalize_monotonic_unique_chunks():
    """
        The test is designed to staged thousands of chunks with variable chunk size.
        To experiment on local computer you can move up to 20k number of chunks approx 10k each

        For stress testing this number is reduced due to github runner HDD size - 16 GB only

        On local disk you must use "arctic_library_lmdb" fixture as it sets 100 GB limit.
        If you use "basic_arctic_library" you might end with much more space taken eating all your space
        if you want to experiment with more number of chunks
    """

    # We would need to generate as fast as possible kind of random
    # dataframes. To do that we build a large cache and will 
    # sample rows from there as we need to run as fast as we can

    # This will serve us as a counter and at the same time it provides unique index for each row
    total_number_rows: TimestampNumber = TimestampNumber(0, cachedDF.TIME_UNIT) # Synchronize index frequency

    print(f"Writing to symbol initially {num_rows_initially} rows")
    df = cachedDF.generate_dataframe_timestamp_indexed(num_rows_initially, total_number_rows, cachedDF.TIME_UNIT)

    print(f"Start staging chunks .... with {num_chunks} chunks")
    chunk_list = generate_chunk_sizes(num_chunks, num_rows_per_chunk, num_rows_per_chunk + 1)
    print(f"Chunks to stage {len(chunk_list)} ")

    for i in range(num_symbols):
        symbol = f"staged-{i}"
        lib.write(symbol, data=df, prune_previous_versions=True)

        start_times = np.cumsum(chunk_list)
        start_times = np.roll(start_times, 1)
        start_times[0] = 0
        start_times += num_rows_initially
        print(f"chunk_list={chunk_list} start_times={start_times}")
        stage_chunks(lib, symbol, chunk_list, start_times)
        print(f"SYMBOL ACTUAL ROWS before finalization - {lib._nvs.get_num_rows(symbol)} ")
