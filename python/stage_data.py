"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import itertools
import multiprocessing
from datetime import datetime
from typing import List
import numpy as np
import pandas as pd
from arcticdb import Arctic
from arcticdb.util.test import get_sample_dataframe
import os

import arcticdb_ext
print(f"Using arcticdb_ext {arcticdb_ext.__file__}")


def generate_chunk_sizes(number_chunks:np.uint32, min_rows:np.uint32=100, max_rows:np.uint32=10000) -> List[np.uint32]:
    return np.random.randint(min_rows, max_rows, number_chunks, dtype=np.uint32)


exponent = int(os.environ["NUM_CHUNKS_EXP"])  # eg 3 => 1000 chunks
num_chunks = 10 ** exponent
num_rows_per_chunk = int(1e5)


num_symbols = 1
stages_counter = multiprocessing.Value("i", 0)


def stage_chunk(lib, symbol, chunk, start_date, offset):
    global stages_counter
    with stages_counter.get_lock():
        stages_counter.value += 1
        if stages_counter.value % 100 == 0:
            print(f"Staging chunk {stages_counter.value}")
    df = get_sample_dataframe(size=chunk)
    start = start_date + pd.Timedelta(seconds=offset)
    df.index = pd.date_range(start, periods=chunk, freq="s")
    lib.write(symbol, data=df, validate_index=True, staged=True)


def stage_chunks(lib, symbol, chunk_sizes, start_date, start_offsets):
    inp = zip(itertools.repeat(lib), itertools.repeat(symbol), chunk_sizes, itertools.repeat(start_date), start_offsets)
    with multiprocessing.Pool(20) as p:
        p.starmap(stage_chunk, inp)


ac = Arctic("s3://172.17.0.2:9000:aseaton?access=BDmqp2RBbLjVWE7tljsh&secret=4v4v1aWkH6mYuPhrH85ppytH3fawqyNNBNc3LeD2")
lib = ac.get_library("tst", create_if_missing=True)
lib._nvs.version_store.clear()


if __name__ == "__main__":
    chunk_list = generate_chunk_sizes(num_chunks, num_rows_per_chunk, num_rows_per_chunk + 1)

    symbol = f"staged-0"

    df = get_sample_dataframe(size=1)
    start_date = datetime(1970, 1, 1)
    df.index = pd.date_range(start_date, periods=1, freq="s")
    lib.write(symbol, data=df, prune_previous_versions=True)

    start_offsets = np.cumsum(chunk_list)
    print(f"Starting staging chunks={len(chunk_list)}")
    stage_chunks(lib, symbol, chunk_list, start_date, start_offsets)
    print(f"Finished staging chunks")
