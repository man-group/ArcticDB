"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb.util.test import get_sample_dataframe
from arcticdb.util.memory import current_mem

LARGE_DF_SIZE = 100000
ITERATIONS = 100


def test_read_write_flat_memory(s3_version_store):
    start_mem = current_mem()
    df = get_sample_dataframe(LARGE_DF_SIZE, 0)

    for x in range(ITERATIONS):
        print("Doing iteration {}".format(x))
        s3_version_store.write("testing", df, dynamic_strings=True, prune_previous=True)
        vit = s3_version_store.read("testing")
        end_mem = current_mem()
        print("Total mem increase (in bytes)=", end_mem - start_mem)
