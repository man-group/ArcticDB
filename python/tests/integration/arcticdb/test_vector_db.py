from arcticdb.vector_db.py_vector_db import PyVectorDB
import numpy as np
import pandas as pd
import time

def test_so_far():
    np.random.seed(0)
    pvdb = PyVectorDB("lmdb://pvdb")
    write_speeds = {}
    search_speeds = {}

    tempus = time.time()
    lib = pvdb.get_vector_library(f"vector_library_{tempus}", create_if_missing=True)

    for upsert_vectors in [10**5, 10**6, 10**7, 10**8]:
        for dimension in [100, 500, 1000, 10000]:
            metric = "L2"
            bucketiser = "exact"
            buckets = 50
            training_vectors = 1000

            initial_vectors = 1000

            lib.create_namespace("vectors", dimension, "L2", "exact", buckets, "HNSW,IDMap", np.random.rand(training_vectors, dimension).astype(np.float32))

            begin = time.time()
            lib.upsert("vectors", pd.DataFrame(np.random.rand(upsert_vectors, dimension).astype(np.float32)))
            # lib.upsert("vectors", pd.DataFrame(np.random.rand(upsert_vectors, dimension).astype(np.float32)).tail(upsert_vectors - initial_vectors))
            upsert = time.time()
            write_speeds[(upsert_vectors, dimension)] = 10**-6 * (upsert_vectors*dimension) / (upsert - begin)
            start = time.time()
            for k in [5, 10, 15, 20, 100]:
                for query_vectors in [1, 2, 5, 10, 20, 100]:
                    res = lib.query("vectors", np.random.rand(10, dimension).astype(np.float32), 15, 8)
                    end = time.time()
                    search_speeds[(upsert_vectors, dimension, k, query_vectors)] = 10**-6 * (upsert_vectors*dimension*query_vectors) / (end-start)
    print(0)
    pass