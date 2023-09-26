from arcticdb.vector_db.py_vector_db import PyVectorDB
import numpy as np
import pandas as pd
import time
import faiss

def test_faiss():
    num_vecs = 5*10**5
    dimension = 100

    index = faiss.index_factory(dimension, "HNSW32", faiss.METRIC_L2)
    start = time.time()
    index.add(np.random.rand(num_vecs, dimension).astype(np.float32))
    end = time.time()
    fps = 10**-6 * (num_vecs * dimension)/(end-start)
    print(0)

def test_so_far():
    np.random.seed(0)
    pvdb = PyVectorDB("lmdb:///scratch/data/jpl/pyvdb")
    write_speeds = {}
    search_speeds = {}

    tempus = time.time()
    lib = pvdb.get_vector_library(f"vector_library_{tempus}", create_if_missing=True)

    for upsert_vectors in [10**6]:
        for dimension in [1000]:
            print(f"{upsert_vectors} vectors, {dimension} dimensions.")
            metric = "L2"
            bucketiser = "exact"
            buckets = 1000
            training_vectors = 1000

            initial_vectors = 1000

            lib.create_namespace("vectors", dimension, "L2", "exact", buckets, None, np.random.rand(training_vectors, dimension).astype(np.float32))
            for i in range(1000):
                upsertion_vectors = pd.DataFrame(np.random.rand(upsert_vectors, dimension).astype(np.float32))                begin = time.time()
                lib.upsert("vectors", upsertion_vectors)
                # lib.upsert("vectors", pd.DataFrame(np.random.rand(upsert_vectors, dimension).astype(np.float32)).tail(upsert_vectors - initial_vectors))
                upsert = time.time()
                write_speeds[(upsert_vectors, dimension)] = 10**-6 * (upsert_vectors*dimension) / (upsert - begin)
            for k in [5, 10, 15]:
                for query_vectors in [1, 2, 5, 10, 20, 100]:
                    for nprobes in [1, 5, 10, 20, 50, 100, 200]:
                        print(f"k = {k}, query_vectors = {query_vectors}, nprobes = {nprobes}")
                        start = time.time()
                        res = lib.query("vectors", np.random.rand(query_vectors, dimension).astype(np.float32), k, nprobes)
                        end = time.time()
                        search_speeds[(upsert_vectors, dimension, k, query_vectors, nprobes)] = 10**-6 * (upsert_vectors*dimension*query_vectors) / (end-start)
                        with open("timings.txt", "w") as f: print(str(search_speeds), file=f)
    print(0)
    pass