import arcticdb
from arcticdb.util.test import random_strings_of_length, random_string
import datetime
import pandas as pd
from datetime import datetime
from pandas.testing import assert_frame_equal
import numpy as np
from multiprocessing.pool import ThreadPool
from threading import Thread, Event


def test_stress_all_strings(lmdb_version_store_big_map):
    lib = lmdb_version_store_big_map
    num_columns = 10
    symbol = "stress_strings"
    string_length = 10
    num_rows = 100000
    columns = random_strings_of_length(num_columns, string_length, True)
    data = {col : random_strings_of_length(num_rows, string_length, False) for col in columns}
    df = pd.DataFrame(data)
    lib.write(symbol, df)
    start_time = datetime.now()
    vit = lib.read(symbol)
    print("Read took {}".format(datetime.now() - start_time))
    assert_frame_equal(df, vit.data)


def test_stress_all_strings_dynamic(lmdb_version_store_big_map):
    lib = lmdb_version_store_big_map
    num_columns = 10
    symbol = "stress_strings"
    string_length = 10
    num_rows = 100000
    columns = random_strings_of_length(num_columns, string_length, True)
    data = {col : random_strings_of_length(num_rows, string_length, False) for col in columns}
    df = pd.DataFrame(data)
    lib.write(symbol, df, dynamic_strings=True)
    start_time = datetime.now()
    vit = lib.read(symbol)
    print("Read took {}".format(datetime.now() - start_time))
    assert_frame_equal(df, vit.data)


def dataframe_with_none_and_nan(rows: int, cols: int):
    return pd.DataFrame(np.asfortranarray(np.random.choice([None, np.nan, str(np.random.randn())], size=(rows, cols))))
def alloc_nones_and_nans():
    nones = [None for _ in range(200_000)]
    nans = [np.nan for _ in range(200_000)]
    return nones, nans

class TestConcurrentHandlingOfNoneAndNan:
    """
    Tests the proper handling of the None refcount. None is a global static object that should never go away, however it
    is refcounted. String columns in ArcticDB are allowed to have None values. ArcticDB must increment the refcount in
    order to do so the GIL must be held. This creates a background job in a separate Python thread to constantly
    allocate and deallocate None and NaN values and then runs multiple Python threads doing a reads. This way we test
    that Arctic and the background allocation thread are not racing on the None refcount. Each symbol has multiple
    string columns to also test that Arctic native threads are not racing on the None refcount. We also add NaN values
    as their refcount is also managed by Arctic. Note that in contrast to None, NaN is not a global static.
    """
    def setup_method(self, method):
        self.done_reading = Event()

    def spin_none_nan_creation(self):
        while not self.done_reading.is_set():
            alloc_nones_and_nans()
    def test_stress_parallel_strings(self, s3_storage, lib_name):
        ac = s3_storage.create_arctic()
        lib = ac.create_library(lib_name)
        symbol_count = 20
        write_payload = [arcticdb.WritePayload(symbol=f"stringy{i}", data=dataframe_with_none_and_nan(150_000, 20)) for i in range(symbol_count)]
        lib.write_batch(write_payload)
        none_nan_background_creator = Thread(target=self.spin_none_nan_creation)
        none_nan_background_creator.start()
        jobs = [payload for rep in range(5) for payload in write_payload]
        with ThreadPool(10) as pool:
            for _ in pool.imap_unordered(lambda payload: lib.read(payload.symbol).data, jobs):
                pass
            self.done_reading.set()
        none_nan_background_creator.join()


