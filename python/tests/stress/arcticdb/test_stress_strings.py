import arcticdb
from arcticdb.util.test import random_strings_of_length, random_string
import datetime
import pandas as pd
from datetime import datetime
from pandas.testing import assert_frame_equal
import numpy as np
from multiprocessing.pool import ThreadPool
from threading import Thread, Event
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.storage import KeyType
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata
from arcticdb.version_store._custom_normalizers import register_normalizer, clear_registered_normalizers
from arcticdb.util.test import CustomDictNormalizer, CustomDict
from tests.conftest import Marks
from tests.util.marking import marks


def test_stress_all_strings(lmdb_version_store_big_map):
    lib = lmdb_version_store_big_map
    num_columns = 10
    symbol = "stress_strings"
    string_length = 10
    num_rows = 100000
    columns = random_strings_of_length(num_columns, string_length, True)
    data = {col: random_strings_of_length(num_rows, string_length, False) for col in columns}
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
    data = {col: random_strings_of_length(num_rows, string_length, False) for col in columns}
    df = pd.DataFrame(data)
    lib.write(symbol, df, dynamic_strings=True)
    start_time = datetime.now()
    vit = lib.read(symbol)
    print("Read took {}".format(datetime.now() - start_time))
    assert_frame_equal(df, vit.data)


def dataframe_with_none_and_nan(rows: int, cols: int):
    return pd.DataFrame(
        {f"col_{i}": np.random.choice([None, np.nan, str(np.random.randn())], size=rows) for i in range(cols)}
    )


def alloc_nones_and_nans():
    nones = [None for _ in range(200_000)]
    nans = [np.nan for _ in range(200_000)]
    return nones, nans


class TestConcurrentHandlingOfNoneAndNan:
    """
    Tests the proper handling of the None refcount. None is a global static object that should never go away; however, it
    is refcounted. String columns in ArcticDB are allowed to have None values. ArcticDB must increment the refcount
    to do so the GIL must be held. This creates a background job in a separate Python thread to constantly allocate and
    deallocate None and NaN values and then runs multiple Python threads doing a reads. This way we test that Arctic and
    the background allocation thread are not racing on the None refcount. Each symbol has multiple string columns to
    also test that Arctic native threads are not racing on the None refcount. We also add NaN values as their refcount
    is also managed by Arctic. Note that in contrast to None, NaN is not a global static variable.
    """

    def setup_method(self, method):
        self.done_reading = Event()

    def spin_none_nan_creation(self):
        while not self.done_reading.is_set():
            alloc_nones_and_nans()

    def start_background_thread(self):
        none_nan_background_creator = Thread(target=self.spin_none_nan_creation)
        none_nan_background_creator.start()
        return none_nan_background_creator

    def init_dataframe(self, lib, symbol_count):
        write_payload = [
            arcticdb.WritePayload(symbol=f"stringy{i}", data=dataframe_with_none_and_nan(150_000, 20))
            for i in range(symbol_count)
        ]
        lib.write_batch(write_payload)
        return write_payload

    def test_stress_parallel_strings_read(self, s3_storage, lib_name):
        ac = s3_storage.create_arctic()
        lib = ac.create_library(lib_name)
        write_payload = self.init_dataframe(lib, symbol_count=20)
        none_nan_background_creator = self.start_background_thread()
        jobs = [payload for rep in range(5) for payload in write_payload]
        with ThreadPool(10) as pool:
            for _ in pool.imap_unordered(lambda payload: lib.read(payload.symbol).data, jobs):
                pass
            self.done_reading.set()
        none_nan_background_creator.join()

    def test_stress_parallel_strings_read_batch(self, s3_storage, lib_name):
        ac = s3_storage.create_arctic()
        lib = ac.create_library(lib_name)
        write_payload = self.init_dataframe(lib, symbol_count=20)
        none_nan_background_creator = self.start_background_thread()
        jobs = [[payload.symbol for payload in write_payload]] * 15
        with ThreadPool(10) as pool:
            for _ in pool.imap_unordered(lambda symbol_list: lib.read_batch(symbol_list), jobs):
                pass
            self.done_reading.set()
        none_nan_background_creator.join()

    @marks([Marks.pipeline])
    def test_stress_parallel_strings_query_builder(self, s3_storage, lib_name):
        ac = s3_storage.create_arctic()
        lib = ac.create_library(lib_name)
        write_payload = self.init_dataframe(lib, symbol_count=20)
        none_nan_background_creator = self.start_background_thread()
        jobs = [payload for rep in range(5) for payload in write_payload]
        qb = QueryBuilder()
        qb = qb[
            qb["col_0"].isnull()
            | qb["col_1"].isnull()
            | qb["col_2"].isnull()
            | qb["col_3"].isnull()
            | qb["col_4"].isnull()
            | qb["col_5"].isnull()
            | qb["col_6"].isnull()
            | qb["col_7"].isnull()
            | qb["col_8"].isnull()
            | qb["col_9"].isnull()
        ]
        with ThreadPool(10) as pool:
            for _ in pool.imap_unordered(lambda payload: lib.read(payload.symbol, query_builder=qb), jobs):
                pass
            self.done_reading.set()
        none_nan_background_creator.join()

    def test_library_tool(self, s3_storage, lib_name):
        ac = s3_storage.create_arctic()
        lib = ac.create_library(lib_name)
        write_payload = self.init_dataframe(lib, symbol_count=20)
        none_nan_background_creator = self.start_background_thread()
        lt = lib._dev_tools.library_tool()
        keys = [key for payload in write_payload for key in lt.find_keys_for_symbol(KeyType.TABLE_DATA, payload.symbol)]
        with ThreadPool(10) as pool:
            for _ in pool.imap_unordered(lambda key: lt.read_to_dataframe(key), keys):
                pass
            self.done_reading.set()
        none_nan_background_creator.join()

    def test_batch_read_keys(self, s3_storage, lib_name):
        register_normalizer(CustomDictNormalizer())
        ac = s3_storage.create_arctic()
        lib = ac.create_library(lib_name)
        symbol_count = 11
        for sym_idx in range(symbol_count):
            data = {f"data{i}": dataframe_with_none_and_nan(150_000, 11) for i in range(5)}
            lib._nvs.write(f"sym{sym_idx}", data)

        none_nan_background_creator = self.start_background_thread()
        with ThreadPool(10) as pool:
            for _ in pool.imap_unordered(lambda sym: lib._nvs.read(sym), [f"sym{i}" for i in range(symbol_count)]):
                pass
            self.done_reading.set()
        none_nan_background_creator.join()
        clear_registered_normalizers()
