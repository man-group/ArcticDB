from arcticdb.util.test import random_strings_of_length, random_string
import datetime
import pandas as pd
from datetime import datetime
from pandas.testing import assert_frame_equal


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


