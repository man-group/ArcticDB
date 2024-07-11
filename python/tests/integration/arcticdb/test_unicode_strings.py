import os
from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np

def read_strings():
    script_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = "{}/blns.txt".format(script_directory)

    with open(file_path, 'r') as file:
        lines = file.readlines()

    filtered_lines = [line.strip() for line in lines if line.strip() and not line.strip().startswith('#')]
    return filtered_lines


def create_dataframe(strings):
    start_date = '2023-01-01'
    data = {
        'strings': strings,
        'ints': np.random.randint(1, 100, size=len(strings))
    }
    date_range = pd.date_range(start=start_date, periods=len(strings), freq='D')
    date_range.freq = None
    df = pd.DataFrame(data, index=date_range)
    return df


def test_write_blns(lmdb_version_store):
    strings = read_strings()
    symbol = "blns_write"
    df = create_dataframe(strings)
    lmdb_version_store.write(symbol, df)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(df, vit.data)


def test_append_blns(lmdb_version_store):
    strings = read_strings()
    symbol = "blns_append"
    df = create_dataframe(strings)
    half_index = len(df) // 2
    df_first_half = df.iloc[:half_index]
    df_second_half = df.iloc[half_index:]
    lmdb_version_store.write(symbol, df_first_half)
    lmdb_version_store.append(symbol, df_second_half)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(df, vit.data)


def test_update_blns(lmdb_version_store):
    strings = read_strings()
    symbol = "blns_update"
    df = create_dataframe(strings)
    total_length = len(df)
    half_length = total_length // 2
    start_index = (total_length - half_length) // 2
    end_index = start_index + half_length
    df_removed_middle = df.drop(df.index[start_index:end_index])
    df_middle_half = df.iloc[start_index:end_index]

    lmdb_version_store.write(symbol, df_removed_middle)
    lmdb_version_store.update(symbol, df_middle_half)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(df, vit.data)


def assert_dicts_of_dfs_equal(dict1, dict2):
    assert dict1.keys() == dict2.keys(), "Dictionary keys do not match"

    for key in dict1:
        pd.testing.assert_frame_equal(dict1[key], dict2[key], obj=f"DataFrame at key '{key}'")

def test_recursive_normalizers_blns(lmdb_version_store):
    lib = lmdb_version_store
    strings = read_strings()
    symbol = "blnd_recursive"
    df = create_dataframe(strings)
    keys = [
        "a",
        "b",
        "c",
        "d"
    ]
    dict = {s: df for s in keys}
    lib.write(symbol, dict, recursive_normalizers=True)
    vit = lib.read(symbol)
    assert_dicts_of_dfs_equal(dict, vit.data)