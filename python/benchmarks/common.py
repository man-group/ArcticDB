"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import urllib.parse
import pandas as pd
import numpy as np

import os
import bz2
import urllib.request

## You can now define SLOW ASV tests
## To run those tests also you have to set following OS variable
## Then you can use this variable along with asv_runner.benchmarks.mark.SkipNotImplemented exception
## (see finalized_staged_data.py)
SLOW_TESTS = os.getenv("ARCTICDB_SLOW_TESTS") == "1"

def generate_pseudo_random_dataframe(n, freq="s", end_timestamp="1/1/2023"):
    """
    Generates a Data Frame with 2 columns (timestamp and value) and N rows
    - timestamp contains timestamps with a given frequency that end at end_timestamp
    - value contains random floats that sum up to approximately N, for easier testing/verifying
    """
    # Generate random values such that their sum is equal to N
    values = np.random.uniform(0, 2, size=n)
    # Generate timestamps
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq=freq)
    # Create dataframe
    df = pd.DataFrame({"value": values})
    df.index = timestamps
    return df


def generate_random_floats_dataframe(num_rows, num_cols):
    """
    Generates a dataframe of random 64 bit floats with num_rows rows and num_cols columns.
    Columns are named "col_n" for n in range(num_cols).
    Row range indexed.
    """
    columns = [f"col_{n}" for n in range(num_cols)]
    rng = np.random.default_rng()
    data = rng.random((num_rows, num_cols), dtype=np.float64)
    return pd.DataFrame(data, columns=columns)


def generate_random_floats_dataframe_with_index(num_rows, num_cols, freq="s", end_timestamp="1/1/2023"):
    timestamps = pd.date_range(end=end_timestamp, periods=num_rows, freq=freq)
    df = generate_random_floats_dataframe(num_rows, num_cols)
    df.index = timestamps
    return df


def generate_benchmark_df(n, freq="min", end_timestamp="1/1/2023"):
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq=freq)
    k = n // 10
    # Based on https://github.com/duckdblabs/db-benchmark/blob/master/_data/groupby-datagen.R#L19
    dt = pd.DataFrame()
    dt["id1"] = np.random.choice([f"id{str(i).zfill(3)}" for i in range(1, k + 1)], n)
    dt["id2"] = np.random.choice([f"id{str(i).zfill(3)}" for i in range(1, k + 1)], n)
    dt["id3"] = np.random.choice([f"id{str(i).zfill(10)}" for i in range(1, n // k + 1)], n)
    dt["id4"] = np.random.choice(range(1, k + 1), n)
    dt["id5"] = np.random.choice(range(1, k + 1), n)
    dt["id6"] = np.random.choice(range(1, n // k + 1), n)
    dt["v1"] = np.random.choice(range(1, 6), n)
    dt["v2"] = np.random.choice(range(1, 16), n)
    dt["v3"] = np.round(np.random.uniform(0, 100, n), 6)

    assert len(timestamps) == len(dt)

    dt.index = timestamps

    return dt


def get_prewritten_lib_name(rows):
    return f"prewritten_{rows}"
    
    
def get_filename_from_url(url):
    parsed_url = urllib.parse.urlparse(url)
    return os.path.basename(parsed_url.path)


def download_file(url: str) -> str:
    """
        Downloads file from specific location and then saves
        it under same name at current directory. 
        Returns the name of file just saved
    """
    print("Downloading file from: ", url)
    name = get_filename_from_url(url)
    urllib.request.urlretrieve(url, name)
    print("File downloaded: ", name)
    return name

def download_and_process_city_to_parquet(save_to_file:str) -> pd.DataFrame :
    '''
        Downloads CSV from a location then saves it in gziped parqet
    '''
    name = download_file("http://www.cwi.nl/~boncz/PublicBIbenchmark/CityMaxCapita/CityMaxCapita_1.csv.bz2")
    name = decompress_bz2_file(name)
    df : pd.DataFrame = read_city(name)
    location = os.path.join(save_to_file)
    directory = os.path.dirname(location)
    if not os.path.exists(directory):
        os.makedirs(directory)
    print("Saving dataframe to gzip/parquet file: " ,location)
    df.to_parquet(location,
                compression='gzip',
                index=True)
    return df

def decompress_bz2_file(name: str) -> str:
    """
        Decompresses a bz2 file and saves content in
        a text file having same name (without bz.2 extensions)
        in current directory.
        Returns the name of the saved file
    """
    print("Decompressing file: ", name)
    nn = name.replace(".bz2", "")
    new_name = os.path.basename(nn)

    with bz2.open(name, 'rb') as input_file:
        decompressed_data = input_file.read()

    with open(new_name, 'wb') as output_file:
        output_file.write(decompressed_data)

    print("Decompressed file: ", new_name)

    return new_name

def read_city(file1:str):
    """
        Data source:
            https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/CityMaxCapita/queries/11.sql

        As CSV file contains nulls in int and float we fix those programatically
    """
    columns =[
        "City/Admin",
        "City/State",
        "City",
        "Created Date/Time",
        "Date Joined",
        "FF Ratio",
        "Favorites",
        "First Link in Tweet",
        "Followers",
        "Following",
        "Gender",
        "Influencer?",
        "Keyword",
        "LPF",
        "Language",
        "Lat",
        "Listed Number",
        "Long Domain",
        "Long",
        "Number of Records",
        "Region",
        "Short Domain",
        "State/Country",
        "State",
        "Tweet Text",
        "Tweets",
        "Twitter Client",
        "User Bio",
        "User Loc",
        "Username 1",
        "Username" 
    ]
    types = {
        "City/Admin" : str,
        "City/State" : str,
        "City" : str,
        "Created Date/Time" : np.float64,
        "Date Joined" : np.float64,
        "FF Ratio" : np.float64,
        "Favorites" : np.int32,
        "First Link in Tweet" : str,
        "Followers" : np.int32,
        "Following" : np.int32,
        "Gender" : str,
        "Influencer?" : pd.Int32Dtype(),
        "Keyword" : str,
        "LPF" : np.float64,
        "Language" : str,
        "Lat" : np.float64,
        "Listed Number" : pd.Int32Dtype(),
        "Long Domain" : str,
        "Long" : np.float64,
        "Number of Records" : np.int32,
        "Region" : str,
        "Short Domain" : str,
        "State/Country" : str,
        "State" : str,
        "Tweet Text" : str,
        "Tweets" : np.int32,
        "Twitter Client" : str,
        "User Bio" : str,
        "User Loc" : str,
        "Username 1" : str,
        "Username" : str   
    }

    df = pd.read_csv(file1, sep="|", 
                        header=None,
                        dtype=types, 
                        names=columns,
                        )
    
    df["Influencer?"]=df["Influencer?"].fillna(0).astype(np.int32)
    df["Listed Number"]=df["Listed Number"].fillna(0).astype(np.int32)
        
    return df

def process_city(fileloc:str) -> pd.DataFrame :
    # read data from bz.2 file
    name = decompress_bz2_file(fileloc)
    df : pd.DataFrame = read_city(name)
    return df
