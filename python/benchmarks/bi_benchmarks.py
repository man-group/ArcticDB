"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import os
from pathlib import Path
from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder

from .common import *


def get_query_groupby_city_count_all(q):
    return q.groupby("City").agg({"Keyword": "count"})


def get_query_groupby_city_count_isin_filter(q):
    return q[q["Keyword"].isin(["bimbo", "twat", "faggot"])].groupby("City").agg({"Keyword": "count"})


def get_query_groupby_city_count_filter_two_aggregations(q):
    return q[q["Keyword"] == "faggot" ].groupby("City").agg({"Keyword": "count", "Number of Records" : "sum"})  

def assert_frame_equal(pandas_df:pd.DataFrame, arctic_df:pd.DataFrame):
    arctic_df.sort_index(inplace=True)
    pd.testing.assert_frame_equal(pandas_df,
                                    arctic_df, 
                                    check_column_type=False, 
                                    check_dtype=False)


class BIBenchmarks:
    '''
        Sample test benchmark for using one opensource BI CSV source.
        The logic of a test is 
            - download if parquet file does not exists source in .bz2 format
            - convert it to parquet format
            - prepare library with it containing  several symbols that are constructed based on this DF
            - for each query we want to benchmark do a pre-check that this query produces SAME result on Pandas and arcticDB
            - run the benchmark tests
    '''


    number = 2
    timeout = 6000
    LIB_NAME = "BI_benchmark_lib"
    # We use dataframe in this file
    CITY_BI_FILE = "data/CityMaxCapita_1.csv.bz2"
    CITY_BI_FILE2 = "data/CityMaxCapita_1.parquet.gzip"

    #Defines how many times bigger the database is
    params = [1, 10]

    def __init__(self):
        self.lib_name = BIBenchmarks.LIB_NAME
        self.symbol = self.lib_name

    def setup_cache(self):

        file = os.path.join(Path(__file__).resolve().parent.parent, BIBenchmarks.CITY_BI_FILE2)
        if (not os.path.exists(file)) :
            dfo = download_and_process_city_to_parquet(file)
            dff = pd.read_parquet(file)
            pd.testing.assert_frame_equal(dfo,dff)
        else:
            print("Parquet file exists!")

        # read data from bz.2 file
        # abs_path = os.path.join(Path(__file__).resolve().parent.parent,BIBenchmarks.CITY_BI_FILE)
        # self.df : pd.DataFrame = process_city(abs_path)

        self.df : pd.DataFrame = pd.read_parquet(file)

        self.ac = Arctic(f"lmdb://opensource_datasets_{self.lib_name}?map_size=20GB")
        self.ac.delete_library(self.lib_name)
        self.lib = self.ac.create_library(self.lib_name)

        print("The procedure is creating N times larger dataframes")
        print("by concatenating original DF N times")
        for num in BIBenchmarks.params:
            _df = self.df.copy(deep=True)
            if (num > 1):
                # lets create N times bigger DF
                dfcum = self.df.copy(deep=True)
                for i in range(1, (BIBenchmarks.params[-1])):
                    dfcum = pd.concat([dfcum, self.df])            
                _df = dfcum
                print("DF for iterration xSize original ready: ", num)
                _df.info(verbose=True,memory_usage='deep')
            self.lib.write(f"{self.symbol}{num}", _df)

        print("If pandas query produces different dataframe than arctic one stop tests!")
        print("This will mean query problem is there most likely")

        print("Pre-check correctness for query_groupby_city_count_all")
        _df = self.df.copy(deep=True)
        arctic_df = self.time_query_groupby_city_count_all(BIBenchmarks.params[0])
        _df = get_query_groupby_city_count_all(_df)
        assert_frame_equal(_df, arctic_df)

        print("Pre-check correctness for query_groupby_city_count_isin_filter")
        _df = self.df.copy(deep=True)
        arctic_df = self.time_query_groupby_city_count_isin_filter(BIBenchmarks.params[0])
        _df = get_query_groupby_city_count_isin_filter(_df)
        assert_frame_equal(_df, arctic_df)

        print("Pre-check correctness for query_groupby_city_count_filter_two_aggregations")
        _df = self.df.copy(deep=True)
        arctic_df = self.time_query_groupby_city_count_filter_two_aggregations(BIBenchmarks.params[0])
        _df = get_query_groupby_city_count_filter_two_aggregations(_df)
        assert_frame_equal(_df, arctic_df)

        print("All pre-checks completed SUCCESSFULLY")

        del self.ac
    
    def setup(self, num_rows):
        self.ac = Arctic(f"lmdb://opensource_datasets_{self.lib_name}?map_size=20GB")
        self.lib = self.ac.get_library(self.lib_name)

    def teardown(self, num_rows):
        del self.ac

    def time_query_readall(self, times_bigger):
        self.lib.read(f"{self.symbol}{times_bigger}")
    
    def peakmem_query_readall(self, times_bigger):
        self.lib.read(f"{self.symbol}{times_bigger}")

    def query_groupby_city_count_all(self, times_bigger) -> pd.DataFrame:
        q = QueryBuilder()
        q = get_query_groupby_city_count_all( q)
        df = self.lib.read(f"{self.symbol}{times_bigger}", query_builder=q)
        return df.data

    def time_query_groupby_city_count_all(self, times_bigger) -> pd.DataFrame:
        return self.query_groupby_city_count_all(times_bigger)

    def peakmem_query_groupby_city_count_all(self, times_bigger) -> pd.DataFrame:
        return self.query_groupby_city_count_all(times_bigger)
    
    def query_groupby_city_count_isin_filter(self, times_bigger) -> pd.DataFrame:
        q = QueryBuilder()
        q = get_query_groupby_city_count_isin_filter(q)   
        df = self.lib.read(f"{self.symbol}{times_bigger}", query_builder=q)
        return df.data

    def time_query_groupby_city_count_isin_filter(self, times_bigger) -> pd.DataFrame:
        return self.query_groupby_city_count_isin_filter(times_bigger)

    def peakmem_query_groupby_city_count_isin_filter(self, times_bigger) -> pd.DataFrame:
        return self.query_groupby_city_count_isin_filter(times_bigger)

    def query_groupby_city_count_filter_two_aggregations(self, times_bigger) -> pd.DataFrame:
        q = QueryBuilder()
        q = get_query_groupby_city_count_filter_two_aggregations(q) 
        df = self.lib.read(f"{self.symbol}{times_bigger}", query_builder=q)
        return df.data

    def time_query_groupby_city_count_filter_two_aggregations(self, times_bigger) -> pd.DataFrame:
        return self.query_groupby_city_count_filter_two_aggregations(times_bigger)

    def peakmem_query_groupby_city_count_filter_two_aggregations(self, times_bigger):
        return self.query_groupby_city_count_filter_two_aggregations(times_bigger)
        
