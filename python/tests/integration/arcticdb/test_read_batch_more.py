"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb import DataError, ErrorCode
from arcticdb.version_store.library import ReadRequest
from arcticdb.version_store.processing import QueryBuilder
import pytest
import numpy as np
import pandas as pd
import datetime as dt
import re

from typing import Any
from arcticdb.util.test import (assert_frame_equal, 
                                create_df_index_datetime, 
                                dataframe_simulate_arcticdb_update_static, 
                                get_sample_dataframe,
                                assert_frame_equal_row_range_fix,
                                dataframe_single_column_string,
                                dataframe_combine_all_dfs_into_one,
                                dataframe_filter_with_datetime_index
                                )

def dataframe_concat_sort(*df_args : pd.DataFrame) -> pd.DataFrame:
    """
        Concatenates and sorts row range indexed dataframes
    """
    result = pd.concat(df_args[0:2], copy=True)
    for idx in range(2,len(df_args),1):
        result = pd.concat([result, df_args[idx]],copy=True)
    result.sort_index(inplace=True) # We need to sort it at the end
    return result

def test_read_batch_2tables_7reads_different_slices(arctic_library):
    """
        Test aims to check if combined read of couple of DF, with several 
        reads from each, which filters different subsections of the timeframes
        is correct, in other words each read request is isolated from each other.
        Covers columns, as_of and date_range parameters of read_batch() function
    """
    lib = arctic_library

    symbol1 = "sym1"
    df1_0 = create_df_index_datetime(7, 0, 5)
    df1_1 = create_df_index_datetime(7, 4, 6)
    df1_2 = create_df_index_datetime(7, 6, 10)
    df1_3 = create_df_index_datetime(7, 0, 10)
    df1_till2 = dataframe_simulate_arcticdb_update_static(df1_0, df1_1)  # DF of state 0+1
    df1_till3 = dataframe_concat_sort(df1_till2, df1_2) # DF of state 0+1+2
    df1_all = dataframe_simulate_arcticdb_update_static(df1_till3, df1_3)

    symbol2 = "sym2"
    df2_0 = create_df_index_datetime(200, 0, 100)  
    df2_1 = create_df_index_datetime(200, 100, 200)
    df2_2 = create_df_index_datetime(200, 200, 300)
    df2_till2 = dataframe_concat_sort(df2_0, df2_1) # DF of state 0+1
    df2_all = dataframe_concat_sort(df2_till2, df2_2)
    # A DF with certain colums selected
    columns_to_select = ["COL_1", "COL_33", "COL_155"]
    df2_all_col_filtered = df2_all.loc[:,columns_to_select] 
    # Here we would like to produce a DF without several first and last rows
    start = df2_all.index[4]
    end = df2_all.index[-5]
    date_range = (start, end)
    df2_all_without_first_and_last = dataframe_filter_with_datetime_index(df2_all, start, end)
    # Here we would like to produce a DF without several first and last rows
    # and only two colums one of the first and one of the last
    columns_to_select1= ["COL_1", "COL_198"]
    start1 = df2_0.index[1]
    end1 = df2_0.index[-2]
    date_range1 = (start1, end1)
    tmp = df2_0.loc[:,columns_to_select1]
    df2_0_allfilters = dataframe_filter_with_datetime_index(tmp, start1, end1)

    symbol3 = "sym3" # non-existing

    lib.write(symbol1, df1_0)
    lib.update(symbol1, df1_1)
    lib.append(symbol1, df1_2)
    lib.update(symbol1, df1_3)

    lib.write(symbol2, df2_0)
    lib.append(symbol2, df2_1)
    lib.append(symbol2, df2_2)

    # Check Pandas update logic (simulating arctic append/update operations)
    assert_frame_equal(df1_all, df1_3)
    
    # Assure last version is exactly what we expect
    symbol1_data_sorted = lib.read(symbol1).data
    assert_frame_equal(df1_all, symbol1_data_sorted)
    assert_frame_equal(df1_3, symbol1_data_sorted)

    # Assure previous version is what we expect
    symbol1_data_sorted_ver_minus_one = lib.read(symbol1, as_of=1).data
    assert_frame_equal(df1_till2, symbol1_data_sorted_ver_minus_one)
    
    batch = lib.read_batch(symbols=[symbol3,
                                    symbol1, 
                                    ReadRequest(symbol1, as_of=2), 
                                    ReadRequest(symbol1, as_of=0),
                                    # daterange that should produce empty DF
                                    ReadRequest(symbol2, date_range=(dt.datetime(1990,1,1,0),dt.datetime(1999,1,1,0))), 
                                    ReadRequest(symbol2, columns=columns_to_select),
                                    ReadRequest(symbol2, date_range=date_range),
                                    ReadRequest(symbol2, date_range=date_range1, columns=columns_to_select1, as_of=0)
                                    ])
    
    
    assert [vi.symbol for vi in batch] == [symbol3, symbol1, symbol1, symbol1, symbol2, symbol2, symbol2, symbol2]
    assert isinstance(batch[0], DataError)
    assert batch[0].symbol == symbol3
    assert_frame_equal(df1_all, batch[1].data)
    assert_frame_equal(df1_till3, batch[2].data)
    assert_frame_equal(df1_0, batch[3].data)
    assert batch[4].data.empty
    # Only 3 colums in the DF, no date filtering
    assert_frame_equal(df2_all_col_filtered, batch[5].data)
    # Check if datetime filtering first several and last several is ok
    assert_frame_equal(df2_all_without_first_and_last, batch[6].data)
    # Column filters + datetime filters applied on the result
    assert_frame_equal(df2_0_allfilters, batch[7].data)

@pytest.mark.xfail(reason = "ArcticDB#1970")
def test_read_batch_query_with_and(arctic_library):
    """
        A very small test to isolate the problem with usage of "and" 
        in arctic queries. It produces wrong result, and should have 
        raised an error
    """

    lib = arctic_library

    symbol = "_s_"
    df = get_sample_dataframe(100)

    dfq = "bool == True and int8 > 5"
    q = QueryBuilder()
    q = q[q["bool"] and (q["int8"] > 5)]

    lib.write(symbol, df)

    batch = lib.read_batch(symbols=[ReadRequest(symbol, query_builder=q)])
    df_result = df.query(dfq, inplace=False)

    assert batch[0].symbol == symbol
    assert isinstance(batch[0], DataError)

def test_read_batch_metadata_on_different_version(arctic_library):
    """
        Here we test if read of metadata over several different states of DB with
        several differen read_batch() invokations works correctly.
        Thus we check isolation of the method over times
    """

    lib = arctic_library

    symbol = "_s_"
    df_0 = get_sample_dataframe(1)
    df_1 = get_sample_dataframe(2, seed=100)
    df_2 = get_sample_dataframe(3, seed=1345)
    df_3 = get_sample_dataframe(4, seed=1345)
    meta0 = {"meta0" : 0, "a" : "b", "c" : 1, 2 : 3}
    meta1 = {"meta1" : 1, "arr" : [1, 2, 4]}
    meta2 = {"meta2" : 2, 1 : {}, "arr2" : [1, 2, 4]}
    df_till1 = pd.concat([df_0, df_1])
    df_all = pd.concat([df_till1, df_2, df_3])

    lib.write(symbol, df_0, metadata=meta0)
    lib.append(symbol, df_1)
    lib.write_metadata(symbol, meta1)

    batch = lib.read_batch(symbols=[ReadRequest(symbol, as_of=2),
                                    ReadRequest(symbol, as_of=0),
                                    ReadRequest(symbol, as_of=1)])
    
    assert meta1 == lib.read_metadata(symbol).metadata
    assert meta0 == batch[1].metadata
    assert meta1 == batch[0].metadata
    assert None == batch[2].metadata

    lib.append(symbol, df_2)

    batch = lib.read_batch(symbols=[ReadRequest(symbol, as_of=2),
                                    ReadRequest(symbol, as_of=0),
                                    symbol,
                                    ReadRequest(symbol, as_of=1)])

    assert None == lib.read_metadata(symbol).metadata
    assert meta0 == batch[1].metadata
    assert meta1 == batch[0].metadata
    assert None == batch[2].metadata
    assert None == batch[3].metadata

    lib.append(symbol, df_3, meta2)

    batch = lib.read_batch(symbols=[ReadRequest(symbol, as_of=2),
                                    ReadRequest(symbol, as_of=0),
                                    symbol,
                                    ReadRequest(symbol, as_of=1)])

    assert meta2 == lib.read_metadata(symbol).metadata
    assert meta0 == batch[1].metadata
    assert meta1 == batch[0].metadata
    assert meta2 == batch[2].metadata
    assert None == batch[3].metadata
    assert_frame_equal_row_range_fix(df_0, batch[1].data)
    assert_frame_equal_row_range_fix(df_till1, batch[0].data)
    assert_frame_equal_row_range_fix(df_till1, batch[3].data)
    assert_frame_equal_row_range_fix(df_all, batch[2].data)

def test_read_batch_multiple_symbols_all_types_data_query_metadata(arctic_library):
    """
        This test aims to combine usage of metadata along with query builder applied in 
        read_batch() requests over time. Along with that we implicitly cover combinations
        of different query types - int, bool, float, string
    """
    
    lib = arctic_library
    
    def generate_mixed_dataframe(num_rows: int, seed=0):
        """
            Generation of a timeframe that is row ranged and has more string 
            columns to work with
        """
        result = dataframe_combine_all_dfs_into_one(get_sample_dataframe(num_rows), 
                                       dataframe_single_column_string(num_rows,"short",1,1), 
                                       dataframe_single_column_string(num_rows,"long",1,279))
        return result

    symbol1 = "s1"
    # Row ranged DF. This would not produce filter data with 
    # correct indexes
    df1_0 = generate_mixed_dataframe(10)
    df1_1 = generate_mixed_dataframe(20)
    df1_2 = generate_mixed_dataframe(66)
    df1_till1 = pd.concat([df1_0, df1_1],ignore_index=True)
    df1_till1.reset_index(inplace = True, drop = True)
    df1_all = pd.concat([df1_till1, df1_2],ignore_index=True)
    df1_all.reset_index(inplace = True, drop = True)
    metadata1 = {"version" : 1 , "data" : [1,3,5]}

    symbol2 = "s2"
    df2_0 = create_df_index_datetime(5, 0, 10)
    df2_1 = create_df_index_datetime(5, 10, 20)
    df2_all = pd.concat([df2_0, df2_1])
    df2_all_added = df2_all.copy(deep=True)
    df2_all_added["ADDED"] = df2_all_added["COL_1"] + df2_all_added["COL_2"] + 1
    metadata2 = {"Version" : 1.23 , "data" : {"a": 1, "b": 3,"c": 5}}
    metadata3 = {"final" : [1, 2]}

    lib.write(symbol1, df1_0, metadata=metadata1)
    lib.append(symbol1, df1_1)
    lib.append(symbol1, df1_2)

    lib.write(symbol2, df2_0)
    lib.write_metadata(symbol2, metadata2)
    lib.append(symbol2, df2_1, metadata3)

    # Simplest Boolean condition in query
    qdf1 = "bool == True"
    q1 = QueryBuilder()
    q1 = q1[q1["bool"]]
    # Boolean AND Integer condition in query
    qdf2 = "bool == True and int8 > 5" 
    q2 = QueryBuilder()
    q2 = q2[q2["bool"] & (q2["int8"] > 5)]
    qdf3 = "COL_1 > COL_2"
    # With Apply clause
    q3 = QueryBuilder()
    q3 = q3[q3["COL_1"] > q3["COL_2"]]
    q3.apply("ADDED", q3["COL_1"] + q3["COL_2"] + 1)
    # Text and float clause in query
    qdf4 = "short == 'K' and float64 > 12.5"
    q4 = QueryBuilder()
    q4 = q4[(q4["short"] == 'K') & (q4["float64"] > 12.5)]

    batch = lib.read_batch(symbols=[symbol1,
                                    ReadRequest(symbol1, as_of=0), 
                                    ReadRequest(symbol1, query_builder=q1, as_of=0), 
                                    symbol2,
                                    ReadRequest(symbol1, query_builder=q2),
                                    ReadRequest(symbol2, query_builder=q3),
                                    ReadRequest(symbol2, as_of=0),
                                    ReadRequest(symbol1, query_builder=q4)
                                    ])

    assert_frame_equal(df1_all, batch[0].data)
    assert None == batch[0].metadata #metadata is only per the version it was specified for
    assert_frame_equal(df1_0, batch[1].data)
    # Filter with boolean condition
    dfqapplied = df1_0.query(qdf1)
    assert_frame_equal_row_range_fix(dfqapplied, batch[2].data)
    assert_frame_equal(df2_all, batch[3].data)
    # Filter with boolean and integer condition
    dfqapplied = df1_all.query(qdf2)
    assert_frame_equal_row_range_fix(dfqapplied, batch[4].data)
    # filter with column between another column and apply clause
    dfqapplied = df2_all_added.query(qdf3)
    assert_frame_equal_row_range_fix(dfqapplied, batch[5].data)
    assert metadata3 == batch[5].metadata
    assert metadata3 == lib.read_metadata(symbol2).metadata
    # Filter fload and string condition
    dfqapplied = df1_all.query(qdf4)
    assert_frame_equal_row_range_fix(dfqapplied, batch[7].data)


def test_read_batch_multiple_wrong_things_at_once(arctic_library):
    """
        Check that many types of errors cannot prevent exraction of many other
        valid queries
    """
    lib = arctic_library

    symbol1 = "s1"
    df1_0 = get_sample_dataframe(100)
    df1_1 = get_sample_dataframe(100)
    q_wrong = QueryBuilder()
    q_wrong = q_wrong[q_wrong["bool"] & (q_wrong["int88"] > 5)]
    qdf = "bool == True"
    q = QueryBuilder()
    q = q[q["bool"]]

    symbol2 = "s2"
    df2_0 = create_df_index_datetime(7, 0, 5)
    df2_1 = create_df_index_datetime(7, 10, 50)
    df2_all= pd.concat([df2_0,df2_1])

    lib.write(symbol1, df1_0)
    lib.write(symbol1, df1_1)
    lib.write(symbol2, df2_0)
    lib.append(symbol2, df2_1)
    lib.delete(symbol1, versions=[1])

    batch = lib.read_batch(symbols=[symbol2,
                                ReadRequest(symbol1, as_of=1),
                                ReadRequest("nonExisting"),
                                ReadRequest(symbol1),
                                ReadRequest(symbol1, query_builder=q_wrong),
                                ReadRequest(symbol1, query_builder=q)
                                ])
    
    assert_frame_equal(df2_all, batch[0].data)
    assert isinstance(batch[1], DataError)
    assert batch[1].symbol == symbol1
    assert batch[1].error_code == ErrorCode.E_NO_SUCH_VERSION
    assert isinstance(batch[2], DataError)
    assert batch[2].symbol == "nonExisting"
    assert_frame_equal_row_range_fix(df1_0, batch[3].data)
    # No such column error
    assert isinstance(batch[4], DataError)
    # This query is ok and we expect results
    df = df1_0.query(qdf)
    assert_frame_equal_row_range_fix(df, batch[5].data)

