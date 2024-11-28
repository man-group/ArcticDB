"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import numpy as np
import pytest
from arcticdb.util.test import assert_frame_equal, get_sample_dataframe
import arcticdb as adb


def arctic_chunk(df, lib, symbol, read_args, chunk_args):
    lib.write(symbol, df)
    chunks = [c for c in lib.iter_read_chunks(symbol, **read_args, **chunk_args)]
    if 'lazy' in read_args and read_args['lazy']:
        chunks = [c.collect().data for c in chunks]
    return chunks

def arange_include_stop(start, stop, step):
    return np.concatenate([np.arange(start, stop, step), [stop]])

def dataframe_chunks(df, row_chunks):
    df_chunks = []
    for i in range(len(row_chunks) - 1):
        df_chunks.append(df.iloc[row_chunks[i]: row_chunks[i+1]])
    return df_chunks

def calc_row_chunks(lib, df, chunk_args):
    step = None
    if not chunk_args:
        step = lib.options().rows_per_segment
    elif 'rows_per_chunk' in chunk_args:
        step = chunk_args['rows_per_chunk']

    if step is not None:
        return arange_include_stop(0, len(df), step)

    row_count = len(df)
    num_chunks = chunk_args['num_chunks']
    base_rows = row_count // num_chunks
    extra_rows = row_count % num_chunks
    start_row = 0
    row_chunks = [start_row]
    for i in range(num_chunks):
        end_row = min(start_row + base_rows + (1 if i<extra_rows else 0), row_count)
        row_chunks.append(end_row)
        start_row = end_row
    return row_chunks

def pandas_chunk(df, lib, symbol, read_args, chunk_args, row_chunks_override=None, ignore_db=False):
    if ignore_db:
        df_db = df
    else:
        lib.write(symbol, df)
        if 'lazy' in read_args and read_args['lazy']:
            df_db = lib.read(symbol, **read_args).collect().data
        else:
            df_db = lib.read(symbol, **read_args).data
    row_chunks = calc_row_chunks(lib, df_db, chunk_args) if row_chunks_override is None else row_chunks_override
    return dataframe_chunks(df_db, row_chunks)

def assert_chunks_match(c_test, c_check):
    assert len(c_test) == len(c_check), f"Number of chunks does not match: test={len(c_test)}, check={len(c_check)}"
    for i in range(len(c_test)):
        assert_frame_equal(c_test[i], c_check[i])

def get_sample_dataframe_timeseries(size=1000, start_date='20200101'):
    df = get_sample_dataframe(size=size)
    df['timestamp'] = pd.date_range(start=start_date, periods=size)
    return df.set_index('timestamp')

DF_SMALL = get_sample_dataframe_timeseries(17)
DF_SMALL2 = get_sample_dataframe_timeseries(21)
CHUNK_ARGS_ALL = [dict(), dict(num_chunks=4), dict(rows_per_chunk=3)]

def create_lib(ac):
    return ac.get_library('chunk_tests', create_if_missing=True)

def create_lib_tiny_segments(ac):
    lib_opts = adb.LibraryOptions(rows_per_segment=2, columns_per_segment=2)
    return ac.get_library('chunk_tests_tiny', create_if_missing=True, library_options=lib_opts)

def create_lib_tiny_segments_dynamic(ac):
    lib_opts = adb.LibraryOptions(rows_per_segment=2, columns_per_segment=2, dynamic_schema=True)
    return ac.get_library('chunk_tests_tiny_dynamic', create_if_missing=True, library_options=lib_opts)

def generic_chunk_test(lib, symbol, df, read_args, chunk_args, df_test=None, pd_ignore_db=False):
    chunks_db = arctic_chunk(df, lib, symbol, read_args, chunk_args)
    df_pd = df if df_test is None else df_test
    chunks_pd = pandas_chunk(df_pd, lib, symbol, read_args, chunk_args, ignore_db=pd_ignore_db)
    assert_chunks_match(chunks_db, chunks_pd)

def test_chunk_simple(arctic_client):
    lib = create_lib(arctic_client)
    sym = 'simple'
    for chunk_args in CHUNK_ARGS_ALL:
        generic_chunk_test(lib, sym, DF_SMALL, dict(), chunk_args)

def test_chunk_simple_tiny(arctic_client):
    lib = create_lib_tiny_segments(arctic_client)
    sym = 'simple'
    for chunk_args in CHUNK_ARGS_ALL:
        generic_chunk_test(lib, sym, DF_SMALL, dict(), chunk_args)

def test_chunk_simple_tiny_lazy(arctic_client):
    lib = create_lib_tiny_segments(arctic_client)
    sym = 'simple'
    for chunk_args in CHUNK_ARGS_ALL:
        generic_chunk_test(lib, sym, DF_SMALL, dict(lazy=True), chunk_args)

def test_chunk_simple_tiny_dynamic(arctic_client):
    lib = create_lib_tiny_segments_dynamic(arctic_client)
    sym = 'simple'
    for chunk_args in CHUNK_ARGS_ALL:
        generic_chunk_test(lib, sym, DF_SMALL, dict(), chunk_args)

def test_chunk_as_of_tiny(arctic_client):
    lib = create_lib_tiny_segments(arctic_client)
    sym = 'as_of'
    write_v = lib.write(sym, DF_SMALL)
    for chunk_args in CHUNK_ARGS_ALL:
        generic_chunk_test(lib, sym, DF_SMALL2, dict(as_of=write_v.version), chunk_args, DF_SMALL)

def test_chunk_date_range_tiny(arctic_client):
    lib = create_lib_tiny_segments(arctic_client)
    sym = 'date_range'
    dr = (pd.Timestamp('20200102'), pd.Timestamp('20200103'))
    for chunk_args in CHUNK_ARGS_ALL:
        with pytest.raises(ValueError):
            generic_chunk_test(lib, sym, DF_SMALL, dict(date_range=dr), chunk_args)

def test_chunk_row_range_tiny(arctic_client):
    lib = create_lib_tiny_segments(arctic_client)
    sym = 'row_range'
    for chunk_args in CHUNK_ARGS_ALL:
        generic_chunk_test(lib, sym, DF_SMALL, dict(row_range=(0,-1)), chunk_args)

def test_chunk_columns_tiny(arctic_client):
    lib = create_lib_tiny_segments(arctic_client)
    sym = 'columns'
    cols = DF_SMALL.columns[[1, 7, 11]]
    for chunk_args in CHUNK_ARGS_ALL:
        #generic_chunk_test(lib, sym, DF_SMALL, dict(columns=cols), chunk_args, DF_SMALL[cols])
        generic_chunk_test(lib, sym, DF_SMALL, dict(columns=cols), chunk_args)

def test_chunk_querybuilder_tiny(arctic_client):
    # behaviour of query_builder with/without chunks can be complicated, because the query is run against each chunk
    # so this test is limited to a simple case
    lib = create_lib_tiny_segments(arctic_client)
    sym = 'query_builder'
    q = adb.QueryBuilder()[adb.col('timestamp') > DF_SMALL.index[0]]
    df_test = DF_SMALL[DF_SMALL.index > DF_SMALL.index[0]]
    generic_chunk_test(lib, sym, DF_SMALL, dict(query_builder=q), dict(num_chunks=2), df_test, pd_ignore_db=True)
