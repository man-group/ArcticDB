from arcticdb.arctic import Arctic
from arcticdb.util.test import assert_frame_equal

import pandas as pd

def test_query_stats(s3_storage):
    ac = Arctic(s3_storage.arctic_uri)
    lib = ac.create_library("test")
    admin_tools = lib.get_admin_tools()
    query_stats_tools_start = admin_tools.get_query_stats()
    # some write blah blah blah
    query_stats_tools_end = admin_tools.get_query_stats()
    data = {
        'arcticdb_call': ['read', 'write'],
        'stage': ['list', 'write'],
        'key_type': ['ref', 'd'],
        'library': ['a', 'a1'],
        'storage_op': ['list', 'write'],
        'count': [1, 5],
        'max_time': [1, 10],
        'min_time': [1, 20],
        'avg_time': [1, 15],
        'uncompressed_size': [10, 1000],
        'compressed_size': [10, 20],
        'retries': [0, 0]
    }
    assert_frame_equal(query_stats_tools_end - query_stats_tools_start, pd.DataFrame(data))
    admin_tools.reset_query_stats()
    assert (query_stats_tools_start - query_stats_tools_end).empty