from arcticdb.arctic import Arctic
from arcticdb.util.test import assert_frame_equal
from arcticdb.toolbox.query_stats import QueryStatsTools

import pandas as pd


fake_data = pd.DataFrame([
    {"arcticdb_call": "list_symbols", "stage": None, "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": 1, "time_count_50": None, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    {"arcticdb_call": "list_symbols", "stage": "list", "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": 1, "time_count_50": None, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    {"arcticdb_call": "list_symbols", "stage": "list", "key_type": "sl", "storage_op": "ListObjectsV2", "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": 1, "time_count_50": None, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    {"arcticdb_call": "read", "stage": None, "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": None, "time_count_50": None, "time_count_80": None, "time_count_100": 1, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    {"arcticdb_call": "read", "stage": "find_version", "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": None, "time_count_50": 1, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    {"arcticdb_call": "read", "stage": "find_version", "key_type": "vref", "storage_op": "GetObject", "parallelized": False, "count": 1, "ver": 1, "tindex": 1, "tdata": None, "tall": None, "uncompressed_size": 15, "compressed_size": None, "bandwidth": 260, "time_count_30": None, "time_count_50": 1, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    {"arcticdb_call": "read", "stage": "read", "key_type": None, "storage_op": None, "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": None, "compressed_size": None, "bandwidth": None, "time_count_30": None, "time_count_50": None, "time_count_80": 1, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    {"arcticdb_call": "read", "stage": "read", "key_type": "tdata", "storage_op": "GetObject", "parallelized": True, "count": 1, "ver": None, "tindex": None, "tdata": None, "tall": None, "uncompressed_size": 2000, "compressed_size": 200, "bandwidth": 5714, "time_count_30": 1, "time_count_50": None, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None},
    {"arcticdb_call": "read", "stage": "read", "key_type": "tindex", "storage_op": "GetObject", "parallelized": False, "count": 1, "ver": None, "tindex": None, "tdata": 1, "tall": None, "uncompressed_size": 16, "compressed_size": None, "bandwidth": 301, "time_count_30": None, "time_count_50": 1, "time_count_80": None, "time_count_100": None, "time_count_130": None, "time_count_300": None, "time_count_350": None, "time_count_780": None}
])

def test_query_stats(s3_storage):
    ac = Arctic(s3_storage.arctic_uri)
    lib = ac.create_library("test")
    query_stats_tools_start = QueryStatsTools()
    # some write blah blah blah
    query_stats_tools_end = QueryStatsTools()
    assert_frame_equal(query_stats_tools_end - query_stats_tools_start, fake_data)
    QueryStatsTools.reset_stats()

    # This is not implemented in the demo code
    # assert (query_stats_tools_start - query_stats_tools_end).empty


def test_query_stats_context(s3_storage):
    ac = Arctic(s3_storage.arctic_uri)
    lib = ac.create_library("test")
    with QueryStatsTools.context_manager() as query_stats_tools:
        # some write blah blah blah
        pass
    assert_frame_equal(query_stats_tools.get_query_stats(), fake_data)