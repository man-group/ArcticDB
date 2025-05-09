import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import config_context

import pandas as pd

def verify_list_symbol_stats(list_symbol_call_counts):
    stats = qs.get_query_stats()
    # """
    # Sample output:
    # {
    #     "storage_operations": {
    #         "S3_DeleteObjects": {
    #             "count": 2,
    #             "size_bytes": 0,
    #             "total_time_ms": 33
    #         },
    #         "S3_GetObject": {
    #             "count": 3,
    #             "size_bytes": 517,
    #             "total_time_ms": 46
    #         },
    #         "S3_HeadObject": {
    #             "count": 1,
    #             "size_bytes": 0,
    #             "total_time_ms": 4
    #         },
    #         "S3_ListObjectsV2": {
    #             "count": 4,
    #             "size_bytes": 0,
    #             "total_time_ms": 68
    #         },
    #         "S3_PutObject": {
    #             "count": 2,
    #             "size_bytes": 413,
    #             "total_time_ms": 31
    #         }
    #     }
    # }
    # """    
    assert "storage_operations" in stats
    assert "S3_ListObjectsV2" in stats["storage_operations"]
    assert "count" in stats["storage_operations"]["S3_ListObjectsV2"]
    list_object_ststs = stats["storage_operations"]["S3_ListObjectsV2"]
    assert list_object_ststs["count"] == list_symbol_call_counts + 2
    assert list_object_ststs["total_time_ms"] > 1
    assert list_object_ststs["total_time_ms"] < 600


def test_query_stats(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    
    s3_version_store_v1.list_symbols()
    verify_list_symbol_stats(1)
    s3_version_store_v1.list_symbols()
    verify_list_symbol_stats(2)
    

def test_query_stats_context(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    with qs.query_stats():
        s3_version_store_v1.list_symbols()
    verify_list_symbol_stats(1)
    
    with qs.query_stats():
        s3_version_store_v1.list_symbols()
    verify_list_symbol_stats(2)


def test_query_stats_clear(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    s3_version_store_v1.list_symbols()
    qs.reset_stats()
    assert not qs.get_query_stats()

    s3_version_store_v1.list_symbols()
    verify_list_symbol_stats(1)


def test_query_stats_snapshot(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    s3_version_store_v1.snapshot("abc")
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.snapshot("abc2")
    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_DeleteObjects": {
    #             "count": 2,
    #             "size_bytes": 0,
    #             "total_time_ms": 32
    #         },
    #         "S3_GetObject": {
    #             "count": 5,
    #             "size_bytes": 1736,
    #             "total_time_ms": 80
    #         },
    #         "S3_HeadObject": {
    #             "count": 3,
    #             "size_bytes": 0,
    #             "total_time_ms": 15
    #         },
    #         "S3_ListObjectsV2": {
    #             "count": 6,
    #             "size_bytes": 0,
    #             "total_time_ms": 98
    #         },
    #         "S3_PutObject": {
    #             "count": 4,
    #             "size_bytes": 1581,
    #             "total_time_ms": 61
    #         }
    #     }
    # }
    
    
    assert "storage_operations" in stats
    storage_ops = stats["storage_operations"]
    assert "S3_ListObjectsV2" in storage_ops
    assert "total_time_ms" in storage_ops["S3_ListObjectsV2"]
    assert "count" in storage_ops["S3_ListObjectsV2"]
    assert storage_ops["S3_ListObjectsV2"]["count"] == 6
    
    
    assert "S3_PutObject" in storage_ops
    assert "S3_HeadObject" in storage_ops
    
    assert storage_ops["S3_PutObject"]["count"] == 4
    assert storage_ops["S3_HeadObject"]["count"] == 3


def test_query_stats_read_write(s3_version_store_v1, clear_query_stats):
    qs.enable()
    s3_version_store_v1.write("a", 1)
    s3_version_store_v1.write("a", 2)
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.read("a")
        s3_version_store_v1.read("a")
    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_GetObject": {
    #             "count": 10,
    #             "size_bytes": 3414,
    #             "total_time_ms": 167
    #         },
    #         "S3_PutObject": {
    #             "count": 10,
    #             "size_bytes": 4905,
    #             "total_time_ms": 157
    #         }
    #     }
    # }
    
    assert "storage_operations" in stats
    storage_operations = stats["storage_operations"]
    assert "S3_GetObject" in storage_operations
    assert storage_operations["S3_GetObject"]["count"] == 10
    assert storage_operations["S3_GetObject"]["total_time_ms"] > 0
    assert "size_bytes" in storage_operations["S3_GetObject"]

    assert "S3_PutObject" in storage_operations
    assert storage_operations["S3_PutObject"]["count"] == 10
    assert storage_operations["S3_PutObject"]["total_time_ms"] > 0
    assert storage_operations["S3_PutObject"]["size_bytes"] > 50
        

def test_query_stats_metadata(s3_version_store_v1, clear_query_stats):
    qs.enable()
    meta1 = {"meta1" : 1, "arr" : [1, 2, 4]}
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.write_metadata("a", meta1)
        s3_version_store_v1.write_metadata("a", meta1)
        s3_version_store_v1.read_metadata("a")
        s3_version_store_v1.read_metadata("a")
    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_GetObject": {
    #             "count": 14,
    #             "size_bytes": 6078,
    #             "total_time_ms": 231
    #         },
    #         "S3_PutObject": {
    #             "count": 9,
    #             "size_bytes": 4845,
    #             "total_time_ms": 142
    #         }
    #     }
    # }
    assert "storage_operations" in stats
    storage_operations = stats["storage_operations"]
    
    if "S3_GetObject" in storage_operations:
        assert storage_operations["S3_GetObject"]["count"] > 0
        assert storage_operations["S3_GetObject"]["size_bytes"] > 0
        assert storage_operations["S3_GetObject"]["total_time_ms"] > 0
        
    if "S3_PutObject" in storage_operations:
        assert storage_operations["S3_PutObject"]["count"] > 0
        assert storage_operations["S3_PutObject"]["size_bytes"] > 0
        assert storage_operations["S3_PutObject"]["total_time_ms"] > 0


def test_query_stats_batch(s3_version_store_v1, clear_query_stats):
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    qs.enable()
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.batch_write([sym1, sym2], [df0, df0])
        s3_version_store_v1.batch_read([sym1, sym2])
        s3_version_store_v1.batch_write([sym1, sym2], [df1, df1])
        s3_version_store_v1.batch_read([sym1, sym2])

    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_GetObject": {
    #             "count": 26,
    #             "size_bytes": 11708,
    #             "total_time_ms": 436
    #         },
    #         "S3_PutObject": {
    #             "count": 20,
    #             "size_bytes": 11103,
    #             "total_time_ms": 313
    #         }
    #     }
    # }
    assert "storage_operations" in stats
    storage_operations = stats["storage_operations"]
    
    assert "S3_GetObject" in storage_operations
    assert storage_operations["S3_GetObject"]["count"] > 0
    assert storage_operations["S3_GetObject"]["size_bytes"] > 0
    assert storage_operations["S3_GetObject"]["total_time_ms"] > 0
        
    assert "S3_PutObject" in storage_operations
    assert storage_operations["S3_PutObject"]["count"] > 0
    assert storage_operations["S3_PutObject"]["size_bytes"] > 0
    assert storage_operations["S3_PutObject"]["total_time_ms"] > 0
