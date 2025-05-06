import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import config_context

import pandas as pd

def verify_list_symbol_stats(list_symbol_call_counts):
    stats = qs.get_query_stats()
    # """
    # Sample output:
    # {
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "total_time_ms": 50,
    #                 "count": 3
    #             },
    #             "S3_PutObject": {
    #                 "total_time_ms": 15,
    #                 "count": 1
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 15,
    #                 "count": 1
    #             },
    #             "S3_DeleteObjects": {
    #                 "total_time_ms": 17,
    #                 "count": 1
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "total_time_ms": 15,
    #                 "count": 1
    #             }
    #         }
    #     },
    #     "LOCK": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 15,
    #                 "count": 1
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 31,
    #                 "count": 2
    #             },
    #             "S3_DeleteObjects": {
    #                 "total_time_ms": 15,
    #                 "count": 1
    #             },
    #             "S3_HeadObject": {
    #                 "total_time_ms": 4,
    #                 "count": 1
    #             }
    #         }
    #     }
    # }
    # """    
    assert "SYMBOL_LIST" in stats
    keys_to_check = {"SYMBOL_LIST", "VERSION_REF"}
    for key, key_type_map in stats.items():
        if key in keys_to_check:
            assert "storage_ops" in key_type_map
            assert "S3_ListObjectsV2" in key_type_map["storage_ops"]
            assert "count" in key_type_map["storage_ops"]["S3_ListObjectsV2"]
            list_object_ststs = key_type_map["storage_ops"]["S3_ListObjectsV2"]
            assert list_object_ststs["count"] == 1 if key == "VERSION_REF" else list_symbol_call_counts
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
    #     "SNAPSHOT": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "total_time_ms": 39,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "total_time_ms": 33,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "total_time_ms": 32,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 50,
    #                 "count": 3
    #             }
    #         }
    #     },
    #     "LOCK": {
    #         "storage_ops": {
    #             "S3_HeadObject": {
    #                 "total_time_ms": 13,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "SNAPSHOT_REF": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 34,
    #                 "count": 2
    #             },
    #             "S3_HeadObject": {
    #                 "total_time_ms": 13,
    #                 "count": 2
    #             }
    #         }
    #     }
    # }
    assert {"SNAPSHOT", "SNAPSHOT_REF", "SYMBOL_LIST", "LOCK", "VERSION_REF"} == stats.keys()
    
    assert "storage_ops" in stats["SNAPSHOT"]
    assert "S3_ListObjectsV2" in stats["SNAPSHOT"]["storage_ops"]
    assert "total_time_ms" in stats["SNAPSHOT"]["storage_ops"]["S3_ListObjectsV2"]
    assert "count" in stats["SNAPSHOT"]["storage_ops"]["S3_ListObjectsV2"]
    assert stats["SNAPSHOT"]["storage_ops"]["S3_ListObjectsV2"]["count"] == 2
    
    assert "storage_ops" in stats["SNAPSHOT_REF"]
    snapshot_ref_ops = stats["SNAPSHOT_REF"]["storage_ops"]
    
    assert "S3_PutObject" in snapshot_ref_ops
    assert "S3_HeadObject" in snapshot_ref_ops
    
    assert snapshot_ref_ops["S3_PutObject"]["count"] == 2
    assert snapshot_ref_ops["S3_HeadObject"]["count"] == 2


def test_query_stats_read_write(s3_version_store_v1, clear_query_stats):
    qs.enable()
    s3_version_store_v1.write("a", 1)
    s3_version_store_v1.write("a", 2)
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.read("a")
        s3_version_store_v1.read("a")
    stats = qs.get_query_stats()
    # {
    #     "TABLE_DATA": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 32,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 30,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "TABLE_INDEX": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 33,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 30,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "VERSION": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 30,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 34,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 32,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 31,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 66,
    #                 "count": 4
    #             }
    #         }
    #     }
    # }
    
    expected_keys = ["TABLE_DATA", "TABLE_INDEX", "VERSION", "SYMBOL_LIST", "VERSION_REF"]
    
    for key, stat in stats.items():
        assert key in expected_keys
        assert "storage_ops" in stats[key]
        storage_ops = stat["storage_ops"]
        

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
    #     "TABLE_DATA": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 17,
    #                 "count": 1
    #             }
    #         }
    #     },
    #     "TABLE_INDEX": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 32,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 47,
    #                 "count": 3
    #             }
    #         }
    #     },
    #     "VERSION": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 31,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 66,
    #                 "count": 4
    #             }
    #         }
    #     },
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 30,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 31,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 112,
    #                 "count": 7
    #             }
    #         }
    #     }
    # }
    
    expected_keys = ["TABLE_INDEX", "VERSION", "VERSION_REF"]
    
    for key in expected_keys:
        assert key in stats
        assert "storage_ops" in stats[key]
        storage_ops = stats[key]["storage_ops"]


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
    #     "TABLE_DATA": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 66,
    #                 "count": 4
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 61,
    #                 "count": 4
    #             }
    #         }
    #     },
    #     "TABLE_INDEX": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 64,
    #                 "count": 4
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 63,
    #                 "count": 4
    #             }
    #         }
    #     },
    #     "VERSION": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 63,
    #                 "count": 4
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 97,
    #                 "count": 6
    #             }
    #         }
    #     },
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 63,
    #                 "count": 4
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 63,
    #                 "count": 4
    #             },
    #             "S3_GetObject": {
    #                 "total_time_ms": 218,
    #                 "count": 12
    #             }
    #         }
    #     }
    # }
    
    expected_keys = ["TABLE_DATA", "TABLE_INDEX", "VERSION", "SYMBOL_LIST", "VERSION_REF"]    
    for key in expected_keys:
        assert key in stats
        assert "storage_ops" in stats[key]
        storage_ops = stats[key]["storage_ops"]
