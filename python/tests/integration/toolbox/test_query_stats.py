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
    #     "LOCK": {
    #         "storage_ops": {
    #             "S3_DeleteObjects": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 70
    #             },
    #             "S3_GetObject": {
    #                 "count": 2,
    #                 "size_bytes": 208,
    #                 "total_time_ms": 115
    #             },
    #             "S3_HeadObject": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 53
    #             },
    #             "S3_PutObject": {
    #                 "count": 1,
    #                 "size_bytes": 104,
    #                 "total_time_ms": 45
    #             }
    #         }
    #     },
    #     "SNAPSHOT": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "count": 2,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 71
    #             }
    #         }
    #     },
    #     "SNAPSHOT_REF": {
    #         "storage_ops": {
    #             "S3_HeadObject": {
    #                 "count": 2,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 81
    #             },
    #             "S3_PutObject": {
    #                 "count": 2,
    #                 "size_bytes": 1171,
    #                 "total_time_ms": 47
    #             }
    #         }
    #     },
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_DeleteObjects": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 65
    #             },
    #             "S3_GetObject": {
    #                 "count": 1,
    #                 "size_bytes": 309,
    #                 "total_time_ms": 24
    #             },
    #             "S3_ListObjectsV2": {
    #                 "count": 3,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 172
    #             },
    #             "S3_PutObject": {
    #                 "count": 1,
    #                 "size_bytes": 309,
    #                 "total_time_ms": 31
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_GetObject": {
    #                 "count": 2,
    #                 "size_bytes": 1218,
    #                 "total_time_ms": 53
    #             },
    #             "S3_ListObjectsV2": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 47
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
#     {
#     "SYMBOL_LIST": {
#         "storage_ops": {
#             "S3_PutObject": {
#                 "count": 2,
#                 "size_bytes": 322,
#                 "total_time_ms": 120
#             }
#         }
#     },
#     "TABLE_DATA": {
#         "storage_ops": {
#             "S3_GetObject": {
#                 "count": 2,
#                 "size_bytes": 158,
#                 "total_time_ms": 40
#             },
#             "S3_PutObject": {
#                 "count": 2,
#                 "size_bytes": 158,
#                 "total_time_ms": 122
#             }
#         }
#     },
#     "TABLE_INDEX": {
#         "storage_ops": {
#             "S3_GetObject": {
#                 "count": 2,
#                 "size_bytes": 1992,
#                 "total_time_ms": 73
#             },
#             "S3_PutObject": {
#                 "count": 2,
#                 "size_bytes": 1992,
#                 "total_time_ms": 92
#             }
#         }
#     },
#     "VERSION": {
#         "storage_ops": {
#             "S3_GetObject": {
#                 "count": 2,
#                 "size_bytes": 0,
#                 "total_time_ms": 42
#             },
#             "S3_PutObject": {
#                 "count": 2,
#                 "size_bytes": 1194,
#                 "total_time_ms": 70
#             }
#         }
#     },
#     "VERSION_REF": {
#         "storage_ops": {
#             "S3_GetObject": {
#                 "count": 4,
#                 "size_bytes": 1266,
#                 "total_time_ms": 99
#             },
#             "S3_PutObject": {
#                 "count": 2,
#                 "size_bytes": 1241,
#                 "total_time_ms": 90
#             }
#         }
#     }
# }
    
    expected_keys = ["TABLE_DATA", "TABLE_INDEX", "VERSION", "SYMBOL_LIST", "VERSION_REF"]
    
    for key, stat in stats.items():
        assert key in expected_keys
        assert "storage_ops" in stats[key]
        storage_ops = stat["storage_ops"]
        if key != "SYMBOL_LIST":
            assert "S3_GetObject" in storage_ops
            assert storage_ops["S3_GetObject"]["count"] >= 2
            assert storage_ops["S3_GetObject"]["total_time_ms"] > 0
            assert "size_bytes" in storage_ops["S3_GetObject"]
    
        assert "S3_PutObject" in storage_ops
        assert storage_ops["S3_PutObject"]["count"] >= 2
        assert storage_ops["S3_PutObject"]["total_time_ms"] > 0
        assert storage_ops["S3_PutObject"]["size_bytes"] > 50
        

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
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "count": 2,
    #                 "size_bytes": 322,
    #                 "total_time_ms": 91
    #             }
    #         }
    #     },
    #     "TABLE_DATA": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "count": 1,
    #                 "size_bytes": 79,
    #                 "total_time_ms": 26
    #             }
    #         }
    #     },
    #     "TABLE_INDEX": {
    #         "storage_ops": {
    #             "S3_GetObject": {
    #                 "count": 3,
    #                 "size_bytes": 3036,
    #                 "total_time_ms": 72
    #             },
    #             "S3_PutObject": {
    #                 "count": 2,
    #                 "size_bytes": 2024,
    #                 "total_time_ms": 160
    #             }
    #         }
    #     },
    #     "VERSION": {
    #         "storage_ops": {
    #             "S3_GetObject": {
    #                 "count": 4,
    #                 "size_bytes": 582,
    #                 "total_time_ms": 191
    #             },
    #             "S3_PutObject": {
    #                 "count": 2,
    #                 "size_bytes": 1191,
    #                 "total_time_ms": 133
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_GetObject": {
    #                 "count": 7,
    #                 "size_bytes": 2466,
    #                 "total_time_ms": 299
    #             },
    #             "S3_PutObject": {
    #                 "count": 2,
    #                 "size_bytes": 1233,
    #                 "total_time_ms": 98
    #             }
    #         }
    #     }
    # }    
    expected_keys = ["TABLE_INDEX", "VERSION", "VERSION_REF"]
    
    for key in expected_keys:
        assert key in stats
        assert "storage_ops" in stats[key]
        storage_ops = stats[key]["storage_ops"]
        
        if "S3_GetObject" in storage_ops:
            assert storage_ops["S3_GetObject"]["count"] > 0
            assert storage_ops["S3_GetObject"]["size_bytes"] > 0
            assert storage_ops["S3_GetObject"]["total_time_ms"] > 0
            
        if "S3_PutObject" in storage_ops:
            assert storage_ops["S3_PutObject"]["count"] > 0
            assert storage_ops["S3_PutObject"]["size_bytes"] > 0
            assert storage_ops["S3_PutObject"]["total_time_ms"] > 0


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
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "count": 4,
    #                 "size_bytes": 680,
    #                 "total_time_ms": 202
    #             }
    #         }
    #     },
    #     "TABLE_DATA": {
    #         "storage_ops": {
    #             "S3_GetObject": {
    #                 "count": 4,
    #                 "size_bytes": 986,
    #                 "total_time_ms": 114
    #             },
    #             "S3_PutObject": {
    #                 "count": 4,
    #                 "size_bytes": 986,
    #                 "total_time_ms": 241
    #             }
    #         }
    #     },
    #     "TABLE_INDEX": {
    #         "storage_ops": {
    #             "S3_GetObject": {
    #                 "count": 4,
    #                 "size_bytes": 4286,
    #                 "total_time_ms": 206
    #             },
    #             "S3_PutObject": {
    #                 "count": 4,
    #                 "size_bytes": 4286,
    #                 "total_time_ms": 248
    #             }
    #         }
    #     },
    #     "VERSION": {
    #         "storage_ops": {
    #             "S3_GetObject": {
    #                 "count": 6,
    #                 "size_bytes": 1204,
    #                 "total_time_ms": 215
    #             },
    #             "S3_PutObject": {
    #                 "count": 4,
    #                 "size_bytes": 2494,
    #                 "total_time_ms": 123
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_GetObject": {
    #                 "count": 12,
    #                 "size_bytes": 5236,
    #                 "total_time_ms": 502
    #             },
    #             "S3_PutObject": {
    #                 "count": 4,
    #                 "size_bytes": 2658,
    #                 "total_time_ms": 200
    #             }
    #         }
    #     }
    # }
    expected_keys = ["TABLE_DATA", "TABLE_INDEX", "VERSION", "SYMBOL_LIST", "VERSION_REF"]    
    for key in expected_keys:
        assert key in stats
        assert "storage_ops" in stats[key]
        storage_ops = stats[key]["storage_ops"]
        
        if "S3_GetObject" in storage_ops:
            assert storage_ops["S3_GetObject"]["count"] > 0
            assert storage_ops["S3_GetObject"]["size_bytes"] > 0
            assert storage_ops["S3_GetObject"]["total_time_ms"] > 0
            
        if "S3_PutObject" in storage_ops:
            assert storage_ops["S3_PutObject"]["count"] > 0
            assert storage_ops["S3_PutObject"]["size_bytes"] > 0
            assert storage_ops["S3_PutObject"]["total_time_ms"] > 0
