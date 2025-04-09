import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import config_context
from tests.util.mark import AZURE_TESTS_MARK

import pandas as pd
import pytest
import enum

class Operation(enum.IntEnum):
    READ = 0
    WRITE = 1
    LIST = 2
    EXIST = 3
    DELETE = 4

S3_STORAGE_OPS = ["S3_GetObject", "S3_PutObject", "S3_ListObjectsV2", "S3_HeadObject", "S3_DeleteObjects"]
AZURE_STORAGE_OPS = ["AZURE_DownloadTo", "AZURE_UploadFrom", "AZURE_ListBlobs", "AZURE_GetProperties", "AZURE_DeleteBlob"]

@pytest.fixture(
    scope="function",
    params=(
        "s3_version_store_v1",
        pytest.param("azure_version_store", marks=AZURE_TESTS_MARK),
    ),
)
def query_stats_supported_version_store_and_ops(request):
    if request.param == "s3_version_store_v1":
        storage_ops = S3_STORAGE_OPS
    elif request.param == "azure_version_store":
        storage_ops = AZURE_STORAGE_OPS
    yield (request.getfixturevalue(request.param), storage_ops)


def verify_list_symbool_stats(count, storage_ops):
    stats = qs.get_query_stats()
    # """
    # Sample output:
    # {
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "result_count": 2,
    #                 "total_time_ms": 50,
    #                 "count": 3
    #             },
    #             "S3_PutObject": {
    #                 "total_time_ms": 15,
    #                 "count": 1
    #             },
    #             "S3_GetObject": {
    #                 "result_count": 1,
    #                 "total_time_ms": 15,
    #                 "count": 1
    #             },
    #             "S3_DeleteObjects": {
    #                 "result_count": 1,
    #                 "total_time_ms": 17,
    #                 "count": 1
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "result_count": 1,
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
    #                 "result_count": 2,
    #                 "total_time_ms": 31,
    #                 "count": 2
    #             },
    #             "S3_DeleteObjects": {
    #                 "result_count": 1,
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
            assert storage_ops[Operation.LIST] in key_type_map["storage_ops"]
            assert "result_count" in key_type_map["storage_ops"][storage_ops[Operation.LIST]]
            list_object_ststs = key_type_map["storage_ops"][storage_ops[Operation.LIST]]
            result_count = list_object_ststs["result_count"]
            assert result_count == count if key == "SYMBOL_LIST" else 1 
            assert list_object_ststs["total_time_ms"] / result_count > 1
            assert list_object_ststs["total_time_ms"] / result_count < 100


def test_query_stats(query_stats_supported_version_store_and_ops, clear_query_stats):
    query_stats_supported_version_store, storage_ops = query_stats_supported_version_store_and_ops
    query_stats_supported_version_store.write("a", 1)
    qs.enable()
    
    query_stats_supported_version_store.list_symbols()
    verify_list_symbool_stats(1, storage_ops)
    query_stats_supported_version_store.list_symbols()
    verify_list_symbool_stats(2, storage_ops)
    

def test_query_stats_context(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    with qs.query_stats():
        s3_version_store_v1.list_symbols()
    verify_list_symbool_stats(1, S3_STORAGE_OPS)
    
    with qs.query_stats():
        s3_version_store_v1.list_symbols()
    verify_list_symbool_stats(2, S3_STORAGE_OPS)


def test_query_stats_clear(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    s3_version_store_v1.list_symbols()
    qs.reset_stats()
    assert not qs.get_query_stats()

    s3_version_store_v1.list_symbols()
    verify_list_symbool_stats(1, S3_STORAGE_OPS)


def test_query_stats_snapshot(query_stats_supported_version_store_and_ops, clear_query_stats):
    query_stats_supported_version_store, storage_ops = query_stats_supported_version_store_and_ops
    query_stats_supported_version_store.write("a", 1)
    qs.enable()
    query_stats_supported_version_store.snapshot("abc")
    with config_context("VersionMap.ReloadInterval", 0):
        query_stats_supported_version_store.snapshot("abc2")
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
    #                 "result_count": 2,
    #                 "total_time_ms": 33,
    #                 "count": 2
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "result_count": 2,
    #                 "total_time_ms": 32,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "result_count": 3,
    #                 "total_time_ms": 50,
    #                 "count": 3
    #             },
    #             "Decode": {
    #                 "count": 3,
    #                 "uncompressed_size_bytes": 300,
    #                 "compressed_size_bytes": 1827,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 3
    #                     },
    #                     "VERSION": {
    #                         "count": 3
    #                     }
    #                 }
    #             }
    #         }
    #     },
    #     "LOCK": {
    #         "storage_ops": {
    #             "S3_HeadObject": {
    #                 "total_time_ms": 13,
    #                 "count": 2
    #             },
    #             "Encode": {
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
    #             },
    #             "Encode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 100,
    #                 "compressed_size_bytes": 1169,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 2
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }
    
    assert "SNAPSHOT" in stats
    assert "SNAPSHOT_REF" in stats
    
    assert "storage_ops" in stats["SNAPSHOT"]
    assert storage_ops[Operation.LIST] in stats["SNAPSHOT"]["storage_ops"]
    assert "total_time_ms" in stats["SNAPSHOT"]["storage_ops"][storage_ops[Operation.LIST]]
    assert "count" in stats["SNAPSHOT"]["storage_ops"][storage_ops[Operation.LIST]]
    assert stats["SNAPSHOT"]["storage_ops"][storage_ops[Operation.LIST]]["count"] == 2
    
    assert "storage_ops" in stats["SNAPSHOT_REF"]
    snapshot_ref_ops = stats["SNAPSHOT_REF"]["storage_ops"]
    
    assert storage_ops[Operation.WRITE] in snapshot_ref_ops
    assert storage_ops[Operation.EXIST] in snapshot_ref_ops
    assert "Encode" in snapshot_ref_ops
    
    assert snapshot_ref_ops[storage_ops[Operation.WRITE]]["count"] == 2
    assert snapshot_ref_ops[storage_ops[Operation.EXIST]]["count"] == 2
    assert snapshot_ref_ops["Encode"]["count"] == 2
    assert "uncompressed_size_bytes" in snapshot_ref_ops["Encode"]
    assert "compressed_size_bytes" in snapshot_ref_ops["Encode"]
    assert "key_type" in snapshot_ref_ops["Encode"]
    assert "TABLE_INDEX" in snapshot_ref_ops["Encode"]["key_type"]


def test_query_stats_read_write(query_stats_supported_version_store_and_ops, clear_query_stats):
    query_stats_supported_version_store, storage_ops = query_stats_supported_version_store_and_ops
    qs.enable()
    query_stats_supported_version_store.write("a", 1)
    query_stats_supported_version_store.write("a", 2)
    with config_context("VersionMap.ReloadInterval", 0):
        query_stats_supported_version_store.read("a")
        query_stats_supported_version_store.read("a")
    stats = qs.get_query_stats()
    # {
    #     "TABLE_DATA": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 32,
    #                 "count": 2
    #             },
    #             "S3_GetObject": {
    #                 "result_count": 2,
    #                 "total_time_ms": 30,
    #                 "count": 2
    #             },
    #             "Encode": {
    #                 "total_time_ms": 1,
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 16,
    #                 "compressed_size_bytes": 158
    #             },
    #             "Decode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 16,
    #                 "compressed_size_bytes": 158
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
    #                 "result_count": 2,
    #                 "total_time_ms": 30,
    #                 "count": 2
    #             },
    #             "Encode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 164,
    #                 "compressed_size_bytes": 1991,
    #                 "key_type": {
    #                     "TABLE_DATA": {
    #                         "count": 2
    #                     }
    #                 }
    #             },
    #             "Decode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 164,
    #                 "compressed_size_bytes": 1990,
    #                 "key_type": {
    #                     "TABLE_DATA": {
    #                         "count": 2
    #                     }
    #                 }
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
    #             },
    #             "Encode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 150,
    #                 "compressed_size_bytes": 1192,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 2
    #                     },
    #                     "VERSION": {
    #                         "count": 1
    #                     }
    #                 }
    #             }
    #         }
    #     },
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 32,
    #                 "count": 2
    #             },
    #             "Encode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 16,
    #                 "compressed_size_bytes": 322
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
    #                 "result_count": 2,
    #                 "total_time_ms": 66,
    #                 "count": 4
    #             },
    #             "Encode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 250,
    #                 "compressed_size_bytes": 1241,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 3
    #                     },
    #                     "VERSION": {
    #                         "count": 2
    #                     }
    #                 }
    #             },
    #             "Decode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 300,
    #                 "compressed_size_bytes": 1264,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 4
    #                     },
    #                     "VERSION": {
    #                         "count": 2
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }
    
    expected_keys = ["TABLE_DATA", "TABLE_INDEX", "VERSION", "SYMBOL_LIST", "VERSION_REF"]
    required_ops = {
        "TABLE_DATA": {"Encode", "Decode"},
        "TABLE_INDEX": {"Encode", "Decode"},
        "VERSION": {"Encode"},
        "SYMBOL_LIST": {"Encode"},
        "VERSION_REF": {"Encode", "Decode"}
    }
    expected_storage_ops = {
        "TABLE_DATA": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]},
        "TABLE_INDEX": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]},
        "VERSION": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]},
        "SYMBOL_LIST": {storage_ops[Operation.WRITE]},
        "VERSION_REF": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]}
    }
    
    for key in expected_keys:
        assert key in stats
        assert "storage_ops" in stats[key]
        storage_ops_data = stats[key]["storage_ops"]
        
        for expected_op in expected_storage_ops[key]:
            assert expected_op in storage_ops_data
            op_data = storage_ops_data[expected_op]
            assert "count" in op_data
            assert op_data["count"] > 0
            assert "total_time_ms" in op_data
        
        for op in required_ops[key]:
            assert op in storage_ops_data
            op_data = storage_ops_data[op]
            assert "count" in op_data
            assert op_data["count"] > 0
            
            if "uncompressed_size_bytes" in op_data:
                assert op_data["uncompressed_size_bytes"] > 0
            
            if "compressed_size_bytes" in op_data:
                assert op_data["compressed_size_bytes"] > 0
            
            if "key_type" in op_data:
                assert isinstance(op_data["key_type"], dict)
                assert len(op_data["key_type"]) > 0
                
                if key == "TABLE_INDEX" and op in ["Encode", "Decode"]:
                    assert "TABLE_DATA" in op_data["key_type"]
                
                if key == "VERSION_REF" and op in ["Encode", "Decode"]:
                    assert "TABLE_INDEX" in op_data["key_type"]
                    assert "VERSION" in op_data["key_type"]


def test_query_stats_metadata(query_stats_supported_version_store_and_ops, clear_query_stats):
    query_stats_supported_version_store, storage_ops = query_stats_supported_version_store_and_ops
    qs.enable()
    meta1 = {"meta1" : 1, "arr" : [1, 2, 4]}
    with config_context("VersionMap.ReloadInterval", 0):
        query_stats_supported_version_store.write_metadata("a", meta1)
        query_stats_supported_version_store.write_metadata("a", meta1)
        query_stats_supported_version_store.read_metadata("a")
        query_stats_supported_version_store.read_metadata("a")
    stats = qs.get_query_stats()
    # {
    #     "TABLE_DATA": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 17,
    #                 "count": 1
    #             },
    #             "Encode": {
    #                 "total_time_ms": 1,
    #                 "count": 1,
    #                 "uncompressed_size_bytes": 8,
    #                 "compressed_size_bytes": 79
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
    #                 "result_count": 3,
    #                 "total_time_ms": 47,
    #                 "count": 3
    #             },
    #             "Encode": {
    #                 "total_time_ms": 1,
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 164,
    #                 "compressed_size_bytes": 2024,
    #                 "key_type": {
    #                     "TABLE_DATA": {
    #                         "count": 2
    #                     }
    #                 }
    #             },
    #             "Decode": {
    #                 "count": 1,
    #                 "uncompressed_size_bytes": 82,
    #                 "compressed_size_bytes": 1012,
    #                 "key_type": {
    #                     "TABLE_DATA": {
    #                         "count": 1
    #                     }
    #                 }
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
    #                 "result_count": 1,
    #                 "total_time_ms": 66,
    #                 "count": 4
    #             },
    #             "Encode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 150,
    #                 "compressed_size_bytes": 1193,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 2
    #                     },
    #                     "VERSION": {
    #                         "count": 1
    #                     }
    #                 }
    #             },
    #             "Decode": {
    #                 "count": 1,
    #                 "uncompressed_size_bytes": 50,
    #                 "compressed_size_bytes": 582,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 1
    #                     }
    #                 }
    #             }
    #         }
    #     },
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 30,
    #                 "count": 2
    #             },
    #             "Encode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 16,
    #                 "compressed_size_bytes": 322
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
    #                 "result_count": 4,
    #                 "total_time_ms": 112,
    #                 "count": 7
    #             },
    #             "Encode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 250,
    #                 "compressed_size_bytes": 1234,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 3
    #                     },
    #                     "VERSION": {
    #                         "count": 2
    #                     }
    #                 }
    #             },
    #             "Decode": {
    #                 "count": 4,
    #                 "uncompressed_size_bytes": 500,
    #                 "compressed_size_bytes": 2468,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 6
    #                     },
    #                     "VERSION": {
    #                         "count": 4
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }
    
    expected_keys = ["TABLE_INDEX", "VERSION", "VERSION_REF"]
    required_ops = {
        "TABLE_INDEX": {"Encode", "Decode"},
        "VERSION": {"Encode", "Decode"},
        "VERSION_REF": {"Encode", "Decode"}
    }
    
    expected_storage_ops = {
        "TABLE_INDEX": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]},
        "VERSION": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]},
        "VERSION_REF": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]}
    }
    
    expected_key_types = {
        "TABLE_INDEX": {
            "Encode": ["TABLE_DATA"],
            "Decode": ["TABLE_DATA"]
        },
        "VERSION": {
            "Encode": ["TABLE_INDEX", "VERSION"],
            "Decode": ["TABLE_INDEX"]
        },
        "VERSION_REF": {
            "Encode": ["TABLE_INDEX", "VERSION"],
            "Decode": ["TABLE_INDEX", "VERSION"]
        }
    }
    
    for key in expected_keys:
        assert key in stats
        assert "storage_ops" in stats[key]
        storage_ops_data = stats[key]["storage_ops"]
        
        # Check all expected storage operations are present
        for expected_op in expected_storage_ops[key]:
            assert expected_op in storage_ops_data
            op_data = storage_ops_data[expected_op]
            assert "count" in op_data
            assert op_data["count"] > 0
            assert "total_time_ms" in op_data
        
        for op in required_ops[key]:
            assert op in storage_ops_data
            op_data = storage_ops_data[op]
            assert "count" in op_data
            assert op_data["count"] > 0
            
            if "uncompressed_size_bytes" in op_data:
                assert op_data["uncompressed_size_bytes"] > 0
            
            if "compressed_size_bytes" in op_data:
                assert op_data["compressed_size_bytes"] > 0
            
            if "key_type" in op_data:
                assert isinstance(op_data["key_type"], dict)
                assert len(op_data["key_type"]) > 0
                
                for expected_type in expected_key_types[key][op]:
                    assert expected_type in op_data["key_type"]


def test_query_stats_batch(query_stats_supported_version_store_and_ops, clear_query_stats):
    query_stats_supported_version_store, storage_ops = query_stats_supported_version_store_and_ops
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    qs.enable()
    with config_context("VersionMap.ReloadInterval", 0):
        query_stats_supported_version_store.batch_write([sym1, sym2], [df0, df0])
        query_stats_supported_version_store.batch_read([sym1, sym2])
        query_stats_supported_version_store.batch_write([sym1, sym2], [df1, df1])
        query_stats_supported_version_store.batch_read([sym1, sym2])

    stats = qs.get_query_stats()
    # {
    #     "TABLE_DATA": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 66,
    #                 "count": 4
    #             },
    #             "S3_GetObject": {
    #                 "result_count": 4,
    #                 "total_time_ms": 61,
    #                 "count": 4
    #             },
    #             "Encode": {
    #                 "total_time_ms": 2,
    #                 "count": 4,
    #                 "uncompressed_size_bytes": 128,
    #                 "compressed_size_bytes": 986
    #             },
    #             "Decode": {
    #                 "count": 4,
    #                 "uncompressed_size_bytes": 128,
    #                 "compressed_size_bytes": 986
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
    #                 "result_count": 4,
    #                 "total_time_ms": 63,
    #                 "count": 4
    #             },
    #             "Encode": {
    #                 "total_time_ms": 1,
    #                 "count": 4,
    #                 "uncompressed_size_bytes": 328,
    #                 "compressed_size_bytes": 4288,
    #                 "key_type": {
    #                     "TABLE_DATA": {
    #                         "count": 4
    #                     }
    #                 }
    #             },
    #             "Decode": {
    #                 "count": 4,
    #                 "uncompressed_size_bytes": 328,
    #                 "compressed_size_bytes": 4288,
    #                 "key_type": {
    #                     "TABLE_DATA": {
    #                         "count": 4
    #                     }
    #                 }
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
    #                 "result_count": 2,
    #                 "total_time_ms": 97,
    #                 "count": 6
    #             },
    #             "Encode": {
    #                 "count": 4,
    #                 "uncompressed_size_bytes": 300,
    #                 "compressed_size_bytes": 2493,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 4
    #                     },
    #                     "VERSION": {
    #                         "count": 2
    #                     }
    #                 }
    #             },
    #             "Decode": {
    #                 "count": 2,
    #                 "uncompressed_size_bytes": 100,
    #                 "compressed_size_bytes": 1203,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 2
    #                     }
    #                 }
    #             }
    #         }
    #     },
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_PutObject": {
    #                 "total_time_ms": 63,
    #                 "count": 4
    #             },
    #             "Encode": {
    #                 "count": 4,
    #                 "uncompressed_size_bytes": 32,
    #                 "compressed_size_bytes": 680
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
    #                 "result_count": 8,
    #                 "total_time_ms": 218,
    #                 "count": 12
    #             },
    #             "Encode": {
    #                 "count": 4,
    #                 "uncompressed_size_bytes": 500,
    #                 "compressed_size_bytes": 2656,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 6
    #                     },
    #                     "VERSION": {
    #                         "count": 4
    #                     }
    #                 }
    #             },
    #             "Decode": {
    #                 "count": 8,
    #                 "uncompressed_size_bytes": 900,
    #                 "compressed_size_bytes": 5234,
    #                 "key_type": {
    #                     "TABLE_INDEX": {
    #                         "count": 10
    #                     },
    #                     "VERSION": {
    #                         "count": 8
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }
    
    expected_keys = ["TABLE_DATA", "TABLE_INDEX", "VERSION", "SYMBOL_LIST", "VERSION_REF"]
    required_ops = {
        "TABLE_DATA": {"Encode", "Decode"},
        "TABLE_INDEX": {"Encode", "Decode"},
        "VERSION": {"Encode", "Decode"},
        "SYMBOL_LIST": {"Encode"},
        "VERSION_REF": {"Encode", "Decode"}
    }
    
    expected_counts = {
        "TABLE_DATA": {"Encode": 4, "Decode": 4},
        "TABLE_INDEX": {"Encode": 4, "Decode": 4},
        "VERSION": {"Encode": 4, "Decode": 2},
        "SYMBOL_LIST": {"Encode": 4},
        "VERSION_REF": {"Encode": 4, "Decode": 8}
    }
    
    expected_storage_ops = {
        "TABLE_DATA": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]},
        "TABLE_INDEX": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]},
        "VERSION": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]},
        "SYMBOL_LIST": {storage_ops[Operation.WRITE]},
        "VERSION_REF": {storage_ops[Operation.READ], storage_ops[Operation.WRITE]}
    }
    
    expected_key_types = {
        "TABLE_INDEX": {
            "Encode": ["TABLE_DATA"],
            "Decode": ["TABLE_DATA"]
        },
        "VERSION": {
            "Encode": ["TABLE_INDEX", "VERSION"],
            "Decode": ["TABLE_INDEX"]
        },
        "VERSION_REF": {
            "Encode": ["TABLE_INDEX", "VERSION"],
            "Decode": ["TABLE_INDEX", "VERSION"]
        }
    }
    
    for key in expected_keys:
        assert key in stats
        assert "storage_ops" in stats[key]
        storage_ops_data = stats[key]["storage_ops"]
        
        for expected_op in expected_storage_ops[key]:
            assert expected_op in storage_ops_data
            op_data = storage_ops_data[expected_op]
            assert "count" in op_data
            assert op_data["count"] > 0
            assert "total_time_ms" in op_data
        
        for op in required_ops[key]:
            assert op in storage_ops_data
            op_data = storage_ops_data[op]
            assert "count" in op_data
            assert op_data["count"] == expected_counts[key][op]
            
            if "uncompressed_size_bytes" in op_data:
                assert op_data["uncompressed_size_bytes"] > 0
            
            if "compressed_size_bytes" in op_data:
                assert op_data["compressed_size_bytes"] > 0
            
            if key in expected_key_types and op in expected_key_types[key]:
                assert "key_type" in op_data
                for expected_type in expected_key_types[key][op]:
                    assert expected_type in op_data["key_type"]

    
    for key in expected_keys:
        assert key in stats
        assert "storage_ops" in stats[key]
        storage_ops_data = stats[key]["storage_ops"]
        
        for expected_op in expected_storage_ops[key]:
            assert expected_op in storage_ops_data
            op_data = storage_ops_data[expected_op]
            assert "count" in op_data
            assert op_data["count"] > 0
            assert "total_time_ms" in op_data
        
        for op in required_ops[key]:
            assert op in storage_ops_data
            op_data = storage_ops_data[op]
            assert "count" in op_data
            assert op_data["count"] == expected_counts[key][op]
            
            if "uncompressed_size_bytes" in op_data:
                assert op_data["uncompressed_size_bytes"] > 0
            
            if "compressed_size_bytes" in op_data:
                assert op_data["compressed_size_bytes"] > 0
            
            if key in expected_key_types and op in expected_key_types[key]:
                assert "key_type" in op_data
                for expected_type in expected_key_types[key][op]:
                    assert expected_type in op_data["key_type"]