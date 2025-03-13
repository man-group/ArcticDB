from arcticdb.toolbox.query_stats import QueryStatsTool

def test_query_stats(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    QueryStatsTool.enable()
    s3_version_store_v1.list_symbols()
    QueryStatsTool.disable()
    stats = QueryStatsTool.get_query_stats()
    """
    Sample output:
    {
        "list_symbols": {
            "total_time_ms": 357,
            "count": 1,
            "key_type": {
                "SYMBOL_LIST": {
                    "storage_ops": {
                        "ListObjectsV2": {
                            "result_count": 1,
                            "total_time_ms": 35,
                            "count": 2
                        }
                    }
                },
                "VERSION_REF": {
                    "storage_ops": {
                        "ListObjectsV2": {
                            "result_count": 1,
                            "total_time_ms": 16,
                            "count": 1
                        }
                    }
                }
            }
        }
    }
    """
    assert "list_symbols" in stats
    list_symbol_stats  = stats["list_symbols"]
    assert {"count", "key_type", "total_time_ms"} == list_symbol_stats.keys()
    assert list_symbol_stats["count"] == 1
    assert list_symbol_stats["total_time_ms"] < 500 and list_symbol_stats["total_time_ms"] > 10
    
    key_types = list_symbol_stats["key_type"]
    assert {"SYMBOL_LIST", "VERSION_REF"} == key_types.keys()
    for key_type_map in key_types.values():
        assert "storage_ops" in key_type_map
        assert "ListObjectsV2" in key_type_map["storage_ops"]
        assert "result_count" in key_type_map["storage_ops"]["ListObjectsV2"]
        assert key_type_map["storage_ops"]["ListObjectsV2"]["result_count"] == 1
        assert key_type_map["storage_ops"]["ListObjectsV2"]["total_time_ms"] > 5 and key_type_map["storage_ops"]["ListObjectsV2"]["total_time_ms"] < 100
    

def test_query_stats_context(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    with QueryStatsTool.context_manager():
        s3_version_store_v1.list_symbols()
    stats = QueryStatsTool.get_query_stats()
    key_types = stats["list_symbols"]["key_type"]
    for key_type in ["l", "r"]:
        assert key_types[key_type]["storage_ops"]["ListObjectsV2"]["result_count"] == 1


def test_query_stats_clear(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    QueryStatsTool.enable()
    s3_version_store_v1.list_symbols()
    QueryStatsTool.disable()
    QueryStatsTool.get_query_stats()
    QueryStatsTool.reset_stats()
    assert not QueryStatsTool.get_query_stats()
    