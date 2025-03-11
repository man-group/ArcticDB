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
            "total_time_ms": 476,
            "count": 1,
            "key_type": {
                "l": {
                    "storage_ops": {
                        "ListObjectsV2": {
                            "result_count": 1,
                            "total_time_ms": 48,
                            "count": 2
                        }
                    }
                },
                "r": {
                    "storage_ops": {
                        "ListObjectsV2": {
                            "result_count": 1,
                            "total_time_ms": 21,
                            "count": 1
                        }
                    }
                }
            }
        }
    }
    """
    assert "list_symbols" in stats
    assert "key_type" in stats["list_symbols"]
    key_types = stats["list_symbols"]["key_type"]
    assert "l" in key_types
    assert "r" in key_types
    
    for key_type in ["l", "r"]:
        assert "storage_ops" in key_types[key_type]
        assert "ListObjectsV2" in key_types[key_type]["storage_ops"]
        assert "result_count" in key_types[key_type]["storage_ops"]["ListObjectsV2"]
        assert key_types[key_type]["storage_ops"]["ListObjectsV2"]["result_count"] == 1
        # Not asserting the time values as they are non-deterministic

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
    