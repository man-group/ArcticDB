from arcticdb.toolbox.query_stats import QueryStatsTool

def test_query_stats(s3_version_store_v1):
    query_stats_tools_write = QueryStatsTool() # For testing whether stats has been filtered
    s3_version_store_v1.write("a", 1)
    query_stats_tools_start = QueryStatsTool()
    s3_version_store_v1.list_symbols()
    query_stats_tools_end = QueryStatsTool()
    stats = query_stats_tools_end - query_stats_tools_start
    """
    Sample output:
    {
        "list_symbols": {
            "time": {
                "500": 1
            },
            "stages": {
                "list": {
                    "time": {
                        "500": 1
                    },
                    "key_types": {
                        "l": {
                            "storage_ops": {
                                "ListObjectsV2": {
                                    "result_count": 1,
                                    "time": {
                                        "20": 1,
                                        "10": 1
                                    }
                                }
                            }
                        },
                        "r": {
                            "storage_ops": {
                                "ListObjectsV2": {
                                    "result_count": 1,
                                    "time": {
                                        "10": 1
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    assert "list_symbols" in stats
    assert "stages" in stats["list_symbols"]
    assert "list" in stats["list_symbols"]["stages"]
    assert "key_types" in stats["list_symbols"]["stages"]["list"]
    key_types = stats["list_symbols"]["stages"]["list"]["key_types"]
    assert "l" in key_types
    assert "r" in key_types
    
    for key_type in ["l", "r"]:
        assert "storage_ops" in key_types[key_type]
        assert "ListObjectsV2" in key_types[key_type]["storage_ops"]
        assert "result_count" in key_types[key_type]["storage_ops"]["ListObjectsV2"]
        assert key_types[key_type]["storage_ops"]["ListObjectsV2"]["result_count"] == 1
        # Not asserting the time values as they are non-deterministic

def test_query_stats_context(s3_version_store_v1):
    with QueryStatsTool.context_manager(): # For testing whether stats has been filtered
        s3_version_store_v1.write("a", 1)
        with QueryStatsTool.context_manager() as query_stats_tools:
            s3_version_store_v1.list_symbols()
        stats = query_stats_tools.get_query_stats()    
        key_types = stats["list_symbols"]["stages"]["list"]["key_types"]
        for key_type in ["l", "r"]:
            assert key_types[key_type]["storage_ops"]["ListObjectsV2"]["result_count"] == 1


def test_query_stats_clear(s3_version_store_v1):
    s3_version_store_v1.write("a", 1)
    query_stats_tools_start = QueryStatsTool()
    s3_version_store_v1.list_symbols()
    query_stats_tools_end = QueryStatsTool()
    assert (query_stats_tools_end - query_stats_tools_start)
    
    query_stats_tools_start = QueryStatsTool()
    query_stats_tools_end = QueryStatsTool()
    assert not (query_stats_tools_end - query_stats_tools_start)
