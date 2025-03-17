import arcticdb.toolbox.query_stats as qs

def verify_list_symbool_stats(count):
    stats = qs.get_query_stats()
    """
    Sample output:
    {
        "list_symbols": {
            "total_time_ms": 388,
            "count": 2,
            "storage_ops": {
                ...
                "ListObjectsV2": {
                    "key_type": {
                        "SYMBOL_LIST": {
                            "result_count": 2,
                            "total_time_ms": 51,
                            "count": 3
                        },
                        "VERSION_REF": {
                            "result_count": 1,
                            "total_time_ms": 15,
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
    assert {"count", "storage_ops", "total_time_ms"} == list_symbol_stats.keys()
    assert list_symbol_stats["count"] == count
    assert list_symbol_stats["total_time_ms"] / list_symbol_stats["count"] < 800 # max time is loose as moto could be slow
    assert list_symbol_stats["total_time_ms"] / list_symbol_stats["count"] > 1
    
    assert "ListObjectsV2" in list_symbol_stats["storage_ops"]
    list_objects_map = list_symbol_stats["storage_ops"]["ListObjectsV2"]

    key_types = list_objects_map["key_type"]
    keys_to_check = {"SYMBOL_LIST", "VERSION_REF"}
    for key, key_type_map in key_types.items():
        if key in keys_to_check:
            result_count = key_type_map["result_count"]
            assert result_count == count if key == "SYMBOL_LIST" else 1 
            assert key_type_map["total_time_ms"] / result_count > 1
            assert key_type_map["total_time_ms"] / result_count < 100


def test_query_stats(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    
    s3_version_store_v1.list_symbols()
    verify_list_symbool_stats(1)
    s3_version_store_v1.list_symbols()
    verify_list_symbool_stats(2)
    

def test_query_stats_context(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    with qs.query_stats():
        s3_version_store_v1.list_symbols()
    verify_list_symbool_stats(1)
    
    with qs.query_stats():
        s3_version_store_v1.list_symbols()
    verify_list_symbool_stats(2)


def test_query_stats_clear(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    s3_version_store_v1.list_symbols()
    qs.reset_stats()
    assert not qs.get_query_stats()

    s3_version_store_v1.list_symbols()
    verify_list_symbool_stats(1)


def test_query_stats_snapshot(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    s3_version_store_v1.snapshot("abc")


def test_query_stats_read(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    s3_version_store_v1.read("a")