import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import config_context

import pandas as pd

def verify_list_symbool_stats(count):
    stats = qs.get_query_stats()
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
    assert list_symbol_stats["count"] == count
    assert list_symbol_stats["total_time_ms"] / list_symbol_stats["count"] < 800 # max time is loose as moto could be slow
    assert list_symbol_stats["total_time_ms"] / list_symbol_stats["count"] > 1
    
    key_types = list_symbol_stats["key_type"]
    keys_to_check = {"SYMBOL_LIST", "VERSION_REF"}
    for key, key_type_map in key_types.items():
        if key in keys_to_check:
            assert "storage_ops" in key_type_map
            assert "ListObjectsV2" in key_type_map["storage_ops"]
            assert "result_count" in key_type_map["storage_ops"]["ListObjectsV2"]
            list_object_ststs = key_type_map["storage_ops"]["ListObjectsV2"]
            result_count = list_object_ststs["result_count"]
            assert result_count == count if key == "SYMBOL_LIST" else 1 
            assert list_object_ststs["total_time_ms"] / result_count > 1
            assert list_object_ststs["total_time_ms"] / result_count < 100


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
    import json
    print(json.dumps(qs.get_query_stats(), indent=4))


def test_query_stats_read(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.read("a")
        qs.enable()
        s3_version_store_v1.snapshot("abc")
        # s3_version_store_v1.read("a", as_of="abc")
    import json
    print(json.dumps(qs.get_query_stats(), indent=4))


def test_query_stats_metadata(s3_version_store_v1, clear_query_stats):
    # s3_version_store_v1.write("a", 1)
    qs.enable()
    s3_version_store_v1.write("a", 1)
    # meta1 = {"meta1" : 1, "arr" : [1, 2, 4]}
    # s3_version_store_v1.write_metadata("a", meta1)
    import json
    print(json.dumps(qs.get_query_stats(), indent=4))


def test_query_stats_batch(s3_version_store_v1, clear_query_stats):
    lib_tool = s3_version_store_v1.library_tool()
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    qs.enable()
    s3_version_store_v1.batch_write([sym1, sym2], [df0, df0])

    import json
    print(json.dumps(qs.get_query_stats(), indent=4))
