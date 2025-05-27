import arcticdb.toolbox.query_stats as qs

def verify_list_symbol_stats(list_symbol_call_counts):
    stats = qs.get_query_stats()
    # """
    # Sample output:
    # {
    #     "SYMBOL_LIST": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "total_time_ms": 83,
    #                 "count": 3
    #             }
    #         }
    #     },
    #     "VERSION_REF": {
    #         "storage_ops": {
    #             "S3_ListObjectsV2": {
    #                 "total_time_ms": 21,
    #                 "count": 1
    #             }
    #         }
    #     }
    # }
    # """    
    assert "SYMBOL_LIST" in stats
    for key, key_type_map in stats.items():
        assert "storage_ops" in key_type_map
        assert "S3_ListObjectsV2" in key_type_map["storage_ops"]
        assert "count" in key_type_map["storage_ops"]["S3_ListObjectsV2"]
        list_object_ststs = key_type_map["storage_ops"]["S3_ListObjectsV2"]
        assert list_object_ststs["count"] == 1 if key == "VERSION_REF" else list_symbol_call_counts
        assert list_object_ststs["total_time_ms"] > 1
        assert list_object_ststs["total_time_ms"] < 200


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
