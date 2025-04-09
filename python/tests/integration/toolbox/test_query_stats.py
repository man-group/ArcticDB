import arcticdb.toolbox.query_stats as qs

def verify_list_symbool_stats(count):
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
            assert "S3_ListObjectsV2" in key_type_map["storage_ops"]
            assert "result_count" in key_type_map["storage_ops"]["S3_ListObjectsV2"]
            list_object_ststs = key_type_map["storage_ops"]["S3_ListObjectsV2"]
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
