import arcticdb.toolbox.query_stats as qs
import pytest
from arcticdb.util.test import config_context
from arcticdb.arctic import Arctic

import pandas as pd


def verify_list_symbol_stats(list_symbol_call_counts):
    stats = qs.get_query_stats()
    # Sample output:
    # {
    #     "storage_operations": {
    #         "S3_DeleteObjects": {
    #             "LOCK": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 14
    #             },
    #             "SYMBOL_LIST": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 17
    #             }
    #         },
    #         "S3_GetObject": {
    #             "LOCK": {
    #                 "count": 2,
    #                 "size_bytes": 206,
    #                 "total_time_ms": 31
    #             }
    #         },
    #         "S3_ListObjectsV2": {
    #             "SYMBOL_LIST": {
    #                 "count": 2,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 35
    #             },
    #             "VERSION_REF": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 15
    #             }
    #         },
    #         "S3_PutObject": {
    #             "LOCK": {
    #                 "count": 1,
    #                 "size_bytes": 103,
    #                 "total_time_ms": 15
    #             },
    #             "SYMBOL_LIST": {
    #                 "count": 1,
    #                 "size_bytes": 308,
    #                 "total_time_ms": 15
    #             }
    #         }
    #     }
    # }
    storage_operations = stats["storage_operations"]

    assert "S3_DeleteObjects" in storage_operations, storage_operations
    assert "S3_GetObject" in storage_operations, storage_operations
    assert "S3_ListObjectsV2" in storage_operations, storage_operations
    assert "S3_PutObject" in storage_operations, storage_operations

    assert storage_operations["S3_DeleteObjects"]["SYMBOL_LIST"]["count"] == 1, storage_operations
    list_objects_stats = storage_operations["S3_ListObjectsV2"]
    assert list_objects_stats["SYMBOL_LIST"]["count"] == list_symbol_call_counts + 1, list_objects_stats
    assert list_objects_stats["VERSION_REF"]["count"] == 1, list_objects_stats
    assert storage_operations["S3_PutObject"]["SYMBOL_LIST"]["count"] == 1, storage_operations

    for key_ops in storage_operations:
        for op in storage_operations[key_ops]:
            assert storage_operations[key_ops][op]["total_time_ms"] < 8000, storage_operations
            if key_ops == "S3_PutObject" or key_ops == "S3_GetObject":
                assert storage_operations[key_ops][op]["size_bytes"] > 0, storage_operations
            else:
                assert storage_operations[key_ops][op]["size_bytes"] == 0, storage_operations


def test_query_stats(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    qs.reset_stats()

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


def test_query_stats_snapshot(s3_version_store_v1, clear_query_stats):
    s3_version_store_v1.write("a", 1)
    qs.enable()
    with config_context("VersionMap.ReloadInterval", 2_000_000_000):
        s3_version_store_v1.snapshot("abc")
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.snapshot("abc2")
    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_DeleteObjects": {
    #             "LOCK": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 15
    #             },
    #             "SYMBOL_LIST": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 16
    #             }
    #         },
    #         "S3_GetObject": {
    #             "LOCK": {
    #                 "count": 2,
    #                 "size_bytes": 208,
    #                 "total_time_ms": 32
    #             },
    #             "SYMBOL_LIST": {
    #                 "count": 1,
    #                 "size_bytes": 309,
    #                 "total_time_ms": 15
    #             },
    #             "VERSION_REF": {
    #                 "count": 2,
    #                 "size_bytes": 1218,
    #                 "total_time_ms": 32
    #             }
    #         },
    #         "S3_HeadObject": {
    #             "LOCK": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 5
    #             },
    #             "SNAPSHOT_REF": {
    #                 "count": 2,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 11
    #             }
    #         },
    #         "S3_ListObjectsV2": {
    #             "SNAPSHOT": {
    #                 "count": 2,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 37
    #             },
    #             "SYMBOL_LIST": {
    #                 "count": 3,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 46
    #             },
    #             "VERSION_REF": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 15
    #             }
    #         },
    #         "S3_PutObject": {
    #             "LOCK": {
    #                 "count": 1,
    #                 "size_bytes": 104,
    #                 "total_time_ms": 16
    #             },
    #             "SNAPSHOT_REF": {
    #                 "count": 2,
    #                 "size_bytes": 1169,
    #                 "total_time_ms": 32
    #             },
    #             "SYMBOL_LIST": {
    #                 "count": 1,
    #                 "size_bytes": 309,
    #                 "total_time_ms": 15
    #             }
    #         }
    #     }
    # }

    assert "storage_operations" in stats
    storage_ops = stats["storage_operations"]

    assert "S3_ListObjectsV2" in storage_ops
    assert "SNAPSHOT" in storage_ops["S3_ListObjectsV2"]
    snapshot_stats = storage_ops["S3_ListObjectsV2"]["SNAPSHOT"]
    assert snapshot_stats["count"] == 2
    assert snapshot_stats["size_bytes"] == 0
    assert snapshot_stats["total_time_ms"] < 8000

    assert "VERSION_REF" in storage_ops["S3_GetObject"]
    vref_stats = storage_ops["S3_GetObject"]["VERSION_REF"]
    assert vref_stats["count"] == 2
    assert vref_stats["size_bytes"] > 0
    assert vref_stats["total_time_ms"] < 8000

    assert "SNAPSHOT_REF" in storage_ops["S3_HeadObject"]
    head_object_snapshot_ref_stats = storage_ops["S3_HeadObject"]["SNAPSHOT_REF"]
    assert head_object_snapshot_ref_stats["count"] == 2
    assert head_object_snapshot_ref_stats["size_bytes"] == 0
    assert head_object_snapshot_ref_stats["total_time_ms"] < 8000

    assert "SNAPSHOT_REF" in storage_ops["S3_PutObject"]
    put_object_snapshot_ref_stats = storage_ops["S3_PutObject"]["SNAPSHOT_REF"]
    assert put_object_snapshot_ref_stats["count"] == 2
    assert put_object_snapshot_ref_stats["size_bytes"] > 0
    assert put_object_snapshot_ref_stats["total_time_ms"] < 8000


def test_query_stats_read_write(s3_version_store_v1, clear_query_stats):
    qs.enable()
    with config_context("VersionMap.ReloadInterval", 2_000_000_000):
        s3_version_store_v1.write("a", 1)
        s3_version_store_v1.write("a", 2)
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.read("a")
        s3_version_store_v1.read("a")
    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_GetObject": {
    #             "TABLE_DATA": {
    #                 "count": 2,
    #                 "size_bytes": 158,
    #                 "total_time_ms": 31
    #             },
    #             "TABLE_INDEX": {
    #                 "count": 2,
    #                 "size_bytes": 1990,
    #                 "total_time_ms": 32
    #             },
    #             "VERSION": {
    #                 "count": 2,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 34
    #             },
    #             "VERSION_REF": {
    #                 "count": 4,
    #                 "size_bytes": 1262,
    #                 "total_time_ms": 66
    #             }
    #         },
    #         "S3_PutObject": {
    #             "SYMBOL_LIST": {
    #                 "count": 2,
    #                 "size_bytes": 322,
    #                 "total_time_ms": 31
    #             },
    #             "TABLE_DATA": {
    #                 "count": 2,
    #                 "size_bytes": 158,
    #                 "total_time_ms": 33
    #             },
    #             "TABLE_INDEX": {
    #                 "count": 2,
    #                 "size_bytes": 1991,
    #                 "total_time_ms": 30
    #             },
    #             "VERSION": {
    #                 "count": 2,
    #                 "size_bytes": 1192,
    #                 "total_time_ms": 33
    #             },
    #             "VERSION_REF": {
    #                 "count": 2,
    #                 "size_bytes": 1241,
    #                 "total_time_ms": 31
    #             }
    #         }
    #     }
    # }

    assert "storage_operations" in stats
    storage_operations = stats["storage_operations"]

    assert {"S3_GetObject", "S3_PutObject"} == storage_operations.keys()

    expected_get_keys = {"TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}
    expected_put_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}

    assert expected_get_keys == storage_operations["S3_GetObject"].keys()

    # Check specific count values from the sample output
    assert storage_operations["S3_GetObject"]["TABLE_DATA"]["count"] == 2
    assert storage_operations["S3_GetObject"]["TABLE_INDEX"]["count"] == 2
    assert storage_operations["S3_GetObject"]["VERSION"]["count"] == 2
    assert storage_operations["S3_GetObject"]["VERSION_REF"]["count"] == 4

    for key in expected_get_keys:
        stats_entry = storage_operations["S3_GetObject"][key]
    for key in expected_put_keys:
        stats_entry = storage_operations["S3_PutObject"][key]
        assert stats_entry["count"] > 0
        assert stats_entry["size_bytes"] > 0
        assert stats_entry["total_time_ms"] < 8000


def test_query_stats_metadata(s3_version_store_v1, clear_query_stats):
    qs.enable()
    meta1 = {"meta1": 1, "arr": [1, 2, 4]}
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.write_metadata("a", meta1)
        s3_version_store_v1.write_metadata("a", meta1)
        s3_version_store_v1.read_metadata("a")
        s3_version_store_v1.read_metadata("a")
    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_GetObject": {
    #             "TABLE_INDEX": {
    #                 "count": 3,
    #                 "size_bytes": 3036,
    #                 "total_time_ms": 48
    #             },
    #             "VERSION": {
    #                 "count": 4,
    #                 "size_bytes": 581,
    #                 "total_time_ms": 69
    #             },
    #             "VERSION_REF": {
    #                 "count": 7,
    #                 "size_bytes": 2466,
    #                 "total_time_ms": 118
    #             }
    #         },
    #         "S3_PutObject": {
    #             "SYMBOL_LIST": {
    #                 "count": 2,
    #                 "size_bytes": 322,
    #                 "total_time_ms": 31
    #             },
    #             "TABLE_DATA": {
    #                 "count": 1,
    #                 "size_bytes": 79,
    #                 "total_time_ms": 17
    #             },
    #             "TABLE_INDEX": {
    #                 "count": 2,
    #                 "size_bytes": 2024,
    #                 "total_time_ms": 30
    #             },
    #             "VERSION": {
    #                 "count": 2,
    #                 "size_bytes": 1192,
    #                 "total_time_ms": 30
    #             },
    #             "VERSION_REF": {
    #                 "count": 2,
    #                 "size_bytes": 1233,
    #                 "total_time_ms": 30
    #             }
    #         }
    #     }
    # }
    assert "storage_operations" in stats
    storage_operations = stats["storage_operations"]

    assert {"S3_GetObject", "S3_PutObject"} == storage_operations.keys()

    expected_get_keys = {"TABLE_INDEX", "VERSION", "VERSION_REF"}
    expected_put_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}

    get_object_stats = storage_operations["S3_GetObject"]
    assert expected_get_keys == get_object_stats.keys()

    assert get_object_stats["TABLE_INDEX"]["count"] == 3
    assert get_object_stats["VERSION"]["count"] == 4
    assert get_object_stats["VERSION_REF"]["count"] == 7

    for key in expected_get_keys:
        stats_entry = get_object_stats[key]
        assert stats_entry["size_bytes"] > 0
        assert stats_entry["total_time_ms"] < 8000

    put_object_stats = storage_operations["S3_PutObject"]
    assert expected_put_keys == put_object_stats.keys()

    # Check specific count values from the sample output
    assert put_object_stats["SYMBOL_LIST"]["count"] == 2
    assert put_object_stats["TABLE_DATA"]["count"] == 1
    assert put_object_stats["TABLE_INDEX"]["count"] == 2
    assert put_object_stats["VERSION"]["count"] == 2
    assert put_object_stats["VERSION_REF"]["count"] == 2

    for key in expected_put_keys:
        stats_entry = put_object_stats[key]
        assert stats_entry["size_bytes"] > 0
        assert stats_entry["total_time_ms"] < 8000


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
    #     "storage_operations": {
    #         "S3_GetObject": {
    #             "TABLE_DATA": {
    #                 "count": 4,
    #                 "size_bytes": 986,
    #                 "total_time_ms": 63
    #             },
    #             "TABLE_INDEX": {
    #                 "count": 4,
    #                 "size_bytes": 4287,
    #                 "total_time_ms": 70
    #             },
    #             "VERSION": {
    #                 "count": 6,
    #                 "size_bytes": 1204,
    #                 "total_time_ms": 99
    #             },
    #             "VERSION_REF": {
    #                 "count": 12,
    #                 "size_bytes": 5231,
    #                 "total_time_ms": 223
    #             }
    #         },
    #         "S3_PutObject": {
    #             "SYMBOL_LIST": {
    #                 "count": 4,
    #                 "size_bytes": 680,
    #                 "total_time_ms": 60
    #             },
    #             "TABLE_DATA": {
    #                 "count": 4,
    #                 "size_bytes": 986,
    #                 "total_time_ms": 67
    #             },
    #             "TABLE_INDEX": {
    #                 "count": 4,
    #                 "size_bytes": 4287,
    #                 "total_time_ms": 64
    #             },
    #             "VERSION": {
    #                 "count": 4,
    #                 "size_bytes": 2494,
    #                 "total_time_ms": 61
    #             },
    #             "VERSION_REF": {
    #                 "count": 4,
    #                 "size_bytes": 2655,
    #                 "total_time_ms": 62
    #             }
    #         }
    #     }
    # }
    assert "storage_operations" in stats
    storage_operations = stats["storage_operations"]

    assert {"S3_GetObject", "S3_PutObject"} == storage_operations.keys()

    expected_get_keys = {"TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}
    expected_put_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}

    get_object_stats = storage_operations["S3_GetObject"]
    assert expected_get_keys == get_object_stats.keys()

    assert get_object_stats["TABLE_DATA"]["count"] == 4
    assert get_object_stats["TABLE_INDEX"]["count"] == 4
    assert get_object_stats["VERSION"]["count"] == 6
    assert get_object_stats["VERSION_REF"]["count"] == 12

    put_object_stats = storage_operations["S3_PutObject"]
    assert expected_put_keys == put_object_stats.keys()

    assert put_object_stats["SYMBOL_LIST"]["count"] == 4
    assert put_object_stats["TABLE_DATA"]["count"] == 4
    assert put_object_stats["TABLE_INDEX"]["count"] == 4
    assert put_object_stats["VERSION"]["count"] == 4
    assert put_object_stats["VERSION_REF"]["count"] == 4

    for op_stats in storage_operations.values():
        for key_stat in op_stats.values():
            assert key_stat["count"] > 0
            assert key_stat["size_bytes"] > 0
            assert key_stat["total_time_ms"] < 8000


def test_query_stats_staged_data(s3_version_store_v1, clear_query_stats, sym):
    df_0 = pd.DataFrame({"col": [1, 2]}, index=pd.date_range("2024-01-01", periods=2))
    df_1 = pd.DataFrame({"col": [3, 4]}, index=pd.date_range("2024-01-03", periods=2))

    qs.enable()
    s3_version_store_v1.write(sym, df_0, parallel=True)
    s3_version_store_v1.write(sym, df_1, parallel=True)

    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_PutObject": {
    #             "APPEND_DATA": {
    #                 "count": 2,
    #                 "size_bytes": 950,
    #                 "total_time_ms": 67
    #             }
    #         }
    #     }
    # }
    assert "storage_operations" in stats
    storage_operations = stats["storage_operations"]
    assert {"S3_PutObject"} == storage_operations.keys()
    assert {"APPEND_DATA"} == storage_operations["S3_PutObject"].keys()
    append_data_stats = storage_operations["S3_PutObject"]["APPEND_DATA"]
    assert append_data_stats["count"] == 2
    assert append_data_stats["size_bytes"] > 0
    assert append_data_stats["total_time_ms"] < 8000

    s3_version_store_v1.compact_incomplete(sym, False, False)
    stats = qs.get_query_stats()
    # {
    #     "storage_operations": {
    #         "S3_DeleteObjects": {
    #             "APPEND_DATA": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 17
    #             }
    #         },
    #         "S3_GetObject": {
    #             "APPEND_DATA": {
    #                 "count": 4,
    #                 "size_bytes": 1900,
    #                 "total_time_ms": 63
    #             },
    #             "VERSION": {
    #                 "count": 2,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 34
    #             },
    #             "VERSION_REF": {
    #                 "count": 2,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 34
    #             }
    #         },
    #         "S3_ListObjectsV2": {
    #             "APPEND_DATA": {
    #                 "count": 1,
    #                 "size_bytes": 0,
    #                 "total_time_ms": 52
    #             }
    #         },
    #         "S3_PutObject": {
    #             "APPEND_DATA": {
    #                 "count": 2,
    #                 "size_bytes": 950,
    #                 "total_time_ms": 67
    #             },
    #             "SYMBOL_LIST": {
    #                 "count": 1,
    #                 "size_bytes": 212,
    #                 "total_time_ms": 15
    #             },
    #             "TABLE_DATA": {
    #                 "count": 1,
    #                 "size_bytes": 231,
    #                 "total_time_ms": 16
    #             },
    #             "TABLE_INDEX": {
    #                 "count": 1,
    #                 "size_bytes": 1205,
    #                 "total_time_ms": 15
    #             },
    #             "VERSION": {
    #                 "count": 1,
    #                 "size_bytes": 688,
    #                 "total_time_ms": 15
    #             },
    #             "VERSION_REF": {
    #                 "count": 1,
    #                 "size_bytes": 729,
    #                 "total_time_ms": 15
    #             }
    #         }
    #     }
    # }
    assert "storage_operations" in stats
    storage_operations = stats["storage_operations"]

    expected_operations = {"S3_PutObject", "S3_DeleteObjects", "S3_GetObject", "S3_ListObjectsV2"}
    assert expected_operations == storage_operations.keys()

    assert "APPEND_DATA" in storage_operations["S3_DeleteObjects"]
    delete_append_stats = storage_operations["S3_DeleteObjects"]["APPEND_DATA"]
    assert delete_append_stats["count"] == 1
    assert delete_append_stats["size_bytes"] == 0
    assert delete_append_stats["total_time_ms"] < 8000

    get_object_ops = storage_operations["S3_GetObject"]
    assert {"APPEND_DATA", "VERSION", "VERSION_REF"} == get_object_ops.keys()
    assert get_object_ops["APPEND_DATA"]["count"] == 4
    assert get_object_ops["VERSION"]["count"] == 2
    assert get_object_ops["VERSION_REF"]["count"] == 2

    for key in get_object_ops:
        stats_entry = get_object_ops[key]
        assert stats_entry["total_time_ms"] < 8000
        if key == "APPEND_DATA":
            assert stats_entry["size_bytes"] > 0

    assert "APPEND_DATA" in storage_operations["S3_ListObjectsV2"]
    list_append_stats = storage_operations["S3_ListObjectsV2"]["APPEND_DATA"]
    assert list_append_stats["count"] == 1
    assert list_append_stats["size_bytes"] == 0
    assert list_append_stats["total_time_ms"] < 8000

    put_object_ops = storage_operations["S3_PutObject"]
    expected_puts = {"APPEND_DATA", "SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}
    assert expected_puts == put_object_ops.keys()

    assert put_object_ops["APPEND_DATA"]["count"] == 2
    assert put_object_ops["SYMBOL_LIST"]["count"] == 1
    assert put_object_ops["TABLE_DATA"]["count"] == 1
    assert put_object_ops["TABLE_INDEX"]["count"] == 1
    assert put_object_ops["VERSION"]["count"] == 1
    assert put_object_ops["VERSION_REF"]["count"] == 1

    for key in put_object_ops:
        stats_entry = put_object_ops[key]
        assert stats_entry["size_bytes"] > 0
        assert stats_entry["total_time_ms"] < 8000


@pytest.mark.parametrize("one_op", [True, False])
def test_query_stats_create_library(s3_storage, clear_query_stats, lib_name, one_op):
    qs.enable()
    if one_op:
        Arctic(s3_storage.arctic_uri).create_library(lib_name)
    else:
        ac = Arctic(s3_storage.arctic_uri)
        ac.create_library(lib_name)

    stats = qs.get_query_stats()

    assert stats["storage_operations"].keys() == {"S3_GetObject", "S3_HeadObject", "S3_PutObject"}
    assert stats["storage_operations"]["S3_GetObject"].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"]["S3_GetObject"]["LIBRARY_CONFIG"]["count"] == 1
    assert stats["storage_operations"]["S3_HeadObject"].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"]["S3_HeadObject"]["LIBRARY_CONFIG"]["count"] == 1
    assert stats["storage_operations"]["S3_PutObject"].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"]["S3_PutObject"]["LIBRARY_CONFIG"]["count"] == 1


@pytest.mark.parametrize("one_op", [True, False])
def test_query_stats_get_library_exists(s3_storage, clear_query_stats, lib_name, one_op):
    ac = Arctic(s3_storage.arctic_uri)
    ac.create_library(lib_name)
    del ac
    qs.enable()
    if one_op:
        Arctic(s3_storage.arctic_uri).get_library(lib_name)
    else:
        ac = Arctic(s3_storage.arctic_uri)
        ac.get_library(lib_name)

    stats = qs.get_query_stats()
    assert stats["storage_operations"].keys() == {"S3_GetObject"}
    assert stats["storage_operations"]["S3_GetObject"].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"]["S3_GetObject"]["LIBRARY_CONFIG"]["count"] == 1


@pytest.mark.parametrize("one_op", [True, False])
def test_query_stats_get_library_create_if_missing(s3_storage, clear_query_stats, lib_name, one_op):
    qs.enable()
    if one_op:
        Arctic(s3_storage.arctic_uri).get_library(lib_name, create_if_missing=True)
    else:
        ac = Arctic(s3_storage.arctic_uri)
        ac.get_library(lib_name, create_if_missing=True)

    stats = qs.get_query_stats()

    assert stats["storage_operations"].keys() == {"S3_GetObject", "S3_HeadObject", "S3_PutObject"}
    assert stats["storage_operations"]["S3_GetObject"].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"]["S3_GetObject"]["LIBRARY_CONFIG"]["count"] == 2
    assert stats["storage_operations"]["S3_HeadObject"].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"]["S3_HeadObject"]["LIBRARY_CONFIG"]["count"] == 1
    assert stats["storage_operations"]["S3_PutObject"].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"]["S3_PutObject"]["LIBRARY_CONFIG"]["count"] == 1
