import arcticdb.toolbox.query_stats as qs
import pytest
from arcticdb.util.test import config_context
from arcticdb.arctic import Arctic

import pandas as pd

# Maps a backend identifier to the TaskType names emitted by the C++ layer.
# Backends use the same per-key-type shape but have distinct top-level op names.
OP_NAMES = {
    "s3": {
        "list": "S3_ListObjectsV2",
        "put": "S3_PutObject",
        "get": "S3_GetObject",
        "delete": "S3_DeleteObjects",
        "head": "S3_HeadObject",
        "read_sub_ops": set(),
    },
    "lmdb": {
        "list": "LMDB_ListObjects",
        "put": "LMDB_PutObject",
        "get": "LMDB_GetObject",
        "delete": "LMDB_DeleteObjects",
        "head": "LMDB_HeadObject",
        "read_sub_ops": {"LMDB_DbiGet", "LMDB_SegmentFromBytes"},
    },
}


@pytest.fixture(params=["s3", "lmdb"])
def backend(request):
    return request.param


@pytest.fixture
def op(backend):
    return OP_NAMES[backend]


@pytest.fixture
def version_store(request, backend):
    fixture_name = "s3_version_store_v1" if backend == "s3" else "lmdb_version_store_v1"
    return request.getfixturevalue(fixture_name)


@pytest.fixture
def storage_fixture(request, backend):
    fixture_name = "s3_storage" if backend == "s3" else "lmdb_storage"
    return request.getfixturevalue(fixture_name)


def verify_list_symbol_stats(op, list_symbol_call_counts):
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

    assert op["delete"] in storage_operations, storage_operations
    assert op["get"] in storage_operations, storage_operations
    assert op["list"] in storage_operations, storage_operations
    assert op["put"] in storage_operations, storage_operations

    assert storage_operations[op["delete"]]["SYMBOL_LIST"]["count"] == 1, storage_operations
    list_objects_stats = storage_operations[op["list"]]
    assert list_objects_stats["SYMBOL_LIST"]["count"] == list_symbol_call_counts + 1, list_objects_stats
    assert list_objects_stats["VERSION_REF"]["count"] == 1, list_objects_stats
    assert storage_operations[op["put"]]["SYMBOL_LIST"]["count"] == 1, storage_operations

    for key_ops in storage_operations:
        for key_type in storage_operations[key_ops]:
            assert storage_operations[key_ops][key_type]["total_time_ms"] < 8000, storage_operations
            if key_ops in (op["put"], op["get"]):
                assert storage_operations[key_ops][key_type]["size_bytes"] > 0, storage_operations
            else:
                assert storage_operations[key_ops][key_type]["size_bytes"] == 0, storage_operations


def test_query_stats(version_store, op, clear_query_stats):
    version_store.write("a", 1)
    qs.enable()
    qs.reset_stats()

    version_store.list_symbols()
    verify_list_symbol_stats(op, 1)
    version_store.list_symbols()
    verify_list_symbol_stats(op, 2)


def test_query_stats_context(version_store, op, clear_query_stats):
    version_store.write("a", 1)
    with qs.query_stats():
        version_store.list_symbols()
    verify_list_symbol_stats(op, 1)

    with qs.query_stats():
        version_store.list_symbols()
    verify_list_symbol_stats(op, 2)


def test_query_stats_clear(version_store, clear_query_stats):
    version_store.write("a", 1)
    qs.enable()
    version_store.list_symbols()
    qs.reset_stats()
    assert not qs.get_query_stats()


def test_query_stats_snapshot(version_store, op, clear_query_stats):
    version_store.write("a", 1)
    qs.enable()
    with config_context("VersionMap.ReloadInterval", 2_000_000_000):
        version_store.snapshot("abc")
    with config_context("VersionMap.ReloadInterval", 0):
        version_store.snapshot("abc2")
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

    assert op["list"] in storage_ops
    assert "SNAPSHOT" in storage_ops[op["list"]]
    snapshot_stats = storage_ops[op["list"]]["SNAPSHOT"]
    assert snapshot_stats["count"] == 2
    assert snapshot_stats["size_bytes"] == 0
    assert snapshot_stats["total_time_ms"] < 8000

    assert "VERSION_REF" in storage_ops[op["get"]]
    vref_stats = storage_ops[op["get"]]["VERSION_REF"]
    assert vref_stats["count"] == 2
    assert vref_stats["size_bytes"] > 0
    assert vref_stats["total_time_ms"] < 8000

    assert "SNAPSHOT_REF" in storage_ops[op["head"]]
    head_object_snapshot_ref_stats = storage_ops[op["head"]]["SNAPSHOT_REF"]
    assert head_object_snapshot_ref_stats["count"] == 2
    assert head_object_snapshot_ref_stats["size_bytes"] == 0
    assert head_object_snapshot_ref_stats["total_time_ms"] < 8000

    assert "SNAPSHOT_REF" in storage_ops[op["put"]]
    put_object_snapshot_ref_stats = storage_ops[op["put"]]["SNAPSHOT_REF"]
    assert put_object_snapshot_ref_stats["count"] == 2
    assert put_object_snapshot_ref_stats["size_bytes"] > 0
    assert put_object_snapshot_ref_stats["total_time_ms"] < 8000


def test_query_stats_read_write(version_store, op, clear_query_stats):
    qs.enable()
    with config_context("VersionMap.ReloadInterval", 2_000_000_000):
        version_store.write("a", 1)
        version_store.write("a", 2)
    with config_context("VersionMap.ReloadInterval", 0):
        version_store.read("a")
        version_store.read("a")
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

    assert {op["get"], op["put"]} | op["read_sub_ops"] == storage_operations.keys()

    expected_get_keys = {"TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}
    expected_put_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}

    assert expected_get_keys == storage_operations[op["get"]].keys()

    # Check specific count values from the sample output
    assert storage_operations[op["get"]]["TABLE_DATA"]["count"] == 2
    assert storage_operations[op["get"]]["TABLE_INDEX"]["count"] == 2
    assert storage_operations[op["get"]]["VERSION"]["count"] == 2
    assert storage_operations[op["get"]]["VERSION_REF"]["count"] == 4

    for key in expected_get_keys:
        stats_entry = storage_operations[op["get"]][key]
    for key in expected_put_keys:
        stats_entry = storage_operations[op["put"]][key]
        assert stats_entry["count"] > 0
        assert stats_entry["size_bytes"] > 0
        assert stats_entry["total_time_ms"] < 8000


def test_query_stats_metadata(version_store, op, clear_query_stats):
    qs.enable()
    meta1 = {"meta1": 1, "arr": [1, 2, 4]}
    with config_context("VersionMap.ReloadInterval", 0):
        version_store.write_metadata("a", meta1)
        version_store.write_metadata("a", meta1)
        version_store.read_metadata("a")
        version_store.read_metadata("a")
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

    assert {op["get"], op["put"]} | op["read_sub_ops"] == storage_operations.keys()

    expected_get_keys = {"TABLE_INDEX", "VERSION", "VERSION_REF"}
    expected_put_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}

    get_object_stats = storage_operations[op["get"]]
    assert expected_get_keys == get_object_stats.keys()

    assert get_object_stats["TABLE_INDEX"]["count"] == 3
    assert get_object_stats["VERSION"]["count"] == 4
    assert get_object_stats["VERSION_REF"]["count"] == 7

    for key in expected_get_keys:
        stats_entry = get_object_stats[key]
        assert stats_entry["size_bytes"] > 0
        assert stats_entry["total_time_ms"] < 8000

    put_object_stats = storage_operations[op["put"]]
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


def test_query_stats_batch(version_store, op, clear_query_stats):
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    qs.enable()
    with config_context("VersionMap.ReloadInterval", 0):
        version_store.batch_write([sym1, sym2], [df0, df0])
        version_store.batch_read([sym1, sym2])
        version_store.batch_write([sym1, sym2], [df1, df1])
        version_store.batch_read([sym1, sym2])

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

    assert {op["get"], op["put"]} | op["read_sub_ops"] == storage_operations.keys()

    expected_get_keys = {"TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}
    expected_put_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}

    get_object_stats = storage_operations[op["get"]]
    assert expected_get_keys == get_object_stats.keys()

    assert get_object_stats["TABLE_DATA"]["count"] == 4
    assert get_object_stats["TABLE_INDEX"]["count"] == 4
    assert get_object_stats["VERSION"]["count"] == 6
    assert get_object_stats["VERSION_REF"]["count"] == 12

    put_object_stats = storage_operations[op["put"]]
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


def test_query_stats_staged_data(version_store, op, clear_query_stats, sym):
    df_0 = pd.DataFrame({"col": [1, 2]}, index=pd.date_range("2024-01-01", periods=2))
    df_1 = pd.DataFrame({"col": [3, 4]}, index=pd.date_range("2024-01-03", periods=2))

    qs.enable()
    version_store.write(sym, df_0, parallel=True)
    version_store.write(sym, df_1, parallel=True)

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
    assert {op["put"]} == storage_operations.keys()
    assert {"APPEND_DATA"} == storage_operations[op["put"]].keys()
    append_data_stats = storage_operations[op["put"]]["APPEND_DATA"]
    assert append_data_stats["count"] == 2
    assert append_data_stats["size_bytes"] > 0
    assert append_data_stats["total_time_ms"] < 8000

    version_store.compact_incomplete(sym, False, False)
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

    expected_operations = {op["put"], op["delete"], op["get"], op["list"]} | op["read_sub_ops"]
    assert expected_operations == storage_operations.keys()

    assert "APPEND_DATA" in storage_operations[op["delete"]]
    delete_append_stats = storage_operations[op["delete"]]["APPEND_DATA"]
    assert delete_append_stats["count"] == 1
    assert delete_append_stats["size_bytes"] == 0
    assert delete_append_stats["total_time_ms"] < 8000

    get_object_ops = storage_operations[op["get"]]
    assert {"APPEND_DATA", "VERSION", "VERSION_REF"} == get_object_ops.keys()
    assert get_object_ops["APPEND_DATA"]["count"] == 4
    assert get_object_ops["VERSION"]["count"] == 2
    assert get_object_ops["VERSION_REF"]["count"] == 2

    for key in get_object_ops:
        stats_entry = get_object_ops[key]
        assert stats_entry["total_time_ms"] < 8000
        if key == "APPEND_DATA":
            assert stats_entry["size_bytes"] > 0

    assert "APPEND_DATA" in storage_operations[op["list"]]
    list_append_stats = storage_operations[op["list"]]["APPEND_DATA"]
    assert list_append_stats["count"] == 1
    assert list_append_stats["size_bytes"] == 0
    assert list_append_stats["total_time_ms"] < 8000

    put_object_ops = storage_operations[op["put"]]
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
def test_query_stats_create_library(storage_fixture, op, clear_query_stats, lib_name, one_op):
    qs.enable()
    if one_op:
        Arctic(storage_fixture.arctic_uri).create_library(lib_name)
    else:
        ac = Arctic(storage_fixture.arctic_uri)
        ac.create_library(lib_name)

    stats = qs.get_query_stats()

    assert stats["storage_operations"].keys() == {op["get"], op["head"], op["put"]} | op["read_sub_ops"]
    assert stats["storage_operations"][op["get"]].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"][op["get"]]["LIBRARY_CONFIG"]["count"] == 1
    assert stats["storage_operations"][op["head"]].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"][op["head"]]["LIBRARY_CONFIG"]["count"] == 1
    assert stats["storage_operations"][op["put"]].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"][op["put"]]["LIBRARY_CONFIG"]["count"] == 1


@pytest.mark.parametrize("one_op", [True, False])
def test_query_stats_get_library_exists(storage_fixture, op, clear_query_stats, lib_name, one_op):
    ac = Arctic(storage_fixture.arctic_uri)
    ac.create_library(lib_name)
    del ac
    qs.enable()
    if one_op:
        Arctic(storage_fixture.arctic_uri).get_library(lib_name)
    else:
        ac = Arctic(storage_fixture.arctic_uri)
        ac.get_library(lib_name)

    stats = qs.get_query_stats()
    assert stats["storage_operations"].keys() == {op["get"]} | op["read_sub_ops"]
    assert stats["storage_operations"][op["get"]].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"][op["get"]]["LIBRARY_CONFIG"]["count"] == 1


@pytest.mark.parametrize("one_op", [True, False])
def test_query_stats_get_library_create_if_missing(storage_fixture, op, clear_query_stats, lib_name, one_op):
    qs.enable()
    if one_op:
        Arctic(storage_fixture.arctic_uri).get_library(lib_name, create_if_missing=True)
    else:
        ac = Arctic(storage_fixture.arctic_uri)
        ac.get_library(lib_name, create_if_missing=True)

    stats = qs.get_query_stats()

    assert stats["storage_operations"].keys() == {op["get"], op["head"], op["put"]} | op["read_sub_ops"]
    assert stats["storage_operations"][op["get"]].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"][op["get"]]["LIBRARY_CONFIG"]["count"] == 2
    assert stats["storage_operations"][op["head"]].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"][op["head"]]["LIBRARY_CONFIG"]["count"] == 1
    assert stats["storage_operations"][op["put"]].keys() == {"LIBRARY_CONFIG"}
    assert stats["storage_operations"][op["put"]]["LIBRARY_CONFIG"]["count"] == 1


def test_query_stats_in_mem_read_write(in_memory_version_store, clear_query_stats):
    lib = in_memory_version_store
    qs.enable()
    with config_context("VersionMap.ReloadInterval", 0):
        lib.write("a", 1)
        lib.write("b", 2)
        lib.read("a")
        lib.read("b")
    stats = qs.get_query_stats()
    assert stats

    storage_ops = stats["storage_operations"]
    assert len(storage_ops) == 2
    reads = storage_ops["Memory_GetObject"]

    for key_type in ("TABLE_DATA", "TABLE_INDEX"):
        assert reads[key_type]["count"] == 2
        assert reads[key_type]["size_bytes"] > 0

    assert reads["VERSION_REF"]["count"] == 6
    assert reads["VERSION_REF"]["size_bytes"] > 0
    assert reads["VERSION"]["count"] == 4
    assert reads["VERSION"]["size_bytes"] == 0  # this is from get_if_exists checks. We don't actually read the VERSION
    # key here, instead shortcircuiting to the index key that the VERSION_REF key refers to.

    writes = storage_ops["Memory_PutObject"]
    for key_type in ("TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF", "SYMBOL_LIST"):
        assert writes[key_type]["count"] == 2
        assert writes[key_type]["size_bytes"] > 0


def test_query_stats_in_mem_delete(in_memory_version_store, clear_query_stats):
    lib = in_memory_version_store
    lib.write("a", 1)
    lib.write("b", 2)
    qs.enable()
    lib.delete("a")
    lib.delete("b")
    stats = qs.get_query_stats()
    assert stats

    storage_ops = stats["storage_operations"]
    assert len(storage_ops) == 4

    deletes = storage_ops["Memory_DeleteObject"]
    assert len(deletes) == 3

    assert deletes["COLUMN_STATS"]["count"] == 2
    for key_type in ("TABLE_DATA", "TABLE_INDEX"):
        assert deletes[key_type]["count"] == 2
        assert deletes[key_type]["size_bytes"] > 0

    lists = storage_ops["Memory_ListObjects"]
    for key_type in ("SNAPSHOT", "SNAPSHOT_REF"):
        assert lists[key_type]["count"] == 2
        assert lists[key_type]["size_bytes"] == 0


def test_query_stats_disabled_after_exception(clear_query_stats):
    import arcticdb_ext.tools.query_stats as qs_ext

    with pytest.raises(RuntimeError):
        with qs.query_stats():
            raise RuntimeError("boom")
    assert not qs_ext.is_enabled()
