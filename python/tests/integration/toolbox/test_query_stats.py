import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import config_context

import pandas as pd

def verify_list_symbool_stats(count):
    stats = qs.get_query_stats()
    """
    Sample output:
    {
        "list_symbols": {
            "total_time_ms": 393,
            "count": 2,
            "key_type": {
                "LOCK": {
                    "storage_ops": {
                        "DeleteObjects": {
                            "result_count": 1,
                            "total_time_ms": 17,
                            "count": 1
                        },
                        "GetObject": {
                            "compressed_size_bytes": 208,
                            "uncompressed_size_bytes": 16,
                            "result_count": 2,
                            "total_time_ms": 33,
                            "count": 2
                        },
                        "HeadObject": {
                            "total_time_ms": 4,
                            "count": 1
                        },
                        "PutObject": {
                            "compressed_size_bytes": 104,
                            "uncompressed_size_bytes": 8,
                            "total_time_ms": 15,
                            "count": 1
                        }
                    }
                },
                "SYMBOL_LIST": {
                    "storage_ops": {
                        "DeleteObjects": {
                            "result_count": 1,
                            "total_time_ms": 17,
                            "count": 1
                        },
                        "GetObject": {
                            "compressed_size_bytes": 309,
                            "uncompressed_size_bytes": 24,
                            "result_count": 1,
                            "total_time_ms": 15,
                            "count": 1
                        },
                        "ListObjectsV2": {
                            "result_count": 2,
                            "total_time_ms": 51,
                            "count": 3
                        },
                        "PutObject": {
                            "compressed_size_bytes": 309,
                            "uncompressed_size_bytes": 24,
                            "total_time_ms": 15,
                            "count": 1
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
            list_object_stats = key_type_map["storage_ops"]["ListObjectsV2"]
            result_count = list_object_stats["result_count"]
            assert result_count == count if key == "SYMBOL_LIST" else 1 
            assert list_object_stats["total_time_ms"] / result_count > 1
            assert list_object_stats["total_time_ms"] / result_count < 100


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
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.snapshot("abc2")
    stats = qs.get_query_stats()
    """
    "SNAPSHOT": {
        "storage_ops": {
            "ListObjectsV2": {
                "total_time_ms": 34,
                "count": 2
            }
        }
    },
    "SNAPSHOT_REF": {
        "storage_ops": {
            "HeadObject": {
                "total_time_ms": 9,
                "count": 2
            },
            "PutObject": {
                "compressed_size_bytes": 1167,
                "uncompressed_size_bytes": 100,
                "key_type": {
                    "TABLE_INDEX": {
                        "count": 2
                    }
                },
                "total_time_ms": 30,
                "count": 2
            }
        }
    },
    "VERSION_REF": {
        "storage_ops": {
            "GetObject": {
                "compressed_size_bytes": 1218,
                "uncompressed_size_bytes": 200,
                "key_type": {
                    "TABLE_INDEX": {
                        "count": 2
                    },
                    "VERSION": {
                        "count": 2
                    }
                },
                "result_count": 2,
                "total_time_ms": 29,
                "count": 2
            },
            "ListObjectsV2": {
                "result_count": 1,
                "total_time_ms": 15,
                "count": 1
            }
        }
    }
    ...
    """
    assert "snapshot" in stats
    snapshot_stats = stats["snapshot"]
    
    assert {"count", "key_type", "total_time_ms"} == snapshot_stats.keys()
    assert snapshot_stats["count"] == 2
    assert snapshot_stats["total_time_ms"] > 0
    
    key_types = snapshot_stats["key_type"]
    
    assert "SNAPSHOT" in key_types
    assert "storage_ops" in key_types["SNAPSHOT"]
    assert "ListObjectsV2" in key_types["SNAPSHOT"]["storage_ops"]
    list_ops = key_types["SNAPSHOT"]["storage_ops"]["ListObjectsV2"]
    assert list_ops["count"] == 2
    assert list_ops["total_time_ms"] > 0
    
    assert "SNAPSHOT_REF" in key_types
    assert "storage_ops" in key_types["SNAPSHOT_REF"]
    assert "HeadObject" in key_types["SNAPSHOT_REF"]["storage_ops"]
    assert "PutObject" in key_types["SNAPSHOT_REF"]["storage_ops"]
    put_ops = key_types["SNAPSHOT_REF"]["storage_ops"]["PutObject"]
    assert put_ops["count"] == 2
    assert "compressed_size_bytes" in put_ops
    assert "uncompressed_size_bytes" in put_ops
    assert put_ops["total_time_ms"] > 0
    
    assert "VERSION_REF" in key_types
    assert "storage_ops" in key_types["VERSION_REF"]
    assert {"GetObject", "ListObjectsV2"} == key_types["VERSION_REF"]["storage_ops"].keys()
    get_object_ops = key_types["VERSION_REF"]["storage_ops"]["GetObject"]
    assert "result_count" in get_object_ops
    assert get_object_ops["result_count"] == 2
    assert "key_type" in get_object_ops
    assert {"TABLE_INDEX", "VERSION"} == get_object_ops["key_type"].keys()
    assert "count" in get_object_ops["key_type"]["TABLE_INDEX"]
    assert get_object_ops["key_type"]["TABLE_INDEX"]["count"] == 2
    assert "count" in get_object_ops["key_type"]["VERSION"]
    assert get_object_ops["key_type"]["VERSION"]["count"] == 2


def test_query_stats_read_write(s3_version_store_v1, clear_query_stats):
    qs.enable()
    s3_version_store_v1.write("a", 1)
    s3_version_store_v1.write("a", 2)
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.read("a")
        s3_version_store_v1.read("a")
    stats = qs.get_query_stats()
    """
    {
        "read": {
            "total_time_ms": 97,
            "count": 2,
            "key_type": {
                "TABLE_DATA": {
                    "storage_ops": {
                        "GetObject": {
                            "result_count": 2,
                            "total_time_ms": 31,
                            "count": 2
                        }
                    }
                },
                "TABLE_INDEX": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 1992,
                            "uncompressed_size_bytes": 164,
                            "key_type": {
                                "TABLE_DATA": {
                                    "count": 2
                                }
                            },
                            "result_count": 2,
                            "total_time_ms": 30,
                            "count": 2
                        }
                    }
                },
                "VERSION_REF": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 1266,
                            "uncompressed_size_bytes": 300,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 4
                                },
                                "VERSION": {
                                    "count": 2
                                }
                            },
                            "result_count": 2,
                            "total_time_ms": 30,
                            "count": 2
                        }
                    }
                }
            }
        },
        "write": {
            "total_time_ms": 246,
            "count": 2,
            "key_type": {
                "SYMBOL_LIST": {
                    "storage_ops": {
                        "PutObject": {
                            "compressed_size_bytes": 322,
                            "uncompressed_size_bytes": 16,
                            "total_time_ms": 31,
                            "count": 2
                        }
                    }
                },
                "TABLE_DATA": {
                    "storage_ops": {
                        "PutObject": {
                            "compressed_size_bytes": 158,
                            "uncompressed_size_bytes": 16,
                            "total_time_ms": 33,
                            "count": 2
                        }
                    }
                },
                "TABLE_INDEX": {
                    "storage_ops": {
                        "PutObject": {
                            "compressed_size_bytes": 1992,
                            "uncompressed_size_bytes": 164,
                            "key_type": {
                                "TABLE_DATA": {
                                    "count": 2
                                }
                            },
                            "total_time_ms": 32,
                            "count": 2
                        }
                    }
                },
                "VERSION": {
                    "storage_ops": {
                        "GetObject": {
                            "total_time_ms": 34,
                            "count": 2
                        },
                        "PutObject": {
                            "compressed_size_bytes": 1190,
                            "uncompressed_size_bytes": 150,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 2
                                },
                                "VERSION": {
                                    "count": 1
                                }
                            },
                            "total_time_ms": 31,
                            "count": 2
                        }
                    }
                },
                "VERSION_REF": {
                    "storage_ops": {
                        "GetObject": {
                            "total_time_ms": 34,
                            "count": 2
                        },
                        "PutObject": {
                            "compressed_size_bytes": 1243,
                            "uncompressed_size_bytes": 250,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 3
                                },
                                "VERSION": {
                                    "count": 2
                                }
                            },
                            "total_time_ms": 30,
                            "count": 2
                        }
                    }
                }
            }
        }
    }
    """
    assert "read" in stats
    read_stats = stats["read"]
    assert {"count", "key_type", "total_time_ms"} == read_stats.keys()
    assert read_stats["count"] == 2
    assert read_stats["total_time_ms"] > 0
    
    read_key_types = read_stats["key_type"]
    expected_read_keys = {"TABLE_DATA", "TABLE_INDEX", "VERSION_REF"}
    assert expected_read_keys.issubset(read_key_types.keys())
    
    assert "storage_ops" in read_key_types["TABLE_DATA"]
    assert "GetObject" in read_key_types["TABLE_DATA"]["storage_ops"]
    table_data_get = read_key_types["TABLE_DATA"]["storage_ops"]["GetObject"]
    assert table_data_get["result_count"] == 2
    assert table_data_get["count"] == 2
    assert table_data_get["total_time_ms"] > 0
    
    assert "GetObject" in read_key_types["TABLE_INDEX"]["storage_ops"]
    table_index_get = read_key_types["TABLE_INDEX"]["storage_ops"]["GetObject"]
    assert "compressed_size_bytes" in table_index_get
    assert "uncompressed_size_bytes" in table_index_get
    assert "key_type" in table_index_get
    assert "TABLE_DATA" in table_index_get["key_type"]
    assert table_index_get["key_type"]["TABLE_DATA"]["count"] == 2
    
    version_ref_get = read_key_types["VERSION_REF"]["storage_ops"]["GetObject"]
    assert version_ref_get["result_count"] == 2
    assert "key_type" in version_ref_get
    assert {"TABLE_INDEX", "VERSION"} == version_ref_get["key_type"].keys()
    
    assert "write" in stats
    write_stats = stats["write"]
    assert {"count", "key_type", "total_time_ms"} == write_stats.keys()
    assert write_stats["count"] == 2
    assert write_stats["total_time_ms"] > 0
    
    write_key_types = write_stats["key_type"]
    expected_write_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}
    assert expected_write_keys.issubset(write_key_types.keys())
    
    assert "PutObject" in write_key_types["SYMBOL_LIST"]["storage_ops"]
    symbol_list_put = write_key_types["SYMBOL_LIST"]["storage_ops"]["PutObject"]
    assert "compressed_size_bytes" in symbol_list_put
    assert "uncompressed_size_bytes" in symbol_list_put
    assert symbol_list_put["count"] == 2
    
    assert {"GetObject", "PutObject"} == write_key_types["VERSION"]["storage_ops"].keys()
    version_put = write_key_types["VERSION"]["storage_ops"]["PutObject"]
    assert "key_type" in version_put
    assert {"TABLE_INDEX", "VERSION"}.issubset(version_put["key_type"].keys())
    
    assert {"GetObject", "PutObject"} == write_key_types["VERSION_REF"]["storage_ops"].keys()
    version_ref_put = write_key_types["VERSION_REF"]["storage_ops"]["PutObject"]
    assert "compressed_size_bytes" in version_ref_put
    assert "uncompressed_size_bytes" in version_ref_put
    assert "key_type" in version_ref_put
    assert {"TABLE_INDEX", "VERSION"} == write_key_types["VERSION_REF"]["storage_ops"]["PutObject"]["key_type"].keys()


def test_query_stats_metadata(s3_version_store_v1, clear_query_stats):
    qs.enable()
    meta1 = {"meta1" : 1, "arr" : [1, 2, 4]}
    with config_context("VersionMap.ReloadInterval", 0):
        s3_version_store_v1.write_metadata("a", meta1)
        s3_version_store_v1.write_metadata("a", meta1)
        s3_version_store_v1.read_metadata("a")
        s3_version_store_v1.read_metadata("a")
    stats = qs.get_query_stats()
    """
    {
        "read_metadata": {
            "total_time_ms": 68,
            "count": 2,
            "key_type": {
                "TABLE_INDEX": {
                    "storage_ops": {
                        "GetObject": {
                            "result_count": 2,
                            "total_time_ms": 34,
                            "count": 2
                        }
                    }
                },
                "VERSION_REF": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 1248,
                            "uncompressed_size_bytes": 300,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 4
                                },
                                "VERSION": {
                                    "count": 2
                                }
                            },
                            "result_count": 2,
                            "total_time_ms": 30,
                            "count": 2
                        }
                    }
                }
            }
        },
        "write_metadata": {
            "total_time_ms": 337,
            "count": 2,
            "key_type": {
                "SYMBOL_LIST": {
                    "storage_ops": {
                        "PutObject": {
                            "compressed_size_bytes": 322,
                            "uncompressed_size_bytes": 16,
                            "total_time_ms": 30,
                            "count": 2
                        }
                    }
                },
                "TABLE_DATA": {
                    "storage_ops": {
                        "PutObject": {
                            "compressed_size_bytes": 79,
                            "uncompressed_size_bytes": 8,
                            "total_time_ms": 17,
                            "count": 1
                        }
                    }
                },
                "TABLE_INDEX": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 1012,
                            "uncompressed_size_bytes": 82,
                            "key_type": {
                                "TABLE_DATA": {
                                    "count": 1
                                }
                            },
                            "result_count": 1,
                            "total_time_ms": 17,
                            "count": 1
                        },
                        "PutObject": {
                            "compressed_size_bytes": 2024,
                            "uncompressed_size_bytes": 164,
                            "key_type": {
                                "TABLE_DATA": {
                                    "count": 2
                                }
                            },
                            "total_time_ms": 33,
                            "count": 2
                        }
                    }
                },
                "VERSION": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 581,
                            "uncompressed_size_bytes": 50,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 1
                                }
                            },
                            "result_count": 1,
                            "total_time_ms": 66,
                            "count": 4
                        },
                        "PutObject": {
                            "compressed_size_bytes": 1192,
                            "uncompressed_size_bytes": 150,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 2
                                },
                                "VERSION": {
                                    "count": 1
                                }
                            },
                            "total_time_ms": 34,
                            "count": 2
                        }
                    }
                },
                "VERSION_REF": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 1218,
                            "uncompressed_size_bytes": 200,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 2
                                },
                                "VERSION": {
                                    "count": 2
                                }
                            },
                            "result_count": 2,
                            "total_time_ms": 84,
                            "count": 5
                        },
                        "PutObject": {
                            "compressed_size_bytes": 1233,
                            "uncompressed_size_bytes": 250,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 3
                                },
                                "VERSION": {
                                    "count": 2
                                }
                            },
                            "total_time_ms": 33,
                            "count": 2
                        }
                    }
                }
            }
        }
    }
    """
    
    assert "read_metadata" in stats
    read_metadata_stats = stats["read_metadata"]
    assert {"count", "key_type", "total_time_ms"} == read_metadata_stats.keys()
    assert read_metadata_stats["count"] == 2
    assert read_metadata_stats["total_time_ms"] > 0
    
    read_key_types = read_metadata_stats["key_type"]
    assert "TABLE_INDEX" in read_key_types
    assert "VERSION_REF" in read_key_types    
    assert "storage_ops" in read_key_types["TABLE_INDEX"]
    assert "GetObject" in read_key_types["TABLE_INDEX"]["storage_ops"]
    
    table_index_get = read_key_types["TABLE_INDEX"]["storage_ops"]["GetObject"]
    assert "result_count" in table_index_get
    assert table_index_get["result_count"] == 2
    assert table_index_get["count"] == 2
    assert table_index_get["total_time_ms"] > 0
    assert "storage_ops" in read_key_types["VERSION_REF"]
    assert "GetObject" in read_key_types["VERSION_REF"]["storage_ops"]
    
    version_ref_get = read_key_types["VERSION_REF"]["storage_ops"]["GetObject"]
    assert "compressed_size_bytes" in version_ref_get
    assert "uncompressed_size_bytes" in version_ref_get
    assert version_ref_get["compressed_size_bytes"] > 0
    assert version_ref_get["uncompressed_size_bytes"] > 0
    assert "key_type" in version_ref_get
    assert {"TABLE_INDEX", "VERSION"} == version_ref_get["key_type"].keys()
    assert version_ref_get["key_type"]["TABLE_INDEX"]["count"] > 0
    assert version_ref_get["key_type"]["VERSION"]["count"] > 0
    assert "result_count" in version_ref_get
    assert version_ref_get["result_count"] == 2
    assert version_ref_get["count"] == 2
    assert version_ref_get["total_time_ms"] > 0
    
    assert "write_metadata" in stats
    write_metadata_stats = stats["write_metadata"]
    assert {"count", "key_type", "total_time_ms"} == write_metadata_stats.keys()
    assert write_metadata_stats["count"] == 2
    assert write_metadata_stats["total_time_ms"] > 0
    
    write_key_types = write_metadata_stats["key_type"]
    expected_write_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}
    assert expected_write_keys.issubset(write_key_types.keys())
    
    assert "storage_ops" in write_key_types["SYMBOL_LIST"]
    assert "PutObject" in write_key_types["SYMBOL_LIST"]["storage_ops"]
    symbol_list_put = write_key_types["SYMBOL_LIST"]["storage_ops"]["PutObject"]
    assert "compressed_size_bytes" in symbol_list_put
    assert "uncompressed_size_bytes" in symbol_list_put
    assert symbol_list_put["count"] == 2
    assert symbol_list_put["total_time_ms"] > 0
    
    assert "GetObject" in write_key_types["TABLE_INDEX"]["storage_ops"]
    assert "PutObject" in write_key_types["TABLE_INDEX"]["storage_ops"]
    
    table_index_get = write_key_types["TABLE_INDEX"]["storage_ops"]["GetObject"]
    assert "compressed_size_bytes" in table_index_get
    assert "uncompressed_size_bytes" in table_index_get
    assert "key_type" in table_index_get
    assert "TABLE_DATA" in table_index_get["key_type"]
    assert table_index_get["key_type"]["TABLE_DATA"]["count"] == 1
    assert "result_count" in table_index_get
    assert table_index_get["result_count"] == 1
    assert table_index_get["count"] == 1
    
    table_index_put = write_key_types["TABLE_INDEX"]["storage_ops"]["PutObject"]
    assert "compressed_size_bytes" in table_index_put
    assert "uncompressed_size_bytes" in table_index_put
    assert "key_type" in table_index_put
    assert "TABLE_DATA" in table_index_put["key_type"]
    assert table_index_put["key_type"]["TABLE_DATA"]["count"] == 2
    assert table_index_put["total_time_ms"] > 0
    assert table_index_put["count"] == 2
    
    assert "GetObject" in write_key_types["VERSION_REF"]["storage_ops"]
    assert "PutObject" in write_key_types["VERSION_REF"]["storage_ops"]
    version_ref_put = write_key_types["VERSION_REF"]["storage_ops"]["PutObject"]
    assert "compressed_size_bytes" in version_ref_put
    assert "uncompressed_size_bytes" in version_ref_put
    assert "key_type" in version_ref_put
    assert {"TABLE_INDEX", "VERSION"} == version_ref_put["key_type"].keys()


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
    """
    {
        "batch_read": {
            "total_time_ms": 137,
            "count": 2,
            "key_type": {
                "TABLE_DATA": {
                    "storage_ops": {
                        "GetObject": {
                            "result_count": 4,
                            "total_time_ms": 60,
                            "count": 4
                        }
                    }
                },
                "TABLE_INDEX": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 4288,
                            "uncompressed_size_bytes": 328,
                            "key_type": {
                                "TABLE_DATA": {
                                    "count": 4
                                }
                            },
                            "result_count": 4,
                            "total_time_ms": 62,
                            "count": 4
                        }
                    }
                },
                "VERSION_REF": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 2657,
                            "uncompressed_size_bytes": 500,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 6
                                },
                                "VERSION": {
                                    "count": 4
                                }
                            },
                            "result_count": 4,
                            "total_time_ms": 63,
                            "count": 4
                        }
                    }
                }
            }
        },
        "batch_write": {
            "total_time_ms": 300,
            "count": 2,
            "key_type": {
                "SYMBOL_LIST": {
                    "storage_ops": {
                        "PutObject": {
                            "compressed_size_bytes": 680,
                            "uncompressed_size_bytes": 32,
                            "total_time_ms": 60,
                            "count": 4
                        }
                    }
                },
                "TABLE_DATA": {
                    "storage_ops": {
                        "PutObject": {
                            "compressed_size_bytes": 986,
                            "uncompressed_size_bytes": 128,
                            "total_time_ms": 65,
                            "count": 4
                        }
                    }
                },
                "TABLE_INDEX": {
                    "storage_ops": {
                        "PutObject": {
                            "compressed_size_bytes": 4288,
                            "uncompressed_size_bytes": 328,
                            "key_type": {
                                "TABLE_DATA": {
                                    "count": 4
                                }
                            },
                            "total_time_ms": 62,
                            "count": 4
                        }
                    }
                },
                "VERSION": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 1204,
                            "uncompressed_size_bytes": 100,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 2
                                }
                            },
                            "result_count": 2,
                            "total_time_ms": 98,
                            "count": 6
                        },
                        "PutObject": {
                            "compressed_size_bytes": 2494,
                            "uncompressed_size_bytes": 300,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 4
                                },
                                "VERSION": {
                                    "count": 2
                                }
                            },
                            "total_time_ms": 59,
                            "count": 4
                        }
                    }
                },
                "VERSION_REF": {
                    "storage_ops": {
                        "GetObject": {
                            "compressed_size_bytes": 2576,
                            "uncompressed_size_bytes": 400,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 4
                                },
                                "VERSION": {
                                    "count": 4
                                }
                            },
                            "result_count": 4,
                            "total_time_ms": 141,
                            "count": 8
                        },
                        "PutObject": {
                            "compressed_size_bytes": 2657,
                            "uncompressed_size_bytes": 500,
                            "key_type": {
                                "TABLE_INDEX": {
                                    "count": 6
                                },
                                "VERSION": {
                                    "count": 4
                                }
                            },
                            "total_time_ms": 62,
                            "count": 4
                        }
                    }
                }
            }
        }
    }
    """
    assert "batch_read" in stats
    batch_read_stats = stats["batch_read"]
    assert {"count", "key_type", "total_time_ms"} == batch_read_stats.keys()
    assert batch_read_stats["count"] == 2
    assert batch_read_stats["total_time_ms"] > 0
    
    read_key_types = batch_read_stats["key_type"]
    expected_read_keys = {"TABLE_DATA", "TABLE_INDEX", "VERSION_REF"}
    assert expected_read_keys.issubset(read_key_types.keys())
    
    assert "storage_ops" in read_key_types["TABLE_DATA"]
    assert "GetObject" in read_key_types["TABLE_DATA"]["storage_ops"]
    table_data_get = read_key_types["TABLE_DATA"]["storage_ops"]["GetObject"]
    assert table_data_get["result_count"] == 4
    assert table_data_get["count"] == 4
    assert table_data_get["total_time_ms"] > 0
    
    assert "GetObject" in read_key_types["TABLE_INDEX"]["storage_ops"]
    table_index_get = read_key_types["TABLE_INDEX"]["storage_ops"]["GetObject"]
    assert "compressed_size_bytes" in table_index_get
    assert "uncompressed_size_bytes" in table_index_get
    assert "key_type" in table_index_get
    assert "TABLE_DATA" in table_index_get["key_type"]
    assert table_index_get["key_type"]["TABLE_DATA"]["count"] == 4
    assert table_index_get["result_count"] == 4
    
    version_ref_get = read_key_types["VERSION_REF"]["storage_ops"]["GetObject"]
    assert "compressed_size_bytes" in version_ref_get
    assert "uncompressed_size_bytes" in version_ref_get
    assert "key_type" in version_ref_get
    assert {"TABLE_INDEX", "VERSION"} == version_ref_get["key_type"].keys()
    assert version_ref_get["result_count"] == 4
    assert version_ref_get["key_type"]["TABLE_INDEX"]["count"] == 6
    assert version_ref_get["key_type"]["VERSION"]["count"] == 4
    
    assert "batch_write" in stats
    batch_write_stats = stats["batch_write"]
    assert {"count", "key_type", "total_time_ms"} == batch_write_stats.keys()
    assert batch_write_stats["count"] == 2
    assert batch_write_stats["total_time_ms"] > 0
    write_key_types = batch_write_stats["key_type"]
    expected_write_keys = {"SYMBOL_LIST", "TABLE_DATA", "TABLE_INDEX", "VERSION", "VERSION_REF"}
    assert expected_write_keys.issubset(write_key_types.keys())
    
    assert "PutObject" in write_key_types["SYMBOL_LIST"]["storage_ops"]
    symbol_list_put = write_key_types["SYMBOL_LIST"]["storage_ops"]["PutObject"]
    assert "compressed_size_bytes" in symbol_list_put
    assert "uncompressed_size_bytes" in symbol_list_put
    assert symbol_list_put["count"] == 4
    assert {"GetObject", "PutObject"} == write_key_types["VERSION"]["storage_ops"].keys()
    
    version_get = write_key_types["VERSION"]["storage_ops"]["GetObject"]
    assert "compressed_size_bytes" in version_get
    assert "uncompressed_size_bytes" in version_get
    assert "key_type" in version_get
    assert "TABLE_INDEX" in version_get["key_type"]
    assert version_get["key_type"]["TABLE_INDEX"]["count"] == 2
    assert "result_count" in version_get
    assert version_get["result_count"] == 2
    assert version_get["count"] == 6
    assert version_get["total_time_ms"] > 0
    
    version_put = write_key_types["VERSION"]["storage_ops"]["PutObject"]
    assert "compressed_size_bytes" in version_put
    assert version_put["compressed_size_bytes"] > 0
    assert "uncompressed_size_bytes" in version_put
    assert version_put["uncompressed_size_bytes"] > 0
    assert "key_type" in version_put
    assert {"TABLE_INDEX", "VERSION"} == version_put["key_type"].keys()
    assert version_put["key_type"]["TABLE_INDEX"]["count"] == 4
    assert version_put["key_type"]["VERSION"]["count"] == 2
    assert version_put["total_time_ms"] > 0
    assert version_put["count"] == 4
    
    assert {"GetObject", "PutObject"} == write_key_types["VERSION_REF"]["storage_ops"].keys()
    version_ref_put = write_key_types["VERSION_REF"]["storage_ops"]["PutObject"]
    assert "compressed_size_bytes" in version_ref_put
    assert "uncompressed_size_bytes" in version_ref_put
    assert "key_type" in version_ref_put
    assert {"TABLE_INDEX", "VERSION"} == version_ref_put["key_type"].keys()
    assert version_ref_put["key_type"]["TABLE_INDEX"]["count"] == 6
    assert version_ref_put["key_type"]["VERSION"]["count"] == 4
    
    version_ref_get = write_key_types["VERSION_REF"]["storage_ops"]["GetObject"]
    assert "key_type" in version_ref_get
    assert {"TABLE_INDEX", "VERSION"} == version_ref_get["key_type"].keys()
    assert version_ref_get["key_type"]["TABLE_INDEX"]["count"] == 4
    assert version_ref_get["key_type"]["VERSION"]["count"] == 4