"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


def test_can_import_native_library():
    import arcticdb_ext


def test_top_level_imports():
    import arcticdb as adb

    imports_list = [
        "Arctic",
        "LibraryOptions",
        "QueryBuilder",
        "QueryBuilder",
        "VersionedItem",
        "library",
        "LibraryOptions",
        "set_config_from_env_vars",
        "DataError",
        "VersionRequestType",
        "ErrorCode",
        "ErrorCategory",
        "WritePayload",
        "ReadInfoRequest",
        "ReadRequest",
        "StagedDataFinalizeMethod",
        "WriteMetadataPayload",
    ]

    for import_item in imports_list:
        assert hasattr(adb, import_item), f"{import_item} not found"
