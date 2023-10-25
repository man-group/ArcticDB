"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


def test_many_versions(object_version_store):
    for x in range(200):
        object_version_store.write("symbol_{}".format(x), "thing")

    object_version_store.snapshot("test_snap")
