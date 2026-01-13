"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


def test_many_versions(object_version_store):
    syms = [f"symbol_{x}" for x in range(200)]

    data = ["thing" for _ in range(200)]
    object_version_store.batch_write(syms, data)

    object_version_store.snapshot("test_snap")
