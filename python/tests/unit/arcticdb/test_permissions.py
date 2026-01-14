"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb.authorization.permissions import create_permission, perms_to_openmode, OpenMode


def test_create_permissions():
    library = "test.test"
    perm = create_permission(library, write=True)
    assert perm.write, "Write perms not set"
    assert perm.write.enabled, "Write perms not enabled"


def test_perms_to_openmode():
    library = "test.test"
    perm = create_permission(library, write=True)
    open_mode = perms_to_openmode(perm)
    assert open_mode == OpenMode.DELETE

    perm = create_permission(library, write=False)
    open_mode = perms_to_openmode(perm)
    assert open_mode == OpenMode.READ
