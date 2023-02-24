"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
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
