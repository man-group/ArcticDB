"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""


def test_many_versions(lmdb_version_store):
    for x in range(200):
        lmdb_version_store.write("symbol_{}".format(x), "thing")

    lmdb_version_store.snapshot("test_snap")
