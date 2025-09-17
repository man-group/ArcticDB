"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import tempfile

from arcticdb import Arctic


def test_creation_deletion_lmdb():
    # Non-regression test for #345

    with tempfile.TemporaryDirectory() as lmdb_temp_dirname:
        # The following instructions must complete without failure.
        store = Arctic(f"lmdb://{lmdb_temp_dirname}")
        assert store.list_libraries() == []
        store.create_library("option.1day")
        assert store.list_libraries() == ["option.1day"]
        store.delete_library("option.1day")
        assert store.list_libraries() == []
        del store
