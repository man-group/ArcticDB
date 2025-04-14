"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import pytest

from installation_tests.shared_tests import execute_test_library_creation_deletion, execute_test_snapshots_and_deletes, execute_test_write_batch

@pytest.mark.installation
def test_write_metadata_with_none(ac_library):
    lib = ac_library
    symbol = "symbol"
    meta = {"meta_" + str(symbol): 0}

    result_write = lib.write_metadata(symbol, meta)
    assert result_write.version == 0

    read_meta_symbol = lib.read_metadata(symbol)
    assert read_meta_symbol.data is None
    assert read_meta_symbol.metadata == meta
    assert read_meta_symbol.version == 0

    read_symbol = lib.read(symbol)
    assert read_symbol.data is None
    assert read_symbol.metadata == meta
    assert read_symbol.version == 0       

@pytest.mark.installation
def test_library_creation_deletion(ac_client, lib_name):
    execute_test_library_creation_deletion(ac_client, lib_name)

@pytest.mark.installation
def test_write_batch(ac_library_factory):
    execute_test_write_batch(ac_library_factory)    

@pytest.mark.installation
def test_snapshots_and_deletes(ac_library):
    execute_test_snapshots_and_deletes(ac_library)