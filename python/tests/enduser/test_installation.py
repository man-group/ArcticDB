"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import pytest

from tests.enduser.shared_tests import (
    execute_test_library_creation_deletion, 
    execute_test_snapshots_and_deletes, 
    execute_test_write_batch, 
    execute_test_write_metadata_with_none)

@pytest.mark.installation
def test_installation_test_library_creation_deletion(ac_client, lib_name):
    execute_test_library_creation_deletion(ac_client, lib_name)    

@pytest.mark.installation
def test_installation_write_batch(ac_library_factory):
    execute_test_write_batch(ac_library_factory)

@pytest.mark.installation
def test_installation_test_snapshots_and_deletes(ac_library):
    execute_test_snapshots_and_deletes(ac_library) 

@pytest.mark.installation
def test_write_metadata_with_none(ac_library):
    execute_test_write_metadata_with_none(ac_library)