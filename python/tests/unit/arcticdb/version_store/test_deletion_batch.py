"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import pandas as pd
import numpy as np
from arcticdb.exceptions import NoDataFoundException, InternalException
from arcticdb.util.test import assert_frame_equal


@pytest.mark.storage
def test_batch_delete_versions_basic(basic_store):
    """Test basic functionality of batch_delete_versions with multiple symbols."""
    lib = basic_store

    # Create test data
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})

    # Write multiple versions for multiple symbols
    symbols = ["sym1", "sym2", "sym3"]
    for sym in symbols:
        lib.write(sym, df1)
        lib.write(sym, df2, prune_previous_version=False)
        lib.write(sym, df3, prune_previous_version=False)

    # Verify initial state
    for sym in symbols:
        assert len(lib.list_versions(sym)) == 3
        assert_frame_equal(lib.read(sym).data, df3)

    # Delete versions 0 and 1 for all symbols
    versions_to_delete = [[0, 1], [0, 1], [0, 1]]  # One list per symbol
    lib.batch_delete_versions(symbols, versions_to_delete)

    # Verify final state
    for sym in symbols:
        assert len(lib.list_versions(sym)) == 1
        assert_frame_equal(lib.read(sym).data, df3)
        for version in [0, 1]:
            with pytest.raises(NoDataFoundException):
                lib.read(sym, version)


@pytest.mark.storage
def test_batch_delete_versions_with_snapshots(basic_store):
    """Test batch_delete_versions with snapshots."""
    lib = basic_store

    # Create test data
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})

    # Write multiple versions and create snapshots
    symbols = ["sym1", "sym2"]
    for sym in symbols:
        lib.write(sym, df1)
        lib.snapshot(f"{sym}_snap1")
        lib.write(sym, df2, prune_previous_version=False)
        lib.snapshot(f"{sym}_snap2")
        lib.write(sym, df3, prune_previous_version=False)

    # Delete versions 0 and 1 for all symbols
    versions_to_delete = [[0, 1], [0, 1]]  # One list per symbol
    lib.batch_delete_versions(symbols, versions_to_delete)

    # Verify that data is still accessible through snapshots
    for sym in symbols:
        assert_frame_equal(lib.read(sym, as_of=f"{sym}_snap1").data, df1)
        assert_frame_equal(lib.read(sym, as_of=f"{sym}_snap2").data, df2)
        assert_frame_equal(lib.read(sym, as_of=0).data, df1)
        assert_frame_equal(lib.read(sym, as_of=1).data, df2)
        assert_frame_equal(lib.read(sym).data, df3)


@pytest.mark.storage
def test_batch_delete_versions_partial_symbols(basic_store):
    """Test batch_delete_versions with a subset of symbols."""
    lib = basic_store

    # Create test data
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})

    # Write multiple versions for multiple symbols
    symbols = ["sym1", "sym2", "sym3", "sym4"]
    for sym in symbols:
        lib.write(sym, df1)
        lib.write(sym, df2, prune_previous_version=False)
        lib.write(sym, df3, prune_previous_version=False)

    # Delete versions 0 and 1 for only sym1 and sym3
    symbols_to_delete = ["sym1", "sym3"]
    versions_to_delete = [[0, 1], [0, 1]]  # One list per symbol
    results = lib.batch_delete_versions(symbols_to_delete, versions_to_delete)
    # There should be no errors
    assert len(results) == 0
    # Verify that only specified symbols were affected
    for sym in symbols_to_delete:
        assert len(lib.list_versions(sym)) == 1
        assert_frame_equal(lib.read(sym).data, df3)
        for version in [0, 1]:
            with pytest.raises(NoDataFoundException):
                lib.read(sym, version)

    results = lib.batch_delete_versions(symbols_to_delete, versions_to_delete)
    assert len(results) == 2
    assert results[0].symbol == "sym1"
    assert results[1].symbol == "sym3"

    for sym in ["sym2", "sym4"]:
        assert len(lib.list_versions(sym)) == 3
        assert_frame_equal(lib.read(sym).data, df3)
        assert_frame_equal(lib.read(sym, 0).data, df1)
        assert_frame_equal(lib.read(sym, 1).data, df2)


@pytest.mark.storage
def test_batch_delete_versions_empty_input(basic_store):
    """Test batch_delete_versions with empty input lists."""
    lib = basic_store

    # Create test data
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})

    # Write data
    symbols = ["sym1", "sym2"]
    for sym in symbols:
        lib.write(sym, df1)
        lib.write(sym, df2, prune_previous_version=False)

    # Test with empty symbols list
    res = lib.batch_delete_versions([], [])
    assert len(res) == 0

    assert len(lib.list_symbols()) == 2
    for sym in symbols:
        assert len(lib.list_versions(sym)) == 2
        assert_frame_equal(lib.read(sym).data, df2)
        assert_frame_equal(lib.read(sym, 0).data, df1)
        assert_frame_equal(lib.read(sym, 1).data, df2)

    # Test with empty versions list
    res = lib.batch_delete_versions(symbols, [[], []])
    assert len(res) == 0

    assert len(lib.list_symbols()) == 0
    for sym in symbols:
        assert len(lib.list_versions(sym)) == 0
        with pytest.raises(NoDataFoundException):
            lib.read(sym)
        with pytest.raises(NoDataFoundException):
            lib.read(sym, 0)


@pytest.mark.storage
def test_batch_delete_versions_invalid_input(basic_store):
    """Test batch_delete_versions with invalid inputs."""
    lib = basic_store

    # Create test data
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    lib.write("sym1", df1)
    lib.write("sym2", df1)

    # Test with non-existent symbol
    res = lib.batch_delete_versions(["non_existent"], [[0]])
    assert len(res) == 1
    assert res[0].symbol == "non_existent"

    # Test with non-existent version for one symbol
    res = lib.batch_delete_versions(["sym1", "sym2"], [[1], [0]])
    assert len(res) == 1
    assert res[0].symbol == "sym1"

    # sym1 should still be accessible
    # sym2 should be deleted
    assert_frame_equal(lib.read("sym1").data, df1)
    assert len(lib.list_versions("sym1")) == 1
    assert len(lib.list_versions("sym2")) == 0
    assert lib.list_symbols() == ["sym1"]

    # Test with invalid version number for sym1
    with pytest.raises(TypeError):
        lib.batch_delete_versions(["sym1", "sym2"], [[-1], [0]])


@pytest.mark.storage
def test_batch_delete_versions_with_tombstones(basic_store):
    """Test batch_delete_versions with tombstone functionality."""
    lib = basic_store

    # Create test data
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})

    # Write multiple versions for multiple symbols
    symbols = ["sym1", "sym2"]
    for sym in symbols:
        lib.write(sym, df1)
        lib.write(sym, df2, prune_previous_version=False)
        lib.write(sym, df3, prune_previous_version=False)

    # Delete versions 0 and 1 for all symbols
    versions_to_delete = [[0, 1], [0, 1]]  # One list per symbol
    lib.batch_delete_versions(symbols, versions_to_delete)

    # Verify tombstone behavior
    for sym in symbols:
        versions = lib.list_versions(sym)
        assert len(versions) == 1  # All versions should still be listed

        # Verify that deleted versions are not accessible
        for version in [0, 1]:
            with pytest.raises(NoDataFoundException):
                lib.read(sym, version)

        # Verify that non-deleted version is still accessible
        assert_frame_equal(lib.read(sym).data, df3)
