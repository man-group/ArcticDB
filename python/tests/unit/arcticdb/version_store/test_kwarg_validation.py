"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import pytest

from arcticdb.exceptions import ArcticNativeException
from arcticdb.util.test import config_context
from arcticdb.version_store.processing import QueryBuilder


@pytest.mark.parametrize("method", ["stage", "write", "append", "update", "batch_write", "batch_append"])
@pytest.mark.parametrize("env_var_set", [True, False])
def test_modification_methods(lmdb_version_store_v1, monkeypatch, method, env_var_set):
    if env_var_set:
        monkeypatch.setenv("ARCTICDB_DISABLE_KWARG_VALIDATION", "1")
    lib = lmdb_version_store_v1
    sym = "test_modification_methods"
    df = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    # To avoid non-existent symbol errors on updates
    lib.write(sym, df)
    f = getattr(lib, method)
    arg_0 = [sym] if method.startswith("batch_") else sym
    arg_1 = [df] if method.startswith("batch_") else df
    if env_var_set:
        f(arg_0, arg_1, not_a_kwarg=True)
    else:
        with pytest.raises(ArcticNativeException) as e:
            f(arg_0, arg_1, not_a_kwarg=True)
        msg = str(e.value)
        assert method in msg and "not_a_kwarg" in msg


@pytest.mark.parametrize(
    "method",
    [
        "read",
        "head",
        "tail",
        "read_metadata",
        "read_index",
        "is_symbol_pickled",
        "get_info",
        "get_timerange_for_symbol",
        "get_num_rows",
        "restore_version",
        "defragment_symbol_data",
        "delete",
        "batch_read",
        "batch_read_metadata",
        "batch_read_metadata_multi",
        "batch_restore_version",
    ],
)
@pytest.mark.parametrize("env_var_set", [True, False])
def test_single_argument_methods(lmdb_version_store_v1, monkeypatch, method, env_var_set):
    if env_var_set:
        monkeypatch.setenv("ARCTICDB_DISABLE_KWARG_VALIDATION", "1")
    lib = lmdb_version_store_v1
    sym = "test_read_methods"
    df = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    lib.write(sym, df)
    # For defrag
    lib.append(sym, df)
    f = getattr(lib, method)
    arg_0 = [sym] if method.startswith("batch_") else sym
    with config_context("SymbolDataCompact.SegmentCount", 1):
        if env_var_set:
            f(arg_0, not_a_kwarg=True)
        else:
            with pytest.raises(ArcticNativeException) as e:
                f(arg_0, not_a_kwarg=True)
            msg = str(e.value)
            assert method in msg and "not_a_kwarg" in msg


@pytest.mark.parametrize("env_var_set", [True, False])
def test_batch_read_and_join(lmdb_version_store_v1, monkeypatch, env_var_set):
    if env_var_set:
        monkeypatch.setenv("ARCTICDB_DISABLE_KWARG_VALIDATION", "1")
    lib = lmdb_version_store_v1
    sym = "test_batch_read_and_join"
    lib.write(sym, pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)]))
    if env_var_set:
        lib.batch_read_and_join([sym], query_builder=QueryBuilder().concat(), not_a_kwarg=True)
    else:
        with pytest.raises(ArcticNativeException) as e:
            lib.batch_read_and_join([sym], query_builder=QueryBuilder().concat(), not_a_kwarg=True)
        msg = str(e.value)
        assert "batch_read_and_join" in msg and "not_a_kwarg" in msg


@pytest.mark.parametrize("env_var_set", [True, False])
def test_add_to_snapshot(lmdb_version_store_v1, monkeypatch, env_var_set):
    if env_var_set:
        monkeypatch.setenv("ARCTICDB_DISABLE_KWARG_VALIDATION", "1")
    lib = lmdb_version_store_v1
    sym_0 = "test_add_to_snapshot_0"
    lib.write(sym_0, pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)]))
    snap = "test_add_to_snapshot_snap"
    lib.snapshot(snap)
    sym_1 = "test_add_to_snapshot_1"
    lib.write(sym_1, pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)]))
    if env_var_set:
        lib.add_to_snapshot(snap, [sym_1], not_a_kwarg=True)
    else:
        with pytest.raises(ArcticNativeException) as e:
            lib.add_to_snapshot(snap, [sym_1], not_a_kwarg=True)
        msg = str(e.value)
        assert "add_to_snapshot" in msg and "not_a_kwarg" in msg
