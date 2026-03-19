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

# "warn" = env var set to "1" (explicit opt-in to warning)
# "warn_default" = env var not set (default is now "1", so warns)
# "error" = env var set to "0" (opt-in to error)
VALIDATION_MODES = ["warn", "warn_default", "error"]


def _setup_env_var(monkeypatch, mode):
    if mode == "warn":
        monkeypatch.setenv("ARCTICDB_DISABLE_KWARG_VALIDATION", "1")
    elif mode == "warn_default":
        monkeypatch.delenv("ARCTICDB_DISABLE_KWARG_VALIDATION", raising=False)
    elif mode == "error":
        monkeypatch.setenv("ARCTICDB_DISABLE_KWARG_VALIDATION", "0")


@pytest.mark.parametrize("method", ["stage", "write", "append", "update", "batch_write", "batch_append"])
@pytest.mark.parametrize("mode", VALIDATION_MODES)
def test_modification_methods(lmdb_version_store_v1, monkeypatch, method, mode):
    _setup_env_var(monkeypatch, mode)
    lib = lmdb_version_store_v1
    sym = "test_modification_methods"
    df = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    # To avoid non-existent symbol errors on updates
    lib.write(sym, df)
    f = getattr(lib, method)
    arg_0 = [sym] if method.startswith("batch_") else sym
    arg_1 = [df] if method.startswith("batch_") else df
    if mode == "error":
        with pytest.raises(ArcticNativeException) as e:
            f(arg_0, arg_1, not_a_kwarg=True)
        msg = str(e.value)
        assert method in msg and "not_a_kwarg" in msg
    else:
        f(arg_0, arg_1, not_a_kwarg=True)


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
@pytest.mark.parametrize("mode", VALIDATION_MODES)
def test_single_argument_methods(lmdb_version_store_v1, monkeypatch, method, mode):
    _setup_env_var(monkeypatch, mode)
    lib = lmdb_version_store_v1
    sym = "test_read_methods"
    df = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    lib.write(sym, df)
    # For defrag
    lib.append(sym, df)
    f = getattr(lib, method)
    arg_0 = [sym] if method.startswith("batch_") else sym
    with config_context("SymbolDataCompact.SegmentCount", 1):
        if mode == "error":
            with pytest.raises(ArcticNativeException) as e:
                f(arg_0, not_a_kwarg=True)
            msg = str(e.value)
            assert method in msg and "not_a_kwarg" in msg
        else:
            f(arg_0, not_a_kwarg=True)


@pytest.mark.parametrize("mode", VALIDATION_MODES)
def test_batch_read_and_join(lmdb_version_store_v1, monkeypatch, mode):
    _setup_env_var(monkeypatch, mode)
    lib = lmdb_version_store_v1
    sym = "test_batch_read_and_join"
    lib.write(sym, pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)]))
    if mode == "error":
        with pytest.raises(ArcticNativeException) as e:
            lib.batch_read_and_join([sym], query_builder=QueryBuilder().concat(), not_a_kwarg=True)
        msg = str(e.value)
        assert "batch_read_and_join" in msg and "not_a_kwarg" in msg
    else:
        lib.batch_read_and_join([sym], query_builder=QueryBuilder().concat(), not_a_kwarg=True)


@pytest.mark.parametrize("mode", VALIDATION_MODES)
def test_add_to_snapshot(lmdb_version_store_v1, monkeypatch, mode):
    _setup_env_var(monkeypatch, mode)
    lib = lmdb_version_store_v1
    sym_0 = "test_add_to_snapshot_0"
    lib.write(sym_0, pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)]))
    snap = "test_add_to_snapshot_snap"
    lib.snapshot(snap)
    sym_1 = "test_add_to_snapshot_1"
    lib.write(sym_1, pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)]))
    if mode == "error":
        with pytest.raises(ArcticNativeException) as e:
            lib.add_to_snapshot(snap, [sym_1], not_a_kwarg=True)
        msg = str(e.value)
        assert "add_to_snapshot" in msg and "not_a_kwarg" in msg
    else:
        lib.add_to_snapshot(snap, [sym_1], not_a_kwarg=True)
