"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pytest
import pandas as pd

from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb.version_store.library import WritePayload, ArcticUnsupportedDataTypeException
from arcticdb.util.test import assert_frame_equal


@pytest.mark.parametrize("method", ["write", "batch_write"])
@pytest.mark.parametrize("lib_config", [True, False])
@pytest.mark.parametrize("env_var_1", [True, False])
@pytest.mark.parametrize("env_var_2", [True, False])
@pytest.mark.parametrize("arg", [True, False, None])
def test_v1_api(version_store_factory, monkeypatch, method, lib_config, env_var_1, env_var_2, arg):
    lib = version_store_factory(use_norm_failure_handler_known_types=lib_config)
    assert lib.lib_cfg().lib_desc.version.use_norm_failure_handler_known_types == lib_config
    sym = "test_v1_api"
    should_succeed = lib_config
    if env_var_1:
        monkeypatch.setenv("USE_NORM_FAILURE_HANDLER_KNOWN_TYPES", "true")
        should_succeed = True
    if env_var_2:
        monkeypatch.setenv("PICKLE_ON_FAILURE", "true")
        should_succeed = True
    if arg is not None:
        should_succeed = arg
    assert should_succeed == lib._resolve_pickle_on_failure(arg)

    # Normalizable type with non-normalizable object column type
    # This is the case pickle_on_failure controls
    df = pd.DataFrame({"col": [pd.DataFrame()]})
    arg_0 = [sym] if method == "batch_write" else sym
    arg_1 = [df] if method == "batch_write" else df
    write_method = getattr(lib, method)
    if should_succeed:
        write_method(arg_0, arg_1, pickle_on_failure=arg)
        assert lib.is_symbol_pickled(sym)
        assert_frame_equal(df, lib.read(sym).data)
    else:
        with pytest.raises(ArcticDbNotYetImplemented):
            write_method(arg_0, arg_1, pickle_on_failure=arg)

    # Arbitrary data normalizable directly with msgpack should always succeed regardless of this setting
    data = {"hello": "world"}
    arg_1 = [data] if method == "batch_write" else data
    write_method(arg_0, arg_1, pickle_on_failure=arg)
    assert lib.read(sym).data == data


@pytest.mark.parametrize("method", ["write", "write_pickle", "write_batch", "write_pickle_batch"])
def test_v2_api(lmdb_library, method):
    lib = lmdb_library
    assert not lib._nvs.lib_cfg().lib_desc.version.use_norm_failure_handler_known_types
    sym = "test_v2_api"
    should_succeed = "pickle" in method

    # Normalizable type with non-normalizable object column type
    df = pd.DataFrame({"col": [pd.DataFrame()]})
    write_payload = WritePayload(sym, df)
    write_method = getattr(lib, method)
    if should_succeed:
        write_method([write_payload]) if "batch" in method else write_method(*write_payload)
        assert lib._nvs.is_symbol_pickled(sym)
        assert_frame_equal(df, lib.read(sym).data)
    else:
        with pytest.raises(ArcticDbNotYetImplemented):
            write_method([write_payload]) if "batch" in method else write_method(*write_payload)

    # Arbitrary data normalizable directly with msgpack should only succeed with write_pickle[_batch]
    data = {"hello": "world"}
    write_payload = WritePayload(sym, data)
    if should_succeed:
        write_method([write_payload]) if "batch" in method else write_method(*write_payload)
        assert lib._nvs.is_symbol_pickled(sym)
        assert lib.read(sym).data == data
    else:
        with pytest.raises(ArcticUnsupportedDataTypeException):
            write_method([write_payload]) if "batch" in method else write_method(*write_payload)
