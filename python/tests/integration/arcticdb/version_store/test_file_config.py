"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import sys

import pytest
from arcticdb.config import Defaults
from arcticdb.version_store.helper import add_lmdb_library_to_env, save_envs_config, get_arctic_native_lib
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap


def _make_temp_lmdb_lib(tmp_path, library_name):
    cfg_filename = "{}/{}".format(tmp_path, "test_cfg")
    cfg = EnvironmentConfigsMap()
    add_lmdb_library_to_env(
        cfg, lib_name=library_name, env_name=Defaults.ENV, description="a test library", db_dir=str(tmp_path)
    )
    save_envs_config(cfg, conf_path=cfg_filename)
    return "{}@{}".format(library_name, cfg_filename)


@pytest.mark.skipif(sys.platform == "win32", reason="SKIP_WIN Occasionally fails and unused outside of Man")
def test_native_lmdb_library(tmp_path):
    lib = get_arctic_native_lib(_make_temp_lmdb_lib(tmp_path, "test.file_config"))
    lib.write("symbol", "thing")
    assert lib.read("symbol").data == "thing"
