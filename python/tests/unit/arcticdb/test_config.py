"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb.version_store.helper import ArcticMemoryConfig, extract_lib_config
from arcticdb.version_store._store import NativeVersionStore
from pickle import loads, dumps
from arcticdb.config import save_runtime_config, load_runtime_config
from arcticc.pb2.config_pb2 import RuntimeConfig
from arcticdb_ext import read_runtime_config, get_config_int, set_config_int
from arcticdb_ext.storage import OpenMode as _OpenMode


def test_config_roundtrip(arcticdb_test_library_config):
    env = "default"
    arcticc = ArcticMemoryConfig(arcticdb_test_library_config(), env)
    lib_cfg = extract_lib_config(arcticc.env_cfg, "test.example")

    library = NativeVersionStore.create_lib_from_lib_config(lib_cfg, env, _OpenMode.DELETE)
    vs = NativeVersionStore(library, env, lib_cfg)
    assert vs._cfg.write_options.column_group_size == lib_cfg.lib_desc.version.write_options.column_group_size

    dumped = dumps(vs)
    loaded = loads(dumped)
    assert loaded._cfg.write_options.column_group_size == vs._cfg.write_options.column_group_size
    assert loaded._lib_cfg.lib_desc.description == vs._lib_cfg.lib_desc.description
    assert loaded.env == vs.env


def test_runtime_config_roundtrip(tmpdir):
    runtime_cfg = RuntimeConfig()
    runtime_cfg.string_values["string"] = "stuff"
    runtime_cfg.double_values["double"] = 2.5
    runtime_cfg.int_values["int"] = 11
    conf_path = "{}/{}".format(tmpdir, "test_rt_conf.yaml")
    save_runtime_config(runtime_cfg, conf_path)
    read_conf = load_runtime_config(conf_path)
    assert read_conf.string_values["string"] == "stuff"
    assert read_conf.double_values["double"] == 2.5
    assert read_conf.int_values["int"] == 11


def test_load_runtime_config():
    runtime_cfg = RuntimeConfig()
    runtime_cfg.int_values["NumThreads"] = 999999999
    read_runtime_config(runtime_cfg)
    num_threads = get_config_int("NumThreads")
    assert num_threads == 999999999


def test_set_config_int():
    set_config_int("my_value", 25)
    assert get_config_int("my_value") == 25
