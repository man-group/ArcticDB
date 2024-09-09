"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from pickle import loads, dumps
from arcticdb.config import save_runtime_config, load_runtime_config
from arcticc.pb2.config_pb2 import RuntimeConfig
from arcticdb_ext import read_runtime_config, get_config_int, set_config_int


def test_config_roundtrip(version_store_factory):
    vs = version_store_factory(column_group_size=23, segment_row_size=42)

    dumped = dumps(vs)
    loaded = loads(dumped)
    assert loaded._cfg.write_options.column_group_size == 23
    assert loaded._cfg.write_options.segment_row_size == 42
    assert loaded.env == vs.env
    assert loaded._lib_cfg == vs._lib_cfg


def test_runtime_config_roundtrip(tmp_path):
    runtime_cfg = RuntimeConfig()
    runtime_cfg.string_values["string"] = "stuff"
    runtime_cfg.double_values["double"] = 2.5
    runtime_cfg.int_values["int"] = 11
    conf_path = str(tmp_path / "test_rt_conf.yaml")
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
