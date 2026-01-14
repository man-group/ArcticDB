"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from pickle import loads, dumps
from arcticdb_ext import get_config_int, set_config_int


def test_config_roundtrip(version_store_factory):
    vs = version_store_factory(column_group_size=23, segment_row_size=42)

    dumped = dumps(vs)
    loaded = loads(dumped)
    assert loaded._cfg.write_options.column_group_size == 23
    assert loaded._cfg.write_options.segment_row_size == 42
    assert loaded.env == vs.env
    assert loaded._lib_cfg == vs._lib_cfg


def test_set_config_int():
    set_config_int("my_value", 25)
    assert get_config_int("my_value") == 25
