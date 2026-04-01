"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from pickle import loads, dumps
from arcticdb_ext import (
    get_config_int,
    set_config_int,
    unset_config_int,
    get_config_string,
    set_config_string,
    unset_config_string,
    get_config_double,
    set_config_double,
    unset_config_double,
    get_all_config_int,
    set_all_config_int,
    get_all_config_string,
    set_all_config_string,
    get_all_config_double,
    set_all_config_double,
)


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


def test_get_all_set_all_config_roundtrip():
    """Bulk export/import round-trip for all ConfigsMap types."""
    set_config_int("BulkTestInt_A", 100)
    set_config_int("BulkTestInt_B", 200)
    set_config_string("BulkTestStr_X", "hello")
    set_config_double("BulkTestDbl_Y", 3.14)
    try:
        # Export
        all_ints = get_all_config_int()
        all_strings = get_all_config_string()
        all_doubles = get_all_config_double()

        assert all_ints["BULKTESTINT_A"] == 100
        assert all_ints["BULKTESTINT_B"] == 200
        assert all_strings["BULKTESTSTR_X"] == "hello"
        assert abs(all_doubles["BULKTESTDBL_Y"] - 3.14) < 1e-9

        # Clear the values we set
        unset_config_int("BulkTestInt_A")
        unset_config_int("BulkTestInt_B")
        unset_config_string("BulkTestStr_X")
        unset_config_double("BulkTestDbl_Y")

        assert get_config_int("BulkTestInt_A") is None
        assert get_config_string("BulkTestStr_X") is None
        assert get_config_double("BulkTestDbl_Y") is None

        # Re-import from the saved snapshot
        set_all_config_int(all_ints)
        set_all_config_string(all_strings)
        set_all_config_double(all_doubles)

        assert get_config_int("BulkTestInt_A") == 100
        assert get_config_int("BulkTestInt_B") == 200
        assert get_config_string("BulkTestStr_X") == "hello"
        assert abs(get_config_double("BulkTestDbl_Y") - 3.14) < 1e-9
    finally:
        unset_config_int("BulkTestInt_A")
        unset_config_int("BulkTestInt_B")
        unset_config_string("BulkTestStr_X")
        unset_config_double("BulkTestDbl_Y")


def test_config_preserved_in_pickle_roundtrip(version_store_factory):
    """ConfigsMap values survive a pickle dumps/loads cycle via NativeVersionStore."""
    set_config_int("PickleTestKey", 42)
    try:
        vs = version_store_factory()
        dumped = dumps(vs)
        # Clear before loading to prove the value is restored from the pickle payload
        unset_config_int("PickleTestKey")
        assert get_config_int("PickleTestKey") is None

        loaded = loads(dumped)
        assert get_config_int("PickleTestKey") == 42
    finally:
        unset_config_int("PickleTestKey")


def test_config_preserved_in_library_pickle_roundtrip(lmdb_library):
    """ConfigsMap values survive a pickle dumps/loads cycle via Library."""
    set_config_int("LibPickleTestKey", 99)
    try:
        dumped = dumps(lmdb_library)
        unset_config_int("LibPickleTestKey")
        assert get_config_int("LibPickleTestKey") is None

        loaded = loads(dumped)
        assert get_config_int("LibPickleTestKey") == 99
        # DevTools._nvs must be the same object as Library._nvs after unpickling
        assert loaded._dev_tools._nvs is loaded._nvs
    finally:
        unset_config_int("LibPickleTestKey")


def test_pickle_restores_skip_df_consolidation(lmdb_library):
    """skip_df_consolidation flag must be preserved through pickle."""
    # Library.__init__ sets the flag to True (on Pandas 2+); without the pickle
    # fix, _initialize would reset it to False based on the env var default.
    nvs = lmdb_library._nvs
    nvs._normalizer.df._skip_df_consolidation = True
    loaded_nvs = loads(dumps(nvs))
    assert loaded_nvs._normalizer.df._skip_df_consolidation is True
