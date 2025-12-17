"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb.flattener import Flattener

from arcticdb.version_store._custom_normalizers import (
    register_normalizer,
    clear_registered_normalizers,
)

import numpy as np
import pandas as pd
from arcticdb.util.test import assert_frame_equal, assert_series_equal, CustomDictNormalizer, CustomDict

fl = Flattener()
separator = fl.SEPARATOR
base_sym = "sym"


def test_flattened_repr_simple():
    # This should be serializable as a primitive type.
    simple_dict = {"a": 1, "b": 2, "c": [5, 6]}
    simple_list = [5, 6]
    metast, f = fl.create_meta_structure(simple_dict, base_sym)
    assert len(f) == 0
    assert metast["leaf"] is True
    assert "key_name" not in metast

    metast, f = fl.create_meta_structure(simple_list, base_sym)
    assert len(f) == 0
    assert metast["leaf"] is True
    assert "key_name" not in metast


def test_flattened_repr_numpy():
    # dict with 3 leaf nodes.
    numpy_dict = {"a": np.arange(5), "b": np.arange(2), "c": {"d": {"e": np.arange(3)}}}
    _metast, flattened = fl.create_meta_structure(numpy_dict, base_sym)
    assert len(flattened) == 3
    np.testing.assert_array_equal(flattened["sym__a"], np.arange(5))
    np.testing.assert_array_equal(flattened["sym__b"], np.arange(2))
    np.testing.assert_array_equal(flattened["sym__c__d__e"], np.arange(3))


def test_compose_back_metast():
    samples = [
        {"a": np.arange(5), "b": np.arange(2), "c": {"d": {"e": np.arange(3)}}},
        [np.arange(5), np.arange(10)],
        {"a": {"b": {"c": {"d": {"e": pd.DataFrame({"col": [1]})}}}}},
        [{"a": 1}, {"b": np.arange(5)}, [1, 2, 3]],
    ]

    for sample in samples:
        meta, flattened = fl.create_meta_structure(sample, base_sym)
        assert fl.create_original_obj_from_metastruct(meta, dict(flattened)) == sample


def test_dict_like_structure():
    register_normalizer(CustomDictNormalizer())
    sample = CustomDict(
        {
            "a": pd.DataFrame({"col_dict": np.random.randn(2)}),
            "b": pd.DataFrame({"col_dict": np.random.randn(2)}),
            "c": CustomDict({"d": pd.DataFrame({"col_dict": np.random.randn(2)}), "e": [1, 2, 3]}),
        }
    )

    meta, flattened = fl.create_meta_structure(sample, "sym")

    recreated = fl.create_original_obj_from_metastruct(meta, flattened)
    assert type(recreated) == CustomDict
    clear_registered_normalizers()


def test_dict_record_keys():
    # We limit key length to 100. The "d" * 94 key will not be shortened (stored as sym__ddd...) whereas the "e" * 95 key
    # trips the threshold and will be shortened (due to the 5 characters in sym__).
    sample = {
        "a": pd.DataFrame({"col_dict": np.random.randn(2)}),
        "b": {"c": pd.DataFrame({"col_dict": np.random.randn(2)})},
        "d" * 94: pd.DataFrame({"col_dict": np.random.rand(2)}),
        "e" * 95: pd.DataFrame({"col_dict": np.random.rand(2)}),  # key name should be obfuscated for this one
    }

    meta, flattened = fl.create_meta_structure(sample, "sym")
    sub_keys = meta["sub_keys"]
    assert len(sub_keys) == 4
    assert sub_keys[0]["leaf"]
    assert sub_keys[0]["key_name"] == "sym__a"

    assert not sub_keys[1]["leaf"]
    assert "key_name" not in sub_keys[1]
    nested_subkeys = sub_keys[1]["sub_keys"]
    assert nested_subkeys[0]["leaf"]
    assert nested_subkeys[0]["key_name"] == "sym__b__c"
    assert nested_subkeys[0]["symbol"] == "sym__b__c"

    assert sub_keys[2]["leaf"]
    assert sub_keys[2]["key_name"] == f"sym__{'d' * 94}"
    assert sub_keys[2]["symbol"] == f"sym__{'d' * 94}"

    assert sub_keys[3]["leaf"]
    assert sub_keys[3]["key_name"] == f"sym_eee_583539213776"
    assert sub_keys[3]["symbol"] == f"sym__{'e' * 95}"


def test_pandas_series(lmdb_version_store, all_recursive_metastructure_versions):
    test_data = [pd.Series(["hello", "good morning"])]
    meta, flattened = fl.create_meta_structure(test_data, "sym")

    assert fl.create_original_obj_from_metastruct(meta, flattened)[0].equals(test_data[0])

    lib = lmdb_version_store
    lib.write("sym", test_data, recursive_normalizers=True)
    assert_series_equal(lib.read("sym").data[0], test_data[0])


def test_multiindex_recursive_normalizer(lmdb_version_store, all_recursive_metastructure_versions):
    dtidx = pd.date_range(pd.Timestamp("2016-01-01"), periods=2)
    vals = ["a", "b", "c"]
    df = pd.DataFrame(data=np.arange(6), index=pd.MultiIndex.from_product([dtidx, vals], names=["a", "b"]))
    lmdb_version_store.write("df", {"data": df}, recursive_normalizers=True)
    assert_frame_equal(lmdb_version_store.read("df").data["data"], df)
