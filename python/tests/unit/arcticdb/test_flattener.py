"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb.flattener import Flattener

from arcticc.pb2.descriptors_pb2 import NormalizationMetadata
from arcticdb.version_store._custom_normalizers import (
    CustomNormalizer,
    register_normalizer,
    clear_registered_normalizers,
)

import numpy as np
import pandas as pd
from arcticdb.util.test import assert_frame_equal, assert_series_equal

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

    metast, f = fl.create_meta_structure(simple_list, base_sym)
    assert len(f) == 0
    assert metast["leaf"] is True


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
        assert fl.create_original_obj_from_metastruct_new(meta, dict(flattened)) == sample


def test_dict_like_structure():
    # InteractiveData like structure.
    class CustomDict(dict):
        pass

    class CustomDictNormalizer(CustomNormalizer):
        NESTED_STRUCTURE = True

        def normalize(self, item, **kwargs):
            if not isinstance(item, CustomDict):
                return None
            return dict(item), NormalizationMetadata.CustomNormalizerMeta()

        def denormalize(self, item, norm_meta):
            return CustomDict(item)

    register_normalizer(CustomDictNormalizer())
    sample = CustomDict(
        {
            "a": pd.DataFrame({"col_dict": np.random.randn(2)}),
            "b": pd.DataFrame({"col_dict": np.random.randn(2)}),
            "c": CustomDict({"d": pd.DataFrame({"col_dict": np.random.randn(2)}), "e": [1, 2, 3]}),
        }
    )

    meta, flattened = fl.create_meta_structure(sample, "sym")

    recreated = fl.create_original_obj_from_metastruct_new(meta, flattened)
    assert type(recreated) == CustomDict
    clear_registered_normalizers()


def test_pandas_series(lmdb_version_store):
    test_data = [pd.Series(["hello", "good morning"])]
    meta, flattened = fl.create_meta_structure(test_data, "sym")

    assert fl.create_original_obj_from_metastruct_new(meta, flattened)[0].equals(test_data[0])

    lib = lmdb_version_store
    lib.write("sym", test_data, recursive_normalizers=True)
    assert_series_equal(lib.read("sym").data[0], test_data[0])


def test_multiindex_recursive_normalizer(lmdb_version_store):
    dtidx = pd.date_range(pd.Timestamp("2016-01-01"), periods=2)
    vals = ["a", "b", "c"]
    df = pd.DataFrame(data=np.arange(6), index=pd.MultiIndex.from_product([dtidx, vals], names=["a", "b"]))
    lmdb_version_store.write("df", {"data": df}, recursive_normalizers=True)
    assert_frame_equal(lmdb_version_store.read("df").data["data"], df)
