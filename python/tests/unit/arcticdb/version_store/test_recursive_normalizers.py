import pandas as pd
import pytest
import numpy as np
import arcticdb
from arcticdb.util.test import equals
from arcticdb.flattener import Flattener
from arcticdb.version_store._custom_normalizers import CustomNormalizer, register_normalizer
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata  # Importing from arcticdb dynamically loads arcticc.pb2
from arcticdb.exceptions import ArcticDbNotYetImplemented


class AlmostAList(list):
    pass


class AlmostAListNormalizer(CustomNormalizer):
    NESTED_STRUCTURE = True

    def normalize(self, item, **kwargs):
        if not isinstance(item, AlmostAList):
            return None
        return list(item), NormalizationMetadata.CustomNormalizerMeta()

    def denormalize(self, item, norm_meta):
        return AlmostAList(item)


@pytest.mark.parametrize("batch", (True, False))
def test_recursively_written_data(basic_store, batch):
    samples = [
        {"a": np.arange(5), "b": np.arange(8)},  # dict of np arrays
        (np.arange(5), np.arange(6)),  # tuple of np arrays
        [np.arange(9), np.arange(12), (1, 2)],  # list of numpy arrays and a python tuple
        ({"a": np.arange(5), "b": [1, 2, 3]}),  # dict of np arrays and a python list
    ]

    for idx, sample in enumerate(samples):
        recursive_sym = "sym_recursive" + str(idx)
        pickled_sym = "sym_pickled" + str(idx)
        basic_store.write(recursive_sym, sample, recursive_normalizers=True)
        basic_store.write(pickled_sym, sample)  # pickled writes
        if batch:
            recursive_vit = basic_store.batch_read([recursive_sym])[recursive_sym]
            pickled_vit = basic_store.batch_read([pickled_sym])[pickled_sym]
        else:
            recursive_vit = basic_store.read(recursive_sym)
            pickled_vit = basic_store.read(pickled_sym)
        equals(sample, recursive_vit.data)
        equals(pickled_vit.data, recursive_vit.data)
        assert recursive_vit.symbol == recursive_sym
        assert pickled_vit.symbol == pickled_sym


@pytest.mark.parametrize("batch", (True, False))
def test_recursively_written_data_with_metadata(basic_store, batch):
    samples = [
        {"a": np.arange(5), "b": np.arange(8)},  # dict of np arrays
        (np.arange(5), np.arange(6)),  # tuple of np arrays
    ]

    for idx, sample in enumerate(samples):
        sym = "sym_recursive" + str(idx)
        metadata = {"something": 1}
        basic_store.write(sym, sample, metadata=metadata, recursive_normalizers=True)
        if batch:
            vit = basic_store.batch_read([sym])[sym]
        else:
            vit = basic_store.read(sym)
        equals(sample, vit.data)
        assert vit.symbol == sym
        assert vit.metadata == metadata


@pytest.mark.parametrize("batch", (True, False))
def test_recursively_written_data_with_nones(basic_store, batch):
    sample = {"a": np.arange(5), "b": np.arange(8), "c": None}
    recursive_sym = "sym_recursive"
    pickled_sym = "sym_pickled"
    basic_store.write(recursive_sym, sample, recursive_normalizers=True)
    basic_store.write(pickled_sym, sample)  # pickled writes
    if batch:
        recursive_vit = basic_store.batch_read([recursive_sym])[recursive_sym]
        pickled_vit = basic_store.batch_read([pickled_sym])[pickled_sym]
    else:
        recursive_vit = basic_store.read(recursive_sym)
        pickled_vit = basic_store.read(pickled_sym)
    equals(sample, recursive_vit.data)
    equals(pickled_vit.data, recursive_vit.data)
    assert recursive_vit.symbol == recursive_sym
    assert pickled_vit.symbol == pickled_sym


@pytest.mark.parametrize("batch", (True, False))
def test_recursive_nested_data(basic_store, batch):
    sym = "test_recursive_nested_data"
    sample_data = {"a": {"b": {"c": {"d": np.arange(24)}}}}
    fl = Flattener()
    assert fl.can_flatten(sample_data)
    assert fl.is_dict_like(sample_data)
    metast, to_write = fl.create_meta_structure(sample_data, "sym")
    assert len(to_write) == 1
    equals(list(to_write.values())[0], np.arange(24))

    basic_store.write(sym, sample_data, recursive_normalizers=True)
    if batch:
        vit = basic_store.batch_read([sym])[sym]
    else:
        vit = basic_store.read(sym)
    equals(vit.data, sample_data)
    assert vit.symbol == sym

def test_recursive_normalizer_with_custom_class():
    list_like_obj = AlmostAList([1, 2, 3])
    fl = Flattener()
    assert not fl.is_normalizable_to_nested_structure(list_like_obj)  # normalizer not registered yet

    register_normalizer(AlmostAListNormalizer())
    # Should be normalizable now.
    fl = Flattener()
    assert fl.is_normalizable_to_nested_structure(list_like_obj)

def test_nested_custom_types(basic_store):
    data = AlmostAList([1, 2, 3, AlmostAList([5, np.arange(6)])])
    fl = Flattener()
    meta, to_write = fl.create_meta_structure(data, "sym")
    equals(list(to_write.values())[0], np.arange(6))
    basic_store.write("sym", data, recursive_normalizers=True)
    got_back = basic_store.read("sym").data
    assert isinstance(got_back, AlmostAList)
    assert isinstance(got_back[3], AlmostAList)
    assert got_back[0] == 1

def test_data_directly_msgpackable(basic_store):
    data = {"a": [1, 2, 3], "b": {"c": 5}}
    fl = Flattener()
    meta, to_write = fl.create_meta_structure(data, "sym")
    assert len(to_write) == 0
    assert meta["leaf"] is True
    basic_store.write("s", data, recursive_normalizers=True)
    equals(basic_store.read("s").data, data)


@pytest.mark.parametrize("batch", (True, False))
def test_really_large_symbol_for_recursive_data(basic_store, batch):
    sym = "s" * 100
    data = {"a" * 100: {"b" * 100: {"c" * 1000: {"d": np.arange(5)}}}}
    basic_store.write(sym, data, recursive_normalizers=True)
    fl = Flattener()
    metastruct, to_write = fl.create_meta_structure(data, "s" * 100)
    assert len(list(to_write.keys())[0]) < fl.MAX_KEY_LENGTH
    if batch:
        vit = basic_store.batch_read([sym])[sym]
    else:
        vit = basic_store.read(sym)
    equals(vit.data, data)
    assert vit.symbol == sym


def test_too_much_recursive_metastruct_data(monkeypatch, lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_too_much_recursive_metastruct_data"
    data = [pd.DataFrame({"col": [0]}), pd.DataFrame({"col": [1]})]
    with pytest.raises(ArcticDbNotYetImplemented) as e:
        with monkeypatch.context() as m:
            m.setattr(arcticdb.version_store._normalization, "_MAX_RECURSIVE_METASTRUCT", 1)
            lib.write(sym, data, recursive_normalizers=True)
    assert "recursive" in str(e.value).lower()

def test_simple_recursive_normalizer(object_version_store):
    object_version_store.write(
        "rec_norm", data={"a": np.arange(5), "b": np.arange(8), "c": None}, recursive_normalizers=True
    )