import pandas as pd
import pytest
import numpy as np
import arcticdb
from arcticdb.util.test import equals
from arcticdb.flattener import Flattener
from arcticdb.version_store._custom_normalizers import CustomNormalizer, register_normalizer
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata  # Importing from arcticdb dynamically loads arcticc.pb2
from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb.util.venv import CurrentVersion
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.storage import KeyType


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


def assert_vit_equals_except_data(left, right):
    """
    Checks if two VersionedItem objects are equal disregarding differences in the data field. This is because when a
    write is performed the returned VersionedItem does not contain a data field.
    """
    assert left.symbol == right.symbol
    assert left.library == right.library
    assert left.version == right.version
    assert left.metadata == right.metadata
    assert left.host == right.host
    assert left.timestamp == right.timestamp

@pytest.mark.parametrize("read", (lambda lib, sym: lib.batch_read([sym])[sym], lambda lib, sym: lib.read(sym)))
@pytest.mark.storage
def test_recursively_written_data(basic_store, read):
    samples = [
        {"a": np.arange(5), "b": np.arange(8)},  # dict of np arrays
        (np.arange(5), np.arange(6)),  # tuple of np arrays
        [np.arange(9), np.arange(12), (1, 2)],  # list of numpy arrays and a python tuple
        ({"a": np.arange(5), "b": [1, 2, 3]}),  # dict of np arrays and a python list
    ]

    for idx, sample in enumerate(samples):
        recursive_sym = "sym_recursive" + str(idx)
        pickled_sym = "sym_pickled" + str(idx)
        recursive_write_vit = basic_store.write(recursive_sym, sample, recursive_normalizers=True)
        pickled_write_vit = basic_store.write(pickled_sym, sample)  # pickled writes
        recursive_vit = read(basic_store, recursive_sym)
        pickled_vit = read(basic_store, pickled_sym)

        equals(sample, recursive_vit.data)
        equals(pickled_vit.data, recursive_vit.data)
        assert recursive_vit.symbol == recursive_sym
        assert pickled_vit.symbol == pickled_sym
        assert_vit_equals_except_data(recursive_write_vit, recursive_vit)
        assert_vit_equals_except_data(pickled_write_vit, pickled_vit)
        assert basic_store.get_info(recursive_sym)["type"] != "pickled"
        assert basic_store.get_info(pickled_sym)["type"] == "pickled"



@pytest.mark.parametrize("read", (lambda lib, sym: lib.batch_read([sym])[sym], lambda lib, sym: lib.read(sym)))
@pytest.mark.storage
def test_recursively_written_data_with_metadata(basic_store, read):
    samples = [
        {"a": np.arange(5), "b": np.arange(8)},  # dict of np arrays
        (np.arange(5), np.arange(6)),  # tuple of np arrays
    ]

    for idx, sample in enumerate(samples):
        sym = "sym_recursive" + str(idx)
        metadata = {"something": 1}
        write_vit = basic_store.write(sym, sample, metadata=metadata, recursive_normalizers=True)
        read_vit = read(basic_store, sym)

        equals(sample, read_vit.data)
        assert read_vit.symbol == sym
        assert read_vit.metadata == metadata
        assert_vit_equals_except_data(read_vit, write_vit)
        assert basic_store.get_info(sym)["type"] != "pickled"


@pytest.mark.parametrize("read", (lambda lib, sym: lib.batch_read([sym])[sym], lambda lib, sym: lib.read(sym)))
@pytest.mark.storage
def test_recursively_written_data_with_nones(basic_store, read):
    sample = {"a": np.arange(5), "b": np.arange(8), "c": None}
    recursive_sym = "sym_recursive"
    pickled_sym = "sym_pickled"
    recursive_write_vit = basic_store.write(recursive_sym, sample, recursive_normalizers=True)
    pickled_write_vit = basic_store.write(pickled_sym, sample)  # pickled writes
    recursive_read_vit = read(basic_store, recursive_sym)
    pickled_read_vit = read(basic_store, pickled_sym)

    equals(sample, recursive_read_vit.data)
    equals(pickled_read_vit.data, recursive_read_vit.data)
    assert recursive_read_vit.symbol == recursive_sym
    assert pickled_read_vit.symbol == pickled_sym
    assert_vit_equals_except_data(recursive_write_vit, recursive_read_vit)
    assert_vit_equals_except_data(pickled_write_vit, pickled_read_vit)
    assert basic_store.get_info(recursive_sym)["type"] != "pickled"


@pytest.mark.parametrize("read", (lambda lib, sym: lib.batch_read([sym])[sym], lambda lib, sym: lib.read(sym)))
@pytest.mark.storage
def test_recursive_nested_data(basic_store, read):
    sym = "test_recursive_nested_data"
    sample_data = {"a": {"b": {"c": {"d": np.arange(24)}}}}
    fl = Flattener()
    assert fl.can_flatten(sample_data)
    assert fl.is_dict_like(sample_data)
    metast, to_write = fl.create_meta_structure(sample_data, "sym")
    assert len(to_write) == 1
    equals(list(to_write.values())[0], np.arange(24))

    write_vit = basic_store.write(sym, sample_data, recursive_normalizers=True)
    read_vit = read(basic_store, sym)
    equals(read_vit.data, sample_data)
    assert read_vit.symbol == sym
    assert_vit_equals_except_data(read_vit, write_vit)

@pytest.mark.storage
def test_recursive_normalizer_with_custom_class():
    list_like_obj = AlmostAList([1, 2, 3])
    fl = Flattener()
    assert not fl.is_normalizable_to_nested_structure(list_like_obj)  # normalizer not registered yet

    register_normalizer(AlmostAListNormalizer())
    # Should be normalizable now.
    fl = Flattener()
    assert fl.is_normalizable_to_nested_structure(list_like_obj)

@pytest.mark.storage
def test_nested_custom_types(basic_store):
    data = AlmostAList([1, 2, 3, AlmostAList([5, np.arange(6)])])
    fl = Flattener()
    sym = "sym"
    meta, to_write = fl.create_meta_structure(data, sym)
    equals(list(to_write.values())[0], np.arange(6))
    write_vit = basic_store.write(sym, data, recursive_normalizers=True)
    read_vit = basic_store.read(sym)
    got_back = read_vit.data
    assert isinstance(got_back, AlmostAList)
    assert isinstance(got_back[3], AlmostAList)
    assert got_back[0] == 1
    assert_vit_equals_except_data(write_vit, read_vit)
    assert basic_store.get_info(sym)["type"] != "pickled"

@pytest.mark.storage
def test_data_directly_msgpackable(basic_store):
    data = {"a": [1, 2, 3], "b": {"c": 5}}
    fl = Flattener()
    meta, to_write = fl.create_meta_structure(data, "s")
    assert len(to_write) == 0
    assert meta["leaf"] is True
    write_vit = basic_store.write("s", data, recursive_normalizers=True)
    read_vit = basic_store.read("s")
    equals(read_vit.data, data)
    assert_vit_equals_except_data(write_vit, read_vit)
    assert basic_store.get_info("s")["type"] == "pickled"


@pytest.mark.parametrize("read", (lambda lib, sym: lib.batch_read([sym])[sym], lambda lib, sym: lib.read(sym)))
@pytest.mark.storage
def test_really_large_symbol_for_recursive_data(basic_store, read):
    sym = "s" * 100
    data = {"a" * 100: {"b" * 100: {"c" * 1000: {"d": np.arange(5)}}}}
    write_vit = basic_store.write(sym, data, recursive_normalizers=True)
    fl = Flattener()
    metastruct, to_write = fl.create_meta_structure(data, "s" * 100)
    assert len(list(to_write.keys())[0]) < fl.MAX_KEY_LENGTH
    read_vit = read(basic_store, sym)
    equals(read_vit.data, data)
    assert read_vit.symbol == sym
    assert_vit_equals_except_data(write_vit, read_vit)
    assert basic_store.get_info(sym)["type"] != "pickled"


def test_too_much_recursive_metastruct_data(monkeypatch, lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_too_much_recursive_metastruct_data"
    data = [pd.DataFrame({"col": [0]}), pd.DataFrame({"col": [1]})]
    with pytest.raises(ArcticDbNotYetImplemented) as e:
        with monkeypatch.context() as m:
            m.setattr(arcticdb.version_store._normalization, "_MAX_RECURSIVE_METASTRUCT", 1)
            lib.write(sym, data, recursive_normalizers=True)
    assert "recursive" in str(e.value).lower()

class TestRecursiveNormalizersCompat:
    def test_compat_write_old_read_new(self, old_venv_and_arctic_uri, lib_name):
        old_venv, arctic_uri = old_venv_and_arctic_uri
        old_ac = old_venv.create_arctic(arctic_uri)
        old_lib = old_ac.create_library(lib_name)
        dfs = {"df_1": pd.DataFrame({"a": [1, 2, 3]}), "df_2": pd.DataFrame({"b": ["a", "b"]})}
        sym = "sym"
        old_lib.execute([
            f"""
from arcticdb_ext.storage import KeyType
lib._nvs.write('sym', {{"a": df_1, "b": df_2}}, recursive_normalizers=True, pickle_on_failure=True)
lib_tool = lib._nvs.library_tool()
assert len(lib_tool.find_keys_for_symbol(KeyType.MULTI_KEY, 'sym')) == 1
            """
        ], dfs=dfs)

        with CurrentVersion(arctic_uri, lib_name) as curr:
            data = curr.lib.read(sym).data
            expected = {"a": dfs["df_1"], "b": dfs["df_2"]}
            assert set(data.keys()) == set(expected.keys())
            for key in data.keys():
                assert_frame_equal(data[key], expected[key])

    def test_write_new_read_old(self, old_venv_and_arctic_uri, lib_name):
        old_venv, arctic_uri = old_venv_and_arctic_uri
        old_ac = old_venv.create_arctic(arctic_uri)
        old_lib = old_ac.create_library(lib_name)
        dfs = {"df_1": pd.DataFrame({"a": [1, 2, 3]}), "df_2": pd.DataFrame({"b": ["a", "b"]})}
        with CurrentVersion(arctic_uri, lib_name) as curr:
            curr.lib._nvs.write('sym', {"a": dfs["df_1"], "b": dfs["df_2"]}, recursive_normalizers=True, pickle_on_failure=True)
            lib_tool = curr.lib._nvs.library_tool()
            assert len(lib_tool.find_keys_for_symbol(KeyType.MULTI_KEY, 'sym')) == 1

        old_lib.execute([
            """
from pandas.testing import assert_frame_equal
data = lib.read('sym').data
expected = {'a': df_1, 'b': df_2}
assert set(data.keys()) == set(expected.keys())
for key in data.keys():
    assert_frame_equal(data[key], expected[key])
            """
        ], dfs=dfs)