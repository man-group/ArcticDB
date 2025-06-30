import datetime
import string
from collections import namedtuple
import random

import pandas as pd
import pytest
import numpy as np
import arcticdb
from arcticdb import QueryBuilder
from arcticdb.util.test import equals
from arcticdb.flattener import Flattener
from arcticdb.version_store._custom_normalizers import CustomNormalizer, register_normalizer
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata  # Importing from arcticdb dynamically loads arcticc.pb2
from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb.util.venv import CompatLibrary
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.exceptions import UserInputException
from arcticdb_ext.storage import KeyType
from arcticdb_ext.version_store import NoSuchVersionException

from tests.util.mark import MACOS_WHEEL_BUILD

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
        assert recursive_write_vit.version == 0
        pickled_write_vit = basic_store.write(pickled_sym, sample)  # pickled writes
        recursive_vit = read(basic_store, recursive_sym)
        pickled_vit = read(basic_store, pickled_sym)

        equals(sample, recursive_vit.data)
        equals(pickled_vit.data, recursive_vit.data)
        assert recursive_vit.symbol == recursive_sym
        assert pickled_vit.symbol == pickled_sym
        assert recursive_vit.version == 0
        assert_vit_equals_except_data(recursive_write_vit, recursive_vit)
        assert_vit_equals_except_data(pickled_write_vit, pickled_vit)
        assert basic_store.get_info(recursive_sym)["type"] != "pickled"
        assert basic_store.get_info(pickled_sym)["type"] == "pickled"

        recursive_write_vit = basic_store.write(recursive_sym, sample, recursive_normalizers=True)
        assert recursive_write_vit.version == 1
        recursive_vit = read(basic_store, recursive_sym)
        assert recursive_vit.version == 1


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

        metadata = {"something": 2, "else": 1}
        write_vit = basic_store.write(sym, sample, metadata=metadata, recursive_normalizers=True)
        assert write_vit.version == 1
        assert write_vit.metadata == metadata
        read_vit = read(basic_store, sym)
        assert read_vit.symbol == sym
        assert read_vit.metadata == metadata
        assert read_vit.version == 1


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


def test_data_that_can_be_serialized_already(lmdb_version_store_v1):
    """Check that we save data normally when possible even if `recursive_normalizers` is True`"""
    # Given
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"col": [0]})

    # When
    lib.write("sym", df, recursive_normalizers=True)

    # Then
    assert lib.get_info("sym")["type"] == "pandasdf"
    assert_frame_equal(lib.read("sym").data, df)
    lt = lib.library_tool()
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 0


@pytest.mark.parametrize("pickle_on_failure", (True, False))
@pytest.mark.parametrize("type", ("dict", "list"))
def test_recursive_normalizers_not_set(lmdb_version_store_v1, type, pickle_on_failure):
    """Check what happens when we need recursive normalizers but it is not set."""
    # Given
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"col": [0]})
    if type == "dict":
        data = {"key": df}
    else:
        assert type == "list"
        data = [df]

    # When
    lib.write("sym", data, recursive_normalizers=False, pickle_on_failure=pickle_on_failure)

    # Then
    assert lib.get_info("sym")["type"] == "pickled"  # pickle_on_failure=False not respected: Monday 8083916814

    result = lib.read("sym").data
    if type == "dict":
        assert_frame_equal(result["key"], df)
    else:
        assert len(result) == 1
        assert_frame_equal(result[0], df)

    lt = lib.library_tool()
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 0


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


def test_nesting(lmdb_version_store_v1):
    # Given
    lib = lmdb_version_store_v1
    sym = "sym"
    key = "reasonable_length_key"
    data = {key: pd.DataFrame({"col": [0]})}

    nesting_levels = 10
    for i in range(nesting_levels - 1):
        data[key] = {key: data[key]}

    # When
    lib.write(sym, data, recursive_normalizers=True)

    # Then
    result = lib.read(sym).data
    for i in range(nesting_levels):
        result = result[key]

    assert_frame_equal(result, pd.DataFrame({"col": [0]}))


def test_long_lists(lmdb_version_store_v1):
    # Given
    lib = lmdb_version_store_v1
    sym = "sym"
    df = pd.DataFrame({"col": [0]})
    length = 1_000
    data = [df] * length

    # When
    lib.write(sym, data, recursive_normalizers=True)

    # Then
    result = lib.read(sym).data
    assert len(result) == 1000
    assert_frame_equal(result[500], df)


def test_deep_nesting_metastruct_size(lmdb_version_store_v1):
    # Given
    lib = lmdb_version_store_v1
    sym = "sym"
    key = "reasonable_length_key"
    data = {key: pd.DataFrame({"col": [0]})}

    nesting_levels = 1_000
    for i in range(nesting_levels - 1):
        data[key] = {key: data[key]}

    # When & Then
    with pytest.raises(ValueError):
        """Currently raises a ValueError: recursion limit exceeded within msgpack. We should try to do better here."""
        lib.write(sym, data, recursive_normalizers=True)


def test_long_keys(lmdb_version_store_v1):
    """Long keys are truncated when they're saved - check that we can still roundtrip even with long keys."""
    # Given
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"col": [0]})
    sym = "sym"
    key_length = 10_000
    key = "".join(random.choice(string.ascii_letters) for _ in range(key_length))
    key_with_same_prefix = key[:1000]
    data = {key: df, key_with_same_prefix: df}

    # When
    lib.write(sym, data, recursive_normalizers=True)

    # Then
    result = lib.read(sym).data
    assert_frame_equal(result[key], df)
    assert_frame_equal(result[key_with_same_prefix], df)


@pytest.mark.parametrize("key", ("*", "<", ">", chr(31), chr(127)))
def test_unsupported_characters_in_keys(s3_version_store_v1, key):
    """Check how we serialize nested keys with characters that we do not support in normal symbol names"""
    # Given
    lib = s3_version_store_v1
    df = pd.DataFrame({"col": [0]})

    data = {key: df}

    # When & Then
    with pytest.raises(UserInputException):
        lib.write("sym", data, recursive_normalizers=True)


def test_unsupported_characters_in_keys_empty_string(s3_version_store_v1):
    """We allow empty keys in the recursive structure even though these do not work as top-level symbol names"""
    # Given
    lib = s3_version_store_v1
    df = pd.DataFrame({"col": [0]})

    data = {"": df}

    # When
    lib.write("sym", data, recursive_normalizers=True)

    # Then
    res = lib.read("sym").data
    pd.testing.assert_frame_equal(res[""], df)


@pytest.mark.parametrize("sequence_type", (tuple, list))
def test_sequences_data_layout(lmdb_version_store_v1, sequence_type):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"d": [1, 2, 3]})
    data = ("abc", df, {"ghi": df})
    data = sequence_type(data)

    lib.write("sym", data, recursive_normalizers=True)
    assert lib.get_info("sym")["type"] != "pickled"  # check we're testing the right feature!

    actual_data = lib.read("sym").data
    assert actual_data[0] == "abc"
    pd.testing.assert_frame_equal(actual_data[1], df)
    pd.testing.assert_frame_equal(actual_data[2]["ghi"], df)

    lt = lib.library_tool()

    assert len(lt.find_keys(KeyType.VERSION_REF)) == 1
    assert len(lt.find_keys(KeyType.VERSION)) == 1
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 1
    index_keys = lt.find_keys(KeyType.TABLE_INDEX)
    assert [i.id for i in index_keys] == ['sym__1', "sym__2__ghi"]
    data_keys = lt.find_keys(KeyType.TABLE_DATA)
    assert len(data_keys) == 2

    assert len(lt.find_keys_for_id(KeyType.VERSION_REF, "sym")) == 1
    assert len(lt.find_keys_for_id(KeyType.VERSION, "sym")) == 1
    assert len(lt.find_keys_for_id(KeyType.MULTI_KEY, "sym")) == 1
    assert len(lt.find_keys_for_id(KeyType.TABLE_INDEX, "sym")) == 0

    # Check that the multi key structure is correct
    multi_key = lt.find_keys_for_id(KeyType.MULTI_KEY, "sym")[0]
    assert multi_key.version_id == 0
    segment = lt.read_to_dataframe(multi_key)
    assert segment.shape[0] == 2
    contents = segment.iloc[0].to_dict()
    assert contents["key_type"] == KeyType.TABLE_INDEX.value
    assert contents["stream_id"] == b"sym__1"
    contents = segment.iloc[1].to_dict()
    assert contents["key_type"] == KeyType.TABLE_INDEX.value
    assert contents["stream_id"] == b"sym__2__ghi"

    # Check that we cannot read the fake index keys
    with pytest.raises(NoSuchVersionException):
        lib.read("sym__1")

    # Check that keys are cleaned up when we delete
    lib.delete("sym")
    assert len(lt.find_keys(KeyType.VERSION_REF)) == 1
    assert len(lt.find_keys(KeyType.VERSION)) == 2
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 0
    assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 0
    assert len(lt.find_keys(KeyType.TABLE_DATA)) == 0


DataFrameHolder = namedtuple("DataFrameHolder", ["contents"])


def test_namedtuple_gets_pickled(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"d": [1, 2, 3]})

    lib.write("sym", DataFrameHolder(df))

    assert lib.get_info("sym")["type"] == "pickled"
    assert_frame_equal(lib.read("sym").data.contents, df)

    lib.write("sym", [DataFrameHolder(df)])
    assert lib.get_info("sym")["type"] == "pickled"
    assert_frame_equal(lib.read("sym").data[0].contents, df)


class SomeClass:
    pass


def test_something_we_cannot_normalize_just_gets_pickled(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    lib.write("sym", [SomeClass()])
    assert lib.get_info("sym")["type"] == "pickled"


@pytest.mark.xfail(reason="These do not roundtrip properly. Monday: 9256783357")
@pytest.mark.parametrize("key", ("a__", "__a", "a__b", "__a__b", "a__b__"))
def test_key_names(lmdb_version_store_v1, key):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"d": [1, 2, 3]})
    data = {key: df}

    lib.write("sym", data, recursive_normalizers=True)
    assert lib.get_info("sym")["type"] != "pickled"  # check we're testing the right feature!

    actual_data = lib.read("sym").data
    assert key in actual_data
    pd.testing.assert_frame_equal(actual_data[key], df)

    assert lib.read("sym").version == 0


def test_read_asof(lmdb_version_store_v1):
    lib = lmdb_version_store_v1

    df_one = pd.DataFrame({"d": [1, 2, 3]})
    data_one = {"k": df_one}
    lib.write("sym", data_one, recursive_normalizers=True)

    df_two = pd.DataFrame({"d": [4, 5, 6]})
    data_two = {"k": df_two}
    lib.write("sym", data_two, recursive_normalizers=True)

    vit = lib.read("sym")
    assert vit.version == 1
    pd.testing.assert_frame_equal(vit.data["k"], df_two)

    vit = lib.read("sym", as_of=0)
    assert vit.version == 0
    pd.testing.assert_frame_equal(vit.data["k"], df_one)


@pytest.mark.xfail(reason="Validation for bad queries not yet implemented. Monday: 9236603911")
def test_unsupported_queries(lmdb_version_store_v1):
    """Test how we fail with queries that we do not support over recursively normalized data."""
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"d": [1, 2, 3]})
    data = {"sym": df}

    lib.write("sym", data, recursive_normalizers=True)
    assert lib.get_info("sym")["type"] != "pickled"  # check we're testing the right feature!

    # These queries fail with a numpy internals error
    lib.read("sym", date_range=(None, None))
    lib.read("sym", date_range=(datetime.date(2011, 1, 1), None))

    # These queries succeed but give wrong results - we should raise in these cases, at least in the external API
    qb = QueryBuilder()
    qb = qb.date_range((None, None))
    lib.read("sym", query_builder=qb)

    qb = QueryBuilder()
    qb = qb.date_range((datetime.date(2011, 1, 1), None))
    lib.read("sym", query_builder=qb)

    qb = QueryBuilder()
    qb = qb[qb.d < 2]
    lib.read("sym", query_builder=qb)

    lib.read("sym", row_range=(0, 2))

    lib.read("sym", columns=["e"])
    lib.read("sym", columns=["d"])


def test_data_layout(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"d": [1, 2, 3]})
    df_two = pd.DataFrame({"q": [4, 5, 6]})
    data = {"k": df, "l": [1, 2, 3], "m": {"n": "o", "p": df_two}, "p": df}

    lib.write("sym", data, recursive_normalizers=True)
    lib.write("sym", data, recursive_normalizers=True, prune_previous_version=False)
    assert lib.get_info("sym")["type"] != "pickled"  # check we're testing the right feature!

    actual_data = lib.read("sym").data
    pd.testing.assert_frame_equal(actual_data["k"], df)
    pd.testing.assert_frame_equal(actual_data["p"], df)
    assert actual_data["l"] == [1, 2, 3]
    assert actual_data["m"]["n"] == "o"
    pd.testing.assert_frame_equal(actual_data["m"]["p"], df_two)

    lt = lib.library_tool()

    assert len(lt.find_keys(KeyType.VERSION_REF)) == 1
    assert len(lt.find_keys(KeyType.VERSION)) == 2
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 2
    index_keys = lt.find_keys(KeyType.TABLE_INDEX)
    assert len(index_keys) == 6
    data_keys = lt.find_keys(KeyType.TABLE_DATA)
    assert len(data_keys) == 6

    assert len(lt.find_keys_for_id(KeyType.VERSION_REF, "sym")) == 1
    assert len(lt.find_keys_for_id(KeyType.VERSION, "sym")) == 2
    assert len(lt.find_keys_for_id(KeyType.MULTI_KEY, "sym")) == 2
    assert len(lt.find_keys_for_id(KeyType.TABLE_INDEX, "sym")) == 0

    # Check that the multi key structure is correct
    multi_key = lt.find_keys_for_id(KeyType.MULTI_KEY, "sym")[1]
    assert multi_key.version_id == 1
    segment = lt.read_to_dataframe(multi_key)
    assert segment.shape[0] == 3
    contents = segment.iloc[0].to_dict()
    assert contents["key_type"] == KeyType.TABLE_INDEX.value
    assert contents["stream_id"] == b"sym__k"
    contents = segment.iloc[1].to_dict()
    assert contents["key_type"] == KeyType.TABLE_INDEX.value
    assert contents["stream_id"] == b"sym__m__p"
    contents = segment.iloc[2].to_dict()
    assert contents["key_type"] == KeyType.TABLE_INDEX.value
    assert contents["stream_id"] == b"sym__p"

    # Check that we cannot read the fake index keys
    with pytest.raises(NoSuchVersionException):
        lib.read("sym__k")

    # Check that keys are cleaned up when we prune
    lib.write("sym", data, recursive_normalizers=True, prune_previous_version=True)
    assert len(lt.find_keys(KeyType.VERSION_REF)) == 1
    assert len(lt.find_keys(KeyType.VERSION)) == 3
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 1
    assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 3
    assert len(lt.find_keys(KeyType.TABLE_DATA)) == 3

    # Check that keys are cleaned up when we delete
    lib.delete("sym")
    assert len(lt.find_keys(KeyType.VERSION_REF)) == 1
    assert len(lt.find_keys(KeyType.VERSION)) == 4
    assert len(lt.find_keys(KeyType.MULTI_KEY)) == 0
    assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 0
    assert len(lt.find_keys(KeyType.TABLE_DATA)) == 0


class TestRecursiveNormalizersCompat:

    @pytest.mark.skipif(MACOS_WHEEL_BUILD, reason="We don't have previous versions of arcticdb pypi released for MacOS")
    def test_compat_write_old_read_new(self, old_venv_and_arctic_uri, lib_name):
        old_venv, arctic_uri = old_venv_and_arctic_uri
        with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
            dfs = {"df_1": pd.DataFrame({"a": [1, 2, 3]}), "df_2": pd.DataFrame({"b": ["a", "b"]})}
            sym = "sym"
            compat.old_lib.execute([
                f"""
from arcticdb_ext.storage import KeyType
lib._nvs.write('sym', {{"a": df_1, "b": df_2}}, recursive_normalizers=True, pickle_on_failure=True)
lib_tool = lib._nvs.library_tool()
assert len(lib_tool.find_keys_for_symbol(KeyType.MULTI_KEY, 'sym')) == 1
"""
            ], dfs=dfs)

            with compat.current_version() as curr:
                data = curr.lib.read(sym).data
                expected = {"a": dfs["df_1"], "b": dfs["df_2"]}
                assert set(data.keys()) == set(expected.keys())
                for key in data.keys():
                    assert_frame_equal(data[key], expected[key])

    @pytest.mark.skipif(MACOS_WHEEL_BUILD, reason="We don't have previous versions of arcticdb pypi released for MacOS")
    def test_write_new_read_old(self, old_venv_and_arctic_uri, lib_name):
        old_venv, arctic_uri = old_venv_and_arctic_uri
        with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
            dfs = {"df_1": pd.DataFrame({"a": [1, 2, 3]}), "df_2": pd.DataFrame({"b": ["a", "b"]})}
            with compat.current_version() as curr:
                curr.lib._nvs.write('sym', {"a": dfs["df_1"], "b": dfs["df_2"]}, recursive_normalizers=True, pickle_on_failure=True)
                lib_tool = curr.lib._nvs.library_tool()
                assert len(lib_tool.find_keys_for_symbol(KeyType.MULTI_KEY, 'sym')) == 1

            compat.old_lib.execute([
                """
from pandas.testing import assert_frame_equal
data = lib.read('sym').data
expected = {'a': df_1, 'b': df_2}
assert set(data.keys()) == set(expected.keys())
for key in data.keys():
    assert_frame_equal(data[key], expected[key])
"""
            ], dfs=dfs)