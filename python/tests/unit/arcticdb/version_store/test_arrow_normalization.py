import pytest

from arcticdb.exceptions import ArcticDbNotYetImplemented


def test_read_pickled(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_read_pickled"
    obj = {"a": ["b", "c"], "x": 122.3}
    lib.write(sym, obj)
    result = lib.read(sym).data
    assert obj == result


def test_custom_normalizer(custom_thing_with_registered_normalizer, in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_custom_normalizer"
    obj = custom_thing_with_registered_normalizer
    lib.write(sym, obj)
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.read(sym).data
