import sys

import pandas as pd
import pytest
import numpy as np
from arcticdb.version_store._custom_normalizers import (
    CustomNormalizer,
    register_normalizer,
)
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata
from arcticdb.util.venv import CompatLibrary

from arcticdb.util.test import (
    assert_frame_equal,
    equals,
    CustomArray,
    CustomArrayNormalizer,
)
from tests.util.mark import ARCTICDB_USING_CONDA, MACOS_WHEEL_BUILD
from arcticdb_ext.storage import KeyType

if ARCTICDB_USING_CONDA:
    pytest.skip("These tests rely on pip based environments", allow_module_level=True)

if MACOS_WHEEL_BUILD:
    pytest.skip("We don't have previous versions of arcticdb pypi released for MacOS", allow_module_level=True)


@pytest.fixture
def root_custom_array():
    # Make classes as if imported at root namespace so they can be pickled
    globals()["CustomArray"].__module__ = "__main__"
    globals()["CustomArrayNormalizer"].__module__ = "__main__"
    main_module = sys.modules["__main__"]
    main_module.CustomArray = CustomArray
    main_module.CustomArrayNormalizer = CustomArrayNormalizer
    yield
    delattr(main_module, "CustomArray")
    delattr(main_module, "CustomArrayNormalizer")
    globals()["CustomArray"].__module__ = "arcticdb.util.test"
    globals()["CustomArrayNormalizer"].__module__ = "arcticdb.util.test"


_COMPAT_TEST_CLASS_DEFINITIONS = """
class CustomArray():
    def __init__(self, x, y, z):
        self._x = x
        self._y = y
        self._z = z

    def __eq__(self, other):
        return self._x == other._x and self._y == other._y and self._z == other._z

class CustomArrayNormalizer(CustomNormalizer):
    NESTED_STRUCTURE = True

    def normalize(self, item, **kwargs):
        if not isinstance(item, CustomArray):
            return None
        return [item._x, item._y, item._z], NormalizationMetadata.CustomNormalizerMeta()

    def denormalize(self, item, norm_meta):
        return CustomArray(*item)
    """


def test_recursive_norm_compat_write_old_read_new(
    old_venv_and_arctic_uri, lib_name, all_recursive_metastructure_versions
):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        dfs = {"df_1": pd.DataFrame({"a": [1, 2, 3]}), "df_2": pd.DataFrame({"b": ["a", "b"]})}
        sym = "sym"
        compat.old_lib.execute(
            [f"""
from arcticdb_ext.storage import KeyType
lib._nvs.write('sym', {{"a": df_1, "b" * 95: df_2, "c" * 100: df_2}}, recursive_normalizers=True, pickle_on_failure=True)
lib_tool = lib._nvs.library_tool()
assert len(lib_tool.find_keys_for_symbol(KeyType.MULTI_KEY, 'sym')) == 1
"""],
            dfs=dfs,
        )

        with compat.current_version() as curr:
            data = curr.lib.read(sym).data
            expected = {"a": dfs["df_1"], "b" * 95: dfs["df_2"], "c" * 100: dfs["df_2"]}
            assert set(data.keys()) == set(expected.keys())
            for key in data.keys():
                assert_frame_equal(data[key], expected[key])


def test_recursive_norm_write_new_read_old(old_venv_and_arctic_uri, lib_name):
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        dfs = {"df_1": pd.DataFrame({"a": [1, 2, 3]}), "df_2": pd.DataFrame({"b": ["a", "b"]})}
        with compat.current_version() as curr:
            curr.lib._nvs.write(
                "sym",
                {"a": dfs["df_1"], "b" * 95: dfs["df_2"], "c" * 100: dfs["df_2"]},
                recursive_normalizers=True,
                pickle_on_failure=True,
            )
            lib_tool = curr.lib._nvs.library_tool()
            assert len(lib_tool.find_keys_for_symbol(KeyType.MULTI_KEY, "sym")) == 1

        compat.old_lib.execute(
            ["""
from pandas.testing import assert_frame_equal
data = lib.read('sym').data
expected = {'a': df_1, 'b' * 95: df_2, 'c' * 100: df_2}
assert set(data.keys()) == set(expected.keys())
for key in data.keys():
    assert_frame_equal(data[key], expected[key])
"""],
            dfs=dfs,
        )


def test_recursive_norm_write_new_read_old_custom_normalizer(
    old_venv_and_arctic_uri, lib_name, all_recursive_metastructure_versions, root_custom_array
):
    """Test bidirectional compatibility for custom normalizer with CustomArray and partial pickle with CustomClassSeparatorInStr"""
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        dfs = {"df_1": pd.DataFrame({"a": [1, 2, 3]})}
        with compat.current_version() as curr:
            register_normalizer(CustomArrayNormalizer())

            data = {
                "a": [1, 2, 3],
                "b": {"c": np.arange(24)},
                "d": CustomArray(100, 1000, 10),
                "e": (1, 2, 3),
            }
            curr.lib._nvs.write("sym", data, recursive_normalizers=True)
            lib_tool = curr.lib._nvs.library_tool()
            assert len(lib_tool.find_keys_for_symbol(KeyType.MULTI_KEY, "sym")) > 0

        read_lines = (
            """
data = lib.read('sym').data
expected = {
    "a": [1, 2, 3],
    "b": {"c": np.arange(24)},
    "d": CustomArray(100, 1000, 10),
    "e": (1, 2, 3), # tuple must be pickled
}
equals(expected, data)
"""
            if all_recursive_metastructure_versions == 1  # V1
            else """
with pytest.raises(KeyError):
    lib.read("sym")
            """
        )
        compat.old_lib.execute(
            [f"""
from arcticdb.version_store._custom_normalizers import CustomNormalizer, register_normalizer
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata
import numpy as np
import pytest

{_COMPAT_TEST_CLASS_DEFINITIONS}

def equals(x, y):
    if isinstance(x, tuple) or isinstance(x, list):
        assert len(x) == len(y)
        for vx, vy in zip(x, y):
            equals(vx, vy)
    elif isinstance(x, dict):
        assert isinstance(y, dict)
        assert set(x.keys()) == set(y.keys())
        for k in x.keys():
            equals(x[k], y[k])
    elif isinstance(x, np.ndarray):
        assert isinstance(y, np.ndarray)
        assert np.allclose(x, y)
    else:
        assert x == y

register_normalizer(CustomArrayNormalizer())

{read_lines}
"""],
            dfs=dfs,
        )


def test_recursive_norm_compat_write_old_read_new_custom_normalizer(
    old_venv_and_arctic_uri, lib_name, all_recursive_metastructure_versions, root_custom_array
):
    """Test bidirectional compatibility (old writes, new reads) for custom normalizer with CustomArray and partial pickle with CustomClassSeparatorInStr"""
    old_venv, arctic_uri = old_venv_and_arctic_uri
    with CompatLibrary(old_venv, arctic_uri, lib_name) as compat:
        dfs = {"df_1": pd.DataFrame({"a": [1, 2, 3]})}

        compat.old_lib.execute(
            [f"""
from arcticdb.version_store._custom_normalizers import CustomNormalizer, register_normalizer
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata
from arcticdb_ext.storage import KeyType
import numpy as np

{_COMPAT_TEST_CLASS_DEFINITIONS}

# Register the normalizer in old version
register_normalizer(CustomArrayNormalizer())

data = {{
    "a": [1, 2, 3],
    "b": {{"c": np.arange(24)}},
    "d": CustomArray(100, 1000, 10),
    "e": (1, 2, 3), # tuple must be pickled
}}
lib._nvs.write("sym", data, recursive_normalizers=True)
"""],
            dfs=dfs,
        )

        with compat.current_version() as curr:
            register_normalizer(CustomArrayNormalizer())

            data = curr.lib.read("sym").data
            expected = {
                "a": [1, 2, 3],
                "b": {"c": np.arange(24)},
                "d": CustomArray(100, 1000, 10),
                "e": (1, 2, 3),
            }
            equals(expected, data)
