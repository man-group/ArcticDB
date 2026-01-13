"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest

from unittest.mock import patch
from arcticdb.tools import set_config_from_env_vars, _ARCTICDB_ENV_VAR_PREFIX, _ARCTIC_NATIVE_ENV_VAR_PREFIX

_MODULE = set_config_from_env_vars.__module__  # Insulate the tests from any move of the function


@pytest.fixture()
def mocks():
    # Manually breaking the with due to Black...
    with patch(_MODULE + ".set_config_int") as s_int, patch(_MODULE + ".set_config_double") as s_double:
        with patch(_MODULE + ".set_config_string") as s_str:
            yield {int: s_int, float: s_double, str: s_str}


@pytest.mark.parametrize("prefix", [_ARCTICDB_ENV_VAR_PREFIX, _ARCTIC_NATIVE_ENV_VAR_PREFIX])
@pytest.mark.parametrize("key, value", [("a_int", 42), ("a_float", 3.14), ("a_str", "text"), ("without_suffix", "xx")])
def test_get_normal(prefix, key, value, mocks):
    set_config_from_env_vars({prefix + key: str(value)})
    for typ, setter in mocks.items():
        if typ is type(value):
            setter.assert_called_with("a".upper() if key.startswith("a_") else "without.suffix".upper(), value)
        else:
            setter.assert_not_called()


@pytest.mark.parametrize("key, value", [("a_int", "a"), ("a_float", "aa")])
def test_bad_format(key, value, mocks):
    with patch(_MODULE + ".logging") as log_mock:
        set_config_from_env_vars({_ARCTICDB_ENV_VAR_PREFIX + key: value})
        log_mock.error.assert_called()

    for setter in mocks.values():
        setter.assert_not_called()
