"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest

from unittest.mock import patch
from arcticdb.tools import set_config_from_env_vars, _ARCTICDB_ENV_VAR_PREFIX, _ARCTIC_NATIVE_ENV_VAR_PREFIX
from arcticdb.util.utils import strtobool

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
    with patch(_MODULE + ".storage_log") as log_mock:
        set_config_from_env_vars({_ARCTICDB_ENV_VAR_PREFIX + key: value})
        log_mock.error.assert_called()

    for setter in mocks.values():
        setter.assert_not_called()


@pytest.fixture()
def log_mocks():
    with patch(_MODULE + ".set_config_int") as s_int, patch(_MODULE + ".set_log_level") as s_log:
        with patch(_MODULE + ".storage_log") as s_storage_log:
            yield {"set_config_int": s_int, "set_log_level": s_log, "storage_log": s_storage_log}


def _aws_log_level_set_to(mock):
    for call in mock.call_args_list:
        if call.args and call.args[0] == "AWS.LogLevel":
            return call.args[1]
    return None


def test_s3_loglevel_drives_aws_emission(log_mocks):
    set_config_from_env_vars({"ARCTICDB_s3_loglevel": "DEBUG"})

    assert _aws_log_level_set_to(log_mocks["set_config_int"]) == 5  # Debug
    _, kwargs = log_mocks["set_log_level"].call_args
    assert kwargs["specific_log_levels"]["s3"] == "DEBUG"
    log_mocks["storage_log"].warn.assert_not_called()


def test_deprecated_aws_var_warns_and_sets_s3(log_mocks):
    set_config_from_env_vars({"ARCTICDB_AWS_LogLevel_int": "6"})

    assert _aws_log_level_set_to(log_mocks["set_config_int"]) == 6  # Trace
    _, kwargs = log_mocks["set_log_level"].call_args
    assert kwargs["specific_log_levels"]["s3"] == "TRACE"
    log_mocks["storage_log"].warn.assert_called_once()


@pytest.mark.parametrize(
    "s3_level, aws_int, expected_int, expected_s3",
    [
        ("WARN", 5, 5, "DEBUG"),  # AWS (Debug) more verbose than s3 (Warn) -> AWS wins
        ("DEBUG", 2, 5, "DEBUG"),  # s3 (Debug) more verbose than AWS (Error) -> s3 wins
        ("TRACE", 6, 6, "TRACE"),  # equal verbosity
    ],
)
def test_more_verbose_of_the_two_wins(log_mocks, s3_level, aws_int, expected_int, expected_s3):
    set_config_from_env_vars({"ARCTICDB_s3_loglevel": s3_level, "ARCTICDB_AWS_LogLevel_int": str(aws_int)})

    assert _aws_log_level_set_to(log_mocks["set_config_int"]) == expected_int
    _, kwargs = log_mocks["set_log_level"].call_args
    assert kwargs["specific_log_levels"]["s3"] == expected_s3
    log_mocks["storage_log"].warn.assert_called_once()


@pytest.mark.parametrize("bad_value", ["7", "99", "-1"])
def test_out_of_range_aws_log_level_is_ignored(log_mocks, bad_value):
    set_config_from_env_vars({"ARCTICDB_AWS_LogLevel_int": bad_value})

    # The raw value is still forwarded to ConfigsMap, but it does not feed into the reconciled effective level.
    assert _aws_log_level_set_to(log_mocks["set_config_int"]) is None
    log_mocks["storage_log"].error.assert_called()
    log_mocks["set_log_level"].assert_not_called()


def test_no_s3_or_aws_var_does_not_touch_aws_log_level(log_mocks):
    set_config_from_env_vars({"ARCTICDB_version_loglevel": "DEBUG"})

    assert _aws_log_level_set_to(log_mocks["set_config_int"]) is None
    _, kwargs = log_mocks["set_log_level"].call_args
    assert "s3" not in kwargs["specific_log_levels"]
    log_mocks["storage_log"].warn.assert_not_called()


@pytest.mark.parametrize("input_val", ["y", "yes", "t", "true", "on", "1", "YES", "True", "ON"])
def test_strtobool_true(input_val):
    assert strtobool(input_val) is True


@pytest.mark.parametrize("input_val", ["n", "no", "f", "false", "off", "0", "NO", "False", "OFF"])
def test_strtobool_false(input_val):
    assert strtobool(input_val) is False


@pytest.mark.parametrize("invalid_input", ["maybe", "2", "none", "", "blue", "not-on"])
def test_strtobool_invalid(invalid_input):
    assert not strtobool(invalid_input)


def test_strtobool_type_error():
    with pytest.raises(AttributeError):
        strtobool(None)
    with pytest.raises(AttributeError):
        strtobool(1)
