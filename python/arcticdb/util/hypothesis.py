"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from math import inf

from hypothesis import settings, HealthCheck, strategies as st
from hypothesis.extra.numpy import unsigned_integer_dtypes, integer_dtypes, floating_dtypes, from_dtype

_function_scoped_fixture = getattr(HealthCheck, "function_scoped_fixture", None)


def use_of_function_scoped_fixtures_in_hypothesis_checked(fun):
    """
    A decorator to declare that the use of function-scoped fixtures in said scope is correct,
    i.e. the state in the fixture from previous hypothesis tries will not affect future tries.

    See https://hypothesis.readthedocs.io/en/latest/healthchecks.html#hypothesis.HealthCheck.function_scoped_fixture

    Examples
    ========
    ```
    @use_of_function_scoped_fixtures_in_hypothesis_checked
    @given(...)
    def test_something(function_scope_fixture):
        ...
    ```
    """

    if not _function_scoped_fixture:
        return fun

    existing = getattr(fun, "_hypothesis_internal_use_settings", None)
    assert existing or getattr(fun, "is_hypothesis_test", False), "Must be used before @given/@settings"

    combined = {_function_scoped_fixture, *(existing or settings.default).suppress_health_check}
    new_settings = settings(parent=existing, suppress_health_check=combined)
    setattr(fun, "_hypothesis_internal_use_settings", new_settings)
    return fun


def non_infinite(x):
    return -inf < x < inf


def non_zero_and_non_infinite(x):
    return non_infinite(x) and x != 0


@st.composite
def integral_type_strategies(draw):
    return draw(from_dtype(draw(st.one_of([unsigned_integer_dtypes(), integer_dtypes()]))).filter(non_infinite))


@st.composite
def numeric_type_strategies(draw):
    return draw(
        from_dtype(draw(st.one_of([unsigned_integer_dtypes(), integer_dtypes(), floating_dtypes()]))).filter(
            non_infinite
        )
    )


@st.composite
def non_zero_numeric_type_strategies(draw):
    return draw(
        from_dtype(draw(st.one_of([unsigned_integer_dtypes(), integer_dtypes(), floating_dtypes()]))).filter(
            non_zero_and_non_infinite
        )
    )


string_strategy = st.text(
    min_size=1, alphabet=st.characters(blacklist_characters="'\\", min_codepoint=32, max_codepoint=126)
)
