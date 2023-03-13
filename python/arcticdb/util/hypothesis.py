"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from hypothesis.extra.numpy import unsigned_integer_dtypes, integer_dtypes, floating_dtypes, from_dtype
from math import inf
import hypothesis.strategies as st


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
