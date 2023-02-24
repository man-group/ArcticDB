"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
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
