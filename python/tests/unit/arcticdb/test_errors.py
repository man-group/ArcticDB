import pytest

from arcticdb.util.errors import *
from arcticdb_ext.exceptions import NormalizationException, get_error_category, ErrorCategory


test_raise_params = [(NormalizationError.E_UPDATE_NOT_SUPPORTED, NormalizationException)]


@pytest.mark.parametrize("code,exception", test_raise_params)
def test_raise(code, exception):
    with pytest.raises(exception, match=f"E{code.value} a message$"):
        arcticdb_raise(code, lambda: "a message")
