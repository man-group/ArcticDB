"""Utilities for assertions."""
import pandas as pd
from functools import wraps
import pandas as pd


PANDAS_VERSION = tuple([int(i) for i in pd.__version__.split(".")])
CHECK_FREQ_VERSION = (1, 1, 0)


def maybe_not_check_freq(f):
    """Ignore frequency when pandas is newer as starts to check frequency which it did not previously do."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        if PANDAS_VERSION >= CHECK_FREQ_VERSION and "check_freq" not in kwargs:
            kwargs["check_freq"] = False
        return f(*args, **kwargs)

    return wrapper


assert_frame_equal = maybe_not_check_freq(pd.testing.assert_frame_equal)
assert_series_equal = maybe_not_check_freq(pd.testing.assert_series_equal)
