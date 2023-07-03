import pandas as pd
from packaging import version

PANDAS_VERSION = version.parse(pd.__version__)
CHECK_FREQ_VERSION = version.Version("1.1")
IS_PANDAS_ZERO = PANDAS_VERSION < version.Version("1.0")
IS_PANDAS_TWO = PANDAS_VERSION >= version.Version("2.0")
