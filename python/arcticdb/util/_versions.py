"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
import numpy as np
from packaging import version

PANDAS_VERSION = version.parse(pd.__version__)
CHECK_FREQ_VERSION = version.Version("1.1")
IS_PANDAS_ZERO = PANDAS_VERSION < version.Version("1.0")
IS_PANDAS_TWO = PANDAS_VERSION >= version.Version("2.0")
IS_NUMPY_TWO = version.parse(np.__version__) >= version.Version("2.0")
