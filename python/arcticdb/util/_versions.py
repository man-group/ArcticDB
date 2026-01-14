"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import numpy as np
from arcticdb.dependencies import pyarrow as pa, _PYARROW_AVAILABLE
from packaging import version

PANDAS_VERSION = version.parse(pd.__version__)
CHECK_FREQ_VERSION = version.Version("1.1")
IS_PANDAS_ZERO = PANDAS_VERSION < version.Version("1.0")
IS_PANDAS_ONE = PANDAS_VERSION >= version.Version("1.0") and PANDAS_VERSION < version.Version("2.0")
IS_PANDAS_TWO = PANDAS_VERSION >= version.Version("2.0")

NUMPY_VERSION = version.parse(np.__version__)
IS_NUMPY_ONE = NUMPY_VERSION >= version.Version("1.0") and NUMPY_VERSION < version.Version("2.0")
IS_NUMPY_TWO = NUMPY_VERSION >= version.Version("2.0") and NUMPY_VERSION < version.Version("3.0")

PYARROW_VERSION = version.parse(pa.__version__) if _PYARROW_AVAILABLE else None
# Bug with null processing https://github.com/apache/arrow/issues/47234 is fixed as of 22.0.0
IS_PYARROW_WINDOWS_NULL_COMPUTE_FIXED = PYARROW_VERSION and PYARROW_VERSION >= version.Version("22.0")
