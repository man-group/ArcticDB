"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import sys
from typing import Union
from datetime import date
from numpy import datetime64
from packaging import version

from arcticdb.util._versions import PANDAS_VERSION


def _no_op_decorator(fun):
    return fun


def until(until_date: Union[datetime64, date, str], mark_decorator):
    """
    A decorator to conditionally apply the given mark decorator until the given date.

    ```
    @until()
    ```
    """
    return mark_decorator if datetime64("today") <= datetime64(until_date) else _no_op_decorator
