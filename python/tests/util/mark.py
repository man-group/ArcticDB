"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import os
import sys
import pytest
from typing import Union
from datetime import date
from numpy import datetime64

# TODO: Some tests are either segfaulting or failing on MacOS with conda builds.
# This is meant to be used as a temporary flag to skip/xfail those tests.
ARCTICDB_USING_CONDA = os.getenv("ARCTICDB_USING_CONDA", "0") == "1"
MACOS_CONDA_BUILD = sys.platform == "darwin" and ARCTICDB_USING_CONDA
_MACOS_CONDA_BUILD_SKIP_REASON = (
    "Tests fail for macOS conda builds, either because Azurite is improperly configured"
    "on the CI or because there's problem with Azure SDK for C++ in this configuration."
)

# These two should become pytest marks as opposed to variables feeding into skipif
PERSISTENT_STORAGE_TESTS_ENABLED = os.getenv("ARCTICDB_PERSISTENT_STORAGE_TESTS") == "1"
FAST_TESTS_ONLY = os.getenv("ARCTICDB_FAST_TESTS_ONLY") == "1"


# !!!!!!!!!!!!!!!!!!!!!! Below mark (variable) names should reflect where they will be used, not what they do.
# This is to avoid the risk of the name becoming out of sync with the actual condition.
SLOW_TESTS_MARK = pytest.mark.skipif(FAST_TESTS_ONLY, reason="Skipping test as it takes a long time to run")

AZURE_TESTS_MARK = pytest.mark.skipif(FAST_TESTS_ONLY or MACOS_CONDA_BUILD, reason=_MACOS_CONDA_BUILD_SKIP_REASON)
"""Mark to skip all Azure tests when MACOS_CONDA_BUILD or ARCTICDB_FAST_TESTS_ONLY is set."""

MONGO_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or sys.platform != "linux",
    reason="Skipping mongo tests under ARCTICDB_FAST_TESTS_ONLY",
)
"""Mark on tests using the mongo storage fixtures. Currently skips if ARCTICDB_FAST_TESTS_ONLY."""

REAL_S3_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or not PERSISTENT_STORAGE_TESTS_ENABLED,
    reason="Can be used only when persistent storage is enabled",
)
"""Mark on tests using the real (i.e. hosted by AWS as opposed to moto) S3.
Currently controlled by the ARCTICDB_PERSISTENT_STORAGE_TESTS and ARCTICDB_FAST_TESTS_ONLY env vars."""


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
