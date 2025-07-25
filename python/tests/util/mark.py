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
from copy import deepcopy

from arcticdb.util import marks

MACOS = sys.platform.lower().startswith("darwin")
LINUX = sys.platform.lower().startswith("linux")
WINDOWS = sys.platform.lower().startswith("win32")

# TODO: Some tests are either segfaulting or failing on MacOS with conda builds.
# This is meant to be used as a temporary flag to skip/xfail those tests.
ARCTICDB_USING_CONDA = marks.ARCTICDB_USING_CONDA
MACOS_CONDA_BUILD = ARCTICDB_USING_CONDA and MACOS
MACOS_WHEEL_BUILD = not ARCTICDB_USING_CONDA and MACOS

_MACOS_AZURE_TESTS_SKIP_REASON = (
    "Tests fail for macOS vcpkg builds, either because Azurite is improperly configured"
    "on the CI or because there's problem with Azure SDK for C++ in this configuration."
)

SKIP_CONDA_MARK = pytest.mark.skipif(
    ARCTICDB_USING_CONDA,
    reason="Those tests are skipped on conda",
)

# These two should become pytest marks as opposed to variables feeding into skipif
PERSISTENT_STORAGE_TESTS_ENABLED = os.getenv("ARCTICDB_PERSISTENT_STORAGE_TESTS") == "1"
FAST_TESTS_ONLY = os.getenv("ARCTICDB_FAST_TESTS_ONLY") == "1"
DISABLE_SLOW_TESTS = os.getenv("ARCTICDB_DISABLE_SLOW_TESTS") == "1"
# Local storage tests are all LMDB, simulated and a real mongo process/service
LOCAL_STORAGE_TESTS_ENABLED = os.getenv("ARCTICDB_LOCAL_STORAGE_TESTS_ENABLED", "1") == "1"
STORAGE_LMDB = os.getenv("ARCTICDB_STORAGE_LMDB", "1") == "1" or LOCAL_STORAGE_TESTS_ENABLED == "1"
STORAGE_AWS_S3 = os.getenv("ARCTICDB_STORAGE_AWS_S3", "1") == "1"
STORAGE_GCP = os.getenv("ARCTICDB_STORAGE_GCP") == "1"
STORAGE_AZURE = os.getenv("ARCTICDB_STORAGE_AZURE") == "1"

# Defined shorter logs on errors
SHORTER_LOGS = marks.SHORTER_LOGS

# !!!!!!!!!!!!!!!!!!!!!! Below mark (variable) names should reflect where they will be used, not what they do.
# This is to avoid the risk of the name becoming out of sync with the actual condition.
SLOW_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or DISABLE_SLOW_TESTS, reason="Skipping test as it takes a long time to run"
)

AZURE_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or MACOS or not LOCAL_STORAGE_TESTS_ENABLED, reason=_MACOS_AZURE_TESTS_SKIP_REASON
)
"""Mark to skip all Azure tests when MACOS or ARCTICDB_FAST_TESTS_ONLY is set."""

# Mongo tests will run under local storage tests
MONGO_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or sys.platform != "linux" or not LOCAL_STORAGE_TESTS_ENABLED,
    reason="Skipping mongo tests under ARCTICDB_FAST_TESTS_ONLY and if local storage tests are disabled",
)
"""Mark on tests using the mongo storage fixtures. Currently skips if ARCTICDB_FAST_TESTS_ONLY."""

REAL_S3_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or not PERSISTENT_STORAGE_TESTS_ENABLED or not STORAGE_AWS_S3,
    reason="Real S3 can be used only when persistent storage is enabled",
)
"""Mark on tests using the real (i.e. hosted by AWS as opposed to moto) S3.
Currently controlled by the ARCTICDB_PERSISTENT_STORAGE_TESTS and ARCTICDB_FAST_TESTS_ONLY env vars."""
REAL_GCP_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or not PERSISTENT_STORAGE_TESTS_ENABLED or not STORAGE_GCP,
    reason="Real GCP can be used only when persistent storage is enabled",
)
"""Mark on tests using the real Azure.
Currently controlled by the ARCTICDB_PERSISTENT_STORAGE_TESTS and ARCTICDB_FAST_TESTS_ONLY env vars."""
REAL_AZURE_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or not PERSISTENT_STORAGE_TESTS_ENABLED or not STORAGE_AZURE,
    reason="Real Azure can be used only when persistent storage is enabled",
)

"""Mark on tests using the real GCP storage.
"""
"""Mark on tests using S3 model storage.
"""
SIM_S3_TESTS_MARK = pytest.mark.skipif(
    not LOCAL_STORAGE_TESTS_ENABLED,
    reason="Ability to disable local storages - simulates s3 is disabled",
)
"""Mark on tests using GCP model storage.
"""
SIM_GCP_TESTS_MARK = pytest.mark.skipif(
    not LOCAL_STORAGE_TESTS_ENABLED,
    reason="Ability to disable local storages - simulates gcp is disabled",
)
"""Mark on tests using the real GCP storage.
"""
"""Mark on tests using the LMDB storage.
"""
LMDB_TESTS_MARK = pytest.mark.skipif(
    not STORAGE_LMDB,
    reason="Ability to disable local storages - lmdb storage is disabled",
)
"""Mark on tests using the MEM storage.
"""
MEM_TESTS_MARK = pytest.mark.skipif(
    not LOCAL_STORAGE_TESTS_ENABLED,
    reason="Ability to disable local storages - mem storage is disabled",
)
"""Mark on tests using the NFS model storage.
"""
SIM_NFS_TESTS_MARK = pytest.mark.skipif(
    not LOCAL_STORAGE_TESTS_ENABLED,
    reason="Ability to disable local storages - simulated nfs is disabled",
)
"""Mark on tests using the real GCP storage.
"""


"""Windows and MacOS have different handling of self-signed CA cert for test.
TODO: https://github.com/man-group/ArcticDB/issues/1394"""
SSL_TEST_SUPPORTED = sys.platform == "linux"

FORK_SUPPORTED = pytest.mark.skipif(WINDOWS, reason="Fork not supported on Windows")

## MEMRAY supports linux and macos and python 3.8 and above
MEMRAY_SUPPORTED = MACOS or LINUX
MEMRAY_TESTS_MARK = pytest.mark.skipif(
    not MEMRAY_SUPPORTED, reason="MEMRAY supports linux and macos and python 3.8 and above"
)

ZONE_INFO_MARK = pytest.mark.skipif(sys.version_info < (3, 9), reason="zoneinfo module was introduced in Python 3.9")

SSL_TESTS_MARK = pytest.mark.skipif(
    not SSL_TEST_SUPPORTED,
    reason="When SSL tests are enabled",
)

VENV_COMPAT_TESTS_MARK = pytest.mark.skipif(
    MACOS_CONDA_BUILD
    or sys.version.startswith("3.12")
    or sys.version.startswith("3.13"),  # Waiting for https://github.com/man-group/ArcticDB/issues/2008
    reason="Skipping compatibility tests because macOS conda builds don't have an available PyPi arcticdb version",
)

PANDAS_2_COMPAT_TESTS_MARK = pytest.mark.skipif(
    MACOS_CONDA_BUILD
    or sys.version.startswith("3.8")  # Pandas 2 not available in 3.8
    or sys.version.startswith("3.12")
    or sys.version.startswith("3.13"),  # Waiting for https://github.com/man-group/ArcticDB/issues/2008
    reason="Skipping compatibility tests because macOS conda builds don't have an available PyPi arcticdb version and "
    "need a version that has Pandas 2",
)


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


def param_dict(fields, cases=None):
    _cases = deepcopy(cases) if cases else dict()
    if _cases:
        ids, params = zip(*list(sorted(_cases.items())))
    else:
        ids, params = [], []

    return pytest.mark.parametrize(fields, params, ids=ids)


def xfail_azure_chars(nvs, char):
    storage_is_azure = False
    try:
        storage_is_azure = "azure" in nvs.get_backing_store()
    except:
        # for v2 API
        storage_is_azure = "azure" in nvs._nvs.get_backing_store()
    if (storage_is_azure) and (char in [chr(0), chr(30), chr(127)]):
        pytest.xfail("Char is not supported by azure and filtered by arcticdb (see 9669996138)")
