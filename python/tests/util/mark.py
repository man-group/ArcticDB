"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import sys
import pytest
import platform
from typing import Optional, Union
from datetime import date
from numpy import datetime64
from copy import deepcopy

from arcticdb.util import marks
from arcticdb.util.logger import get_logger

MACOS = sys.platform.lower().startswith("darwin")
LINUX = sys.platform.lower().startswith("linux")
WINDOWS = sys.platform.lower().startswith("win32")

# Architecture detection
ARM64 = platform.machine().lower() in ("arm64", "aarch64")


# Defined shorter logs on errors
SHORTER_LOGS = marks.SHORTER_LOGS
logger = get_logger()

RUNS_ON_GITHUB = os.getenv("GITHUB_ACTIONS") == "true"


def getenv_strip(env_var_name: str, default_value: Optional[str] = None) -> Optional[str]:
    """
    Get environment variable and strip whitespace safely.

    Useful for string variables that represent enum values like "0" and "1".
    Returns the stripped value or the default if the variable is not set.
    """
    value = os.getenv(env_var_name)
    return default_value if value is None or value.strip() == "" else value.strip()


# TODO: Some tests are either segfaulting or failing on MacOS with conda builds.
# This is meant to be used as a temporary flag to skip/xfail those tests.
ARCTICDB_USING_CONDA = marks.ARCTICDB_USING_CONDA
MACOS_CONDA_BUILD = ARCTICDB_USING_CONDA and MACOS
MACOS_WHEEL_BUILD = not ARCTICDB_USING_CONDA and MACOS

# These two should become pytest marks as opposed to variables feeding into skipif
PERSISTENT_STORAGE_TESTS_ENABLED = getenv_strip("ARCTICDB_PERSISTENT_STORAGE_TESTS") == "1"
FAST_TESTS_ONLY = getenv_strip("ARCTICDB_FAST_TESTS_ONLY") == "1"
DISABLE_SLOW_TESTS = getenv_strip("ARCTICDB_DISABLE_SLOW_TESTS") == "1"
if PERSISTENT_STORAGE_TESTS_ENABLED:
    # This is for legacy reasons AWS has different treatment because of persistent storages test workflow at github
    STORAGE_AWS_S3 = getenv_strip("ARCTICDB_STORAGE_AWS_S3", "1") == "1"
else:
    STORAGE_AWS_S3 = getenv_strip("ARCTICDB_STORAGE_AWS_S3") == "1"
STORAGE_GCP = getenv_strip("ARCTICDB_STORAGE_GCP") == "1"
STORAGE_AZURE = getenv_strip("ARCTICDB_STORAGE_AZURE") == "1"
# Local storage tests are all LMDB, simulated and a real mongo process/service
LOCAL_STORAGE_TESTS_ENABLED = getenv_strip("ARCTICDB_LOCAL_STORAGE_TESTS_ENABLED", "1") == "1"
# Each storage can be controlled individually
STORAGE_LMDB = getenv_strip("ARCTICDB_STORAGE_LMDB") == "1" or (
    LOCAL_STORAGE_TESTS_ENABLED and getenv_strip("ARCTICDB_STORAGE_LMDB") != "0"
)
STORAGE_AZURITE = getenv_strip("ARCTICDB_STORAGE_AZURITE") == "1" or (
    LOCAL_STORAGE_TESTS_ENABLED and getenv_strip("ARCTICDB_STORAGE_AZURITE") != "0"
)
STORAGE_MONGO = getenv_strip("ARCTICDB_STORAGE_MONGO") == "1" or (
    LOCAL_STORAGE_TESTS_ENABLED and getenv_strip("ARCTICDB_STORAGE_MONGO") != "0"
)
STORAGE_MEM = getenv_strip("ARCTICDB_STORAGE_MEM") == "1" or (
    LOCAL_STORAGE_TESTS_ENABLED and getenv_strip("ARCTICDB_STORAGE_MEM") != "0"
)
STORAGE_NFS = getenv_strip("ARCTICDB_STORAGE_NFS") == "1" or (
    LOCAL_STORAGE_TESTS_ENABLED and getenv_strip("ARCTICDB_STORAGE_NFS") != "0"
)
# When a real storage is turned on the simulated storage is turned off
if STORAGE_AWS_S3:
    STORAGE_SIM_S3 = False
else:
    STORAGE_SIM_S3 = getenv_strip("ARCTICDB_STORAGE_SIM_S3") == "1" or (
        LOCAL_STORAGE_TESTS_ENABLED and getenv_strip("ARCTICDB_STORAGE_SIM_S3") != "0"
    )
if STORAGE_GCP:
    STORAGE_SIM_GCP = False
else:
    STORAGE_SIM_GCP = getenv_strip("ARCTICDB_STORAGE_SIM_GCP") == "1" or (
        LOCAL_STORAGE_TESTS_ENABLED and getenv_strip("ARCTICDB_STORAGE_SIM_GCP") != "0"
    )
if STORAGE_AZURE:
    STORAGE_AZURITE = False
else:
    STORAGE_AZURITE = getenv_strip("ARCTICDB_STORAGE_AZURITE") == "1" or (
        LOCAL_STORAGE_TESTS_ENABLED and getenv_strip("ARCTICDB_STORAGE_AZURITE") != "0"
    )
TEST_ENCODING_V1 = getenv_strip("ARCTICDB_TEST_ENCODING_V1", "1") == "1"
TEST_ENCODING_V2 = getenv_strip("ARCTICDB_TEST_ENCODING_V2", "1") == "1"


if not SHORTER_LOGS:
    logger.info("-" * 120)
    logger.info("  ARCTICDB ENVIRONMENT VARIABLES:")
    for name, value in os.environ.items():
        if name.startswith("ARCTICDB_"):
            logger.info(f"{name}={value}")
    logger.info("-" * 120)
    logger.info("  STORAGE STATUS:")
    logger.info(f"RUNS_ON_GITHUB                   ={RUNS_ON_GITHUB}")
    logger.info(f"LOCAL_STORAGE_TESTS_ENABLED      ={LOCAL_STORAGE_TESTS_ENABLED}")
    logger.info(f"STORAGE_LMDB                     ={STORAGE_LMDB}")
    logger.info(f"STORAGE_MEM                      ={STORAGE_MEM}")
    logger.info(f"STORAGE_S3  (SIMULATED)          ={STORAGE_SIM_S3}")
    logger.info(f"STORAGE_GCP (SIMULATED)          ={STORAGE_SIM_GCP}")
    logger.info(f"STORAGE_AZURITE                  ={STORAGE_AZURITE}")
    logger.info(f"STORAGE_NFS                      ={STORAGE_NFS}")
    logger.info(f"STORAGE_MONGO                    ={STORAGE_MONGO}")
    logger.info(f"PERSISTENT_STORAGE_TESTS_ENABLED ={PERSISTENT_STORAGE_TESTS_ENABLED}")
    logger.info(f"STORAGE_AWS_S3                   ={STORAGE_AWS_S3}")
    logger.info(f"STORAGE_GCP                      ={STORAGE_GCP}")
    logger.info(f"STORAGE_AZURE                    ={STORAGE_AZURE}")
    logger.info(f"Enc.V1                           ={TEST_ENCODING_V1}")
    logger.info(f"Enc.V2                           ={TEST_ENCODING_V2}")
    logger.info("-" * 120)


_MACOS_AZURE_TESTS_SKIP_REASON = (
    "Tests fail for macOS vcpkg builds, either because Azurite is improperly configured"
    "on the CI or because there's problem with Azure SDK for C++ in this configuration."
)
SKIP_CONDA_MARK = pytest.mark.skipif(
    ARCTICDB_USING_CONDA,
    reason="Those tests are skipped on conda",
)
# !!!!!!!!!!!!!!!!!!!!!! Below mark (variable) names should reflect where they will be used, not what they do.
# This is to avoid the risk of the name becoming out of sync with the actual condition.
SLOW_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or DISABLE_SLOW_TESTS, reason="Skipping test as it takes a long time to run"
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
    not STORAGE_SIM_S3,
    reason="Ability to disable local storages - simulates s3 is disabled",
)
"""Mark on tests using GCP model storage.
"""
SIM_GCP_TESTS_MARK = pytest.mark.skipif(
    not STORAGE_SIM_GCP,
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
    not STORAGE_MEM,
    reason="Ability to disable local storages - mem storage is disabled",
)
"""Mark on tests using the NFS model storage.
"""
SIM_NFS_TESTS_MARK = pytest.mark.skipif(
    not STORAGE_NFS,
    reason="Ability to disable local storages - simulated nfs is disabled",
)
"""Mark on tests using the real GCP storage.
"""
AZURE_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or MACOS or not STORAGE_AZURITE, reason=_MACOS_AZURE_TESTS_SKIP_REASON
)
"""Mark to skip all Azure tests when MACOS or ARCTICDB_FAST_TESTS_ONLY is set."""

# Mongo tests will run under local storage tests
MONGO_TESTS_MARK = pytest.mark.skipif(
    FAST_TESTS_ONLY or (not LINUX) or (not STORAGE_MONGO),
    reason="Skipping mongo tests under ARCTICDB_FAST_TESTS_ONLY and if local storage tests are disabled",
)
"""Mark on tests or fixtures that need to skip V1 encoding tests
"""
TEST_ENCODING_V1_MARK = pytest.mark.skipif(
    not TEST_ENCODING_V1,
    reason="Ability to disable encoding tests - V1 is disabled",
)
"""Mark on tests or fixtures that need to skip V2 encoding tests
"""
TEST_ENCODING_V2_MARK = pytest.mark.skipif(
    not TEST_ENCODING_V2,
    reason="Ability to disable encoding tests - V2 is disabled",
)

"""Windows and MacOS have different handling of self-signed CA cert for test.
TODO: https://github.com/man-group/ArcticDB/issues/1394"""
SSL_TEST_SUPPORTED = sys.platform == "linux"

FORK_SUPPORTED = pytest.mark.skipif(WINDOWS, reason="Fork not supported on Windows")

## MEMRAY supports linux and macos
MEMRAY_SUPPORTED = MACOS or LINUX
MEMRAY_TESTS_MARK = pytest.mark.skipif(not MEMRAY_SUPPORTED, reason="MEMRAY supports linux and macos")

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


def xfail_azure_chars(nvs, symbol_name):

    def contains_problem_chars(text: str) -> list:
        target_chars = [chr(c) for c in range(0, 32)] + [chr(126), chr(127), chr(140), chr(142), chr(143), chr(156)]
        found = [(ord(char), repr(char)) for char in text if char in target_chars]
        return found

    storage_is_azure = False
    try:
        storage_is_azure = "azure" in nvs.get_backing_store()
    except:
        # for v2 API
        storage_is_azure = "azure" in nvs._nvs.get_backing_store()
    if (storage_is_azure) and (contains_problem_chars(symbol_name)):
        pytest.xfail("Char is not supported by azure and filtered by arcticdb (see 9669996138)")
