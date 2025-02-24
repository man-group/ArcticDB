import pytest
import os
from packaging.version import Version
from arcticdb.util.venv import Venv
from ..util.mark import (
    AZURE_TESTS_MARK,
    MONGO_TESTS_MARK,
    VENV_COMPAT_TESTS_MARK,
    PANDAS_2_COMPAT_TESTS_MARK
)

@pytest.fixture(
    # scope="session",
    params=[
        pytest.param("1.6.2", marks=VENV_COMPAT_TESTS_MARK),
        pytest.param("4.5.1", marks=VENV_COMPAT_TESTS_MARK),
        pytest.param("5.0.0", marks=VENV_COMPAT_TESTS_MARK),
    ],  # TODO: Extend this list with other old versions
)
def old_venv(request, tmp_path):
    version = request.param
    path = os.path.join("venvs", tmp_path, version)
    compat_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_file = os.path.join(compat_dir, f"requirements-{version}.txt")
    with Venv(path, requirements_file, version) as old_venv:
        yield old_venv


@pytest.fixture(
    params=[
        pytest.param("tmp_path", marks=PANDAS_2_COMPAT_TESTS_MARK)
    ]
)
def pandas_v1_venv(request):
    """A venv with Pandas v1 installed (and an old ArcticDB version). To help test compat across Pandas versions."""
    version = "1.6.2"
    tmp_path = request.getfixturevalue(request.param)
    path = os.path.join("venvs", tmp_path, version)
    compat_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_file = os.path.join(compat_dir, f"requirements-{version}.txt")
    with Venv(path, requirements_file, version) as old_venv:
        yield old_venv


@pytest.fixture(
    params=[
        "lmdb",
        "s3_ssl_disabled",
        pytest.param("azurite", marks=AZURE_TESTS_MARK),
        pytest.param("mongo", marks=MONGO_TESTS_MARK),
    ]
)
def arctic_uri(request):
    """
    arctic_uri is a fixture which provides uri to all storages to be used for creating both old and current Arctic instances.

    We use s3_ssl_disabled because which allows running tests for older versions like 1.6.2
    """
    storage_fixture = request.getfixturevalue(request.param + "_storage")
    if request.param == "mongo":
        return storage_fixture.mongo_uri
    else:
        return storage_fixture.arctic_uri


@pytest.fixture
def old_venv_and_arctic_uri(old_venv, arctic_uri):
    if arctic_uri.startswith("mongo") and "1.6.2" in old_venv.version:
        pytest.skip("Mongo storage backend is not supported in 1.6.2")

    if arctic_uri.startswith("lmdb") and Version(old_venv.version) < Version("5.0.0"):
        pytest.skip(
            "LMDB storage backed has a bug in versions before 5.0.0 which leads to flaky segfaults"
        )

    return old_venv, arctic_uri
