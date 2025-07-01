"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import enum
from typing import Callable, Generator, Union
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.version_store.library import Library
import hypothesis
import os
import threading
import pytest
import pandas as pd
import platform
import random
import re
import time
import requests
import uuid
from datetime import datetime
from functools import partial
from tempfile import mkdtemp

from arcticdb import LibraryOptions
from arcticdb.storage_fixtures.api import StorageFixture
from arcticdb.storage_fixtures.azure import AzureContainer, AzuriteStorageFixtureFactory
from arcticdb.storage_fixtures.lmdb import LmdbStorageFixture
from arcticdb.storage_fixtures.s3 import (
    BaseGCPStorageFixtureFactory,
    BaseS3StorageFixtureFactory,
    GcpS3Bucket,
    MotoS3StorageFixtureFactory,
    MotoGcpS3StorageFixtureFactory,
    MotoNfsBackedS3StorageFixtureFactory,
    NfsS3Bucket,
    S3Bucket,
    real_gcp_from_environment_variables,
    real_s3_from_environment_variables,
    mock_s3_with_error_simulation,
    real_s3_sts_from_environment_variables,
    real_s3_sts_resources_ready,
    real_s3_sts_clean_up,
)
from arcticdb.storage_fixtures.mongo import auto_detect_server
from arcticdb.storage_fixtures.in_memory import InMemoryStorageFixture
from arcticdb_ext.storage import NativeVariantStorage, AWSAuthMethod, S3Settings as NativeS3Settings
from arcticdb_ext import set_config_int
from arcticdb.version_store._normalization import MsgPackNormalizer
from arcticdb.util.test import create_df
from arcticdb.arctic import Arctic
from .util.mark import (
    LMDB_TESTS_MARK,
    MEM_TESTS_MARK,
    SIM_GCP_TESTS_MARK,
    SIM_NFS_TESTS_MARK,
    SIM_S3_TESTS_MARK,
    REAL_GCP_TESTS_MARK,
    WINDOWS,
    AZURE_TESTS_MARK,
    MONGO_TESTS_MARK,
    REAL_S3_TESTS_MARK,
    SSL_TEST_SUPPORTED,
    VENV_COMPAT_TESTS_MARK,
    PANDAS_2_COMPAT_TESTS_MARK,
    MACOS,
    ARCTICDB_USING_CONDA,
)
from arcticdb.storage_fixtures.utils import safer_rmtree
from packaging.version import Version
from arcticdb.util.venv import Venv
import arcticdb.toolbox.query_stats as query_stats


# region =================================== Misc. Constants & Setup ====================================
hypothesis.settings.register_profile("ci_linux", max_examples=100)
hypothesis.settings.register_profile("ci_windows", max_examples=100)
hypothesis.settings.register_profile("ci_macos", max_examples=100)
hypothesis.settings.register_profile("dev", max_examples=100)

hypothesis.settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))

# Use a smaller memory mapped limit for all tests
MsgPackNormalizer.MMAP_DEFAULT_SIZE = 20 * (1 << 20)


# silence warnings about custom markers
def pytest_configure(config):
    config.addinivalue_line("markers", "storage: Mark tests related to storage functionality")
    config.addinivalue_line("markers", "authentication: Mark tests related to authentication functionality")
    config.addinivalue_line("markers", "pipeline: Mark tests related to pipeline functionality")


if platform.system() == "Linux":
    try:
        from ctypes import cdll

        cdll.LoadLibrary("libSegFault.so")
    except:
        pass


@pytest.fixture()
def sym(request: "pytest.FixtureRequest"):
    return request.node.name + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture()
def lib_name(request: "pytest.FixtureRequest") -> str:
    name = re.sub(r"[^\w]", "_", request.node.name)[:30]
    pid = os.getpid()
    thread_id = threading.get_ident()
    # There is limit to the name length, and note that without
    # the dot (.) in the name mongo will not work!
    return f"{name}.{pid}_{thread_id}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_')}_{uuid.uuid4()}"


@pytest.fixture
def get_stderr(capfd):
    from arcticdb_ext.log import flush_all

    def _get():
        flush_all()
        time.sleep(0.001)
        return capfd.readouterr().err

    return _get


@enum.unique
class EncodingVersion(enum.IntEnum):
    V1 = 0
    V2 = 1


@pytest.fixture(scope="session")
def only_test_encoding_version_v1():
    """Dummy fixture to reference at module/class level to reduce test cases"""


def pytest_generate_tests(metafunc):
    if "encoding_version" in metafunc.fixturenames:
        only_v1 = "only_test_encoding_version_v1" in metafunc.fixturenames
        metafunc.parametrize("encoding_version", [EncodingVersion.V1] if only_v1 else list(EncodingVersion))


# endregion
# region ======================================= Storage Fixtures =======================================

@pytest.fixture(scope="session")
def lmdb_shared_storage(tmp_path_factory) -> Generator[LmdbStorageFixture, None, None]:
    tmp_path = tmp_path_factory.mktemp("lmdb")
    with LmdbStorageFixture(tmp_path) as f:
        yield f


@pytest.fixture(scope="function")
def lmdb_storage(tmp_path) -> Generator[LmdbStorageFixture, None, None]:
    with LmdbStorageFixture(tmp_path) as f:
        yield f


@pytest.fixture
def lmdb_library(lmdb_storage, lib_name) -> Generator[Library, None, None]:
    yield lmdb_storage.create_arctic().create_library(lib_name)


@pytest.fixture
def lmdb_library_dynamic_schema(lmdb_storage, lib_name) -> Generator[Library, None, None]:
    yield lmdb_storage.create_arctic().create_library(lib_name, library_options=LibraryOptions(dynamic_schema=True))


@pytest.fixture(
    scope="function",
    params=("lmdb_library", "lmdb_library_dynamic_schema"),
)
def lmdb_library_static_dynamic(request):
    yield request.getfixturevalue(request.param)


@pytest.fixture
def lmdb_library_factory(lmdb_storage, lib_name):
    def f(library_options: LibraryOptions = LibraryOptions()):
        return lmdb_storage.create_arctic().create_library(lib_name, library_options=library_options)

    return f


# ssl is enabled by default to maximize test coverage as ssl is enabled most of the times in real world
@pytest.fixture(scope="session")
def s3_storage_factory() -> Generator[MotoS3StorageFixtureFactory, None, None]:
    with MotoS3StorageFixtureFactory(
        use_ssl=SSL_TEST_SUPPORTED, ssl_test_support=SSL_TEST_SUPPORTED, bucket_versioning=False
    ) as f:
        yield f


@pytest.fixture(scope="session")
def gcp_storage_factory() -> Generator[MotoGcpS3StorageFixtureFactory, None, None]:
    with MotoGcpS3StorageFixtureFactory(
        use_ssl=SSL_TEST_SUPPORTED, ssl_test_support=SSL_TEST_SUPPORTED, bucket_versioning=False
    ) as f:
        yield f


@pytest.fixture(scope="session")
def wrapped_s3_storage_factory() -> Generator[MotoS3StorageFixtureFactory, None, None]:
    with MotoS3StorageFixtureFactory(
        use_ssl=False,
        ssl_test_support=False,
        bucket_versioning=False,
        native_config=NativeVariantStorage(
            NativeS3Settings(AWSAuthMethod.DISABLED, "", True)
        ),  # True: use_internal_client_wrapper_for_testing
    ) as f:
        yield f


@pytest.fixture(scope="session")
def s3_no_ssl_storage_factory() -> Generator[MotoS3StorageFixtureFactory, None, None]:
    with MotoS3StorageFixtureFactory(use_ssl=False, ssl_test_support=SSL_TEST_SUPPORTED, bucket_versioning=False) as f:
        yield f


@pytest.fixture(scope="session")
def s3_ssl_disabled_storage_factory() -> Generator[MotoS3StorageFixtureFactory, None, None]:
    with MotoS3StorageFixtureFactory(use_ssl=False, ssl_test_support=False, bucket_versioning=False) as f:
        yield f


@pytest.fixture(scope="session")
def nfs_backed_s3_storage_factory() -> Generator[MotoNfsBackedS3StorageFixtureFactory, None, None]:
    with MotoNfsBackedS3StorageFixtureFactory(
        use_ssl=False, ssl_test_support=False, bucket_versioning=False, _test_only_is_nfs_layout=True
    ) as f:
        yield f


@pytest.fixture(scope="session")
def s3_storage(s3_storage_factory) -> Generator[S3Bucket, None, None]:
    with s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="function")
def s3_clean_bucket(s3_storage_factory) -> Generator[S3Bucket, None, None]:
    with s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="function")
def azurite_clean_bucket(azurite_storage_factory) -> Generator[S3Bucket, None, None]:
    with azurite_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="function")
def nfs_clean_bucket(nfs_backed_s3_storage_factory) -> Generator[NfsS3Bucket, None, None]:
    with nfs_backed_s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def gcp_storage(gcp_storage_factory) -> Generator[S3Bucket, None, None]:
    with gcp_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def nfs_backed_s3_storage(nfs_backed_s3_storage_factory) -> Generator[NfsS3Bucket, None, None]:
    with nfs_backed_s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def s3_no_ssl_storage(s3_no_ssl_storage_factory) -> Generator[S3Bucket, None, None]:
    with s3_no_ssl_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def s3_ssl_disabled_storage(s3_ssl_disabled_storage_factory) -> Generator[S3Bucket, None, None]:
    with s3_ssl_disabled_storage_factory.create_fixture() as f:
        yield f


# s3 storage is picked just for its versioning capabilities for verifying arcticdb atomicity
# This fixture is made at function scope on purpose
# otherwise there are problems with the moto server and the bucket versioning
@pytest.fixture(scope="function")
def s3_bucket_versioning_storage() -> Generator[S3Bucket, None, None]:
    with MotoS3StorageFixtureFactory(
        use_ssl=False, ssl_test_support=False, bucket_versioning=True
    ) as bucket_versioning_storage:
        with bucket_versioning_storage.create_fixture() as f:
            s3_admin = f.factory._s3_admin
            bucket = f.bucket
            assert s3_admin.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"
            yield f

            # The test exectues too fast for the simulator to cleanup the version objects
            # So we need to manually delete them
            boto_bucket = f.get_boto_bucket()
            boto_bucket.object_versions.delete()
            res = s3_admin.list_object_versions(Bucket=bucket)


@pytest.fixture(scope="session")
def mock_s3_storage_with_error_simulation_factory():
    return mock_s3_with_error_simulation()


@pytest.fixture(scope="session")
def mock_s3_storage_with_error_simulation(mock_s3_storage_with_error_simulation_factory):
    with mock_s3_storage_with_error_simulation_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def real_s3_storage_factory() -> BaseS3StorageFixtureFactory:
    return real_s3_from_environment_variables(
        shared_path=False,
        additional_suffix=f"{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}",
    )


@pytest.fixture(scope="session")
def real_gcp_storage_factory() -> BaseGCPStorageFixtureFactory:
    return real_gcp_from_environment_variables(
        shared_path=False,
        additional_suffix=f"{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}",
    )


@pytest.fixture(
    scope="session",
    params=[pytest.param("real_s3", marks=REAL_S3_TESTS_MARK), pytest.param("real_gcp", marks=REAL_GCP_TESTS_MARK)],
)
def real_storage_factory(request) -> Union[BaseGCPStorageFixtureFactory, BaseGCPStorageFixtureFactory]:
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage_factory")
    return storage_fixture


@pytest.fixture(scope="session")
def real_s3_shared_path_storage_factory() -> BaseS3StorageFixtureFactory:
    return real_s3_from_environment_variables(
        shared_path=True,
        additional_suffix=f"{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}",
    )


@pytest.fixture(scope="session")
def real_gcp_shared_path_storage_factory() -> BaseS3StorageFixtureFactory:
    return real_gcp_from_environment_variables(
        shared_path=True,
        additional_suffix=f"{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}",
    )


@pytest.fixture(scope="session")
def real_s3_storage_without_clean_up(real_s3_shared_path_storage_factory) -> S3Bucket:
    return real_s3_shared_path_storage_factory.create_fixture()


@pytest.fixture(scope="session")
def real_gcp_storage_without_clean_up(real_gcp_shared_path_storage_factory) -> S3Bucket:
    return real_gcp_shared_path_storage_factory.create_fixture()


@pytest.fixture(scope="session")
def real_s3_storage(real_s3_storage_factory) -> Generator[S3Bucket, None, None]:
    with real_s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def real_gcp_storage(real_gcp_storage_factory) -> Generator[GcpS3Bucket, None, None]:
    with real_gcp_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def real_s3_library(real_s3_storage, lib_name) -> Library:
    return real_s3_storage.create_arctic().create_library(lib_name)


@pytest.fixture(scope="session")
def monkeypatch_session():
    from _pytest.monkeypatch import MonkeyPatch

    m = MonkeyPatch()
    yield m
    m.undo()


@pytest.fixture(
    scope="session"
)  # Config loaded at the first ArcticDB binary import, so we need to set it up before any tests
def real_s3_sts_storage_factory(monkeypatch_session) -> Generator[BaseS3StorageFixtureFactory, None, None]:
    profile_name = "sts_test_profile"
    set_config_int("S3Storage.STSTokenExpiryMin", 15)
    # monkeypatch cannot runtime update environment variables in windows as copy of environment is made at startup
    # Need to manually setup credetial beforehand if run locally
    if WINDOWS:
        config_file_path = os.path.expanduser(os.path.join("~", ".aws", "config"))
        f = real_s3_from_environment_variables(False, NativeVariantStorage(), "")
        f.aws_auth = AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER
        f.aws_profile = profile_name
        yield f
    else:
        working_dir = mkdtemp(suffix="S3STSStorageFixtureFactory")
        config_file_path = os.path.join(working_dir, "config")
        sts_test_credentials_prefix = f"{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}"
        username = f"gh_sts_test_user_{sts_test_credentials_prefix}"
        role_name = f"gh_sts_test_role_{sts_test_credentials_prefix}"
        policy_name = f"gh_sts_test_policy_name_{sts_test_credentials_prefix}"
        try:
            f = real_s3_sts_from_environment_variables(
                user_name=username,
                role_name=role_name,
                policy_name=policy_name,
                profile_name=profile_name,
                native_config=NativeVariantStorage(),  # Setting here is purposely wrong to see whether it will get overridden later
                config_file_path=config_file_path,
            )
            # Check is made here as the new user gets authenticated only during being used; the check could be time consuming
            real_s3_sts_resources_ready(
                f
            )  # resources created in iam may not be ready immediately in s3; Could take 10+ seconds
            monkeypatch_session.setenv("AWS_CONFIG_FILE", config_file_path)
            yield f
        finally:
            real_s3_sts_clean_up(role_name, policy_name, username)
            safer_rmtree(None, working_dir)


@pytest.fixture(scope="session")
def real_s3_sts_storage(real_s3_sts_storage_factory) -> Generator[S3Bucket, None, None]:
    with real_s3_sts_storage_factory.create_fixture() as f:
        yield f


# ssl cannot be ON by default due to azurite performance constraints https://github.com/man-group/ArcticDB/issues/1539
@pytest.fixture(scope="session")
def azurite_storage_factory() -> Generator[AzuriteStorageFixtureFactory, None, None]:
    with AzuriteStorageFixtureFactory(use_ssl=False, ssl_test_support=SSL_TEST_SUPPORTED) as f:
        yield f


@pytest.fixture(scope="session")
def azurite_storage(azurite_storage_factory: AzuriteStorageFixtureFactory) -> Generator[AzureContainer, None, None]:
    with azurite_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def azurite_ssl_storage_factory() -> Generator[AzuriteStorageFixtureFactory, None, None]:
    with AzuriteStorageFixtureFactory(use_ssl=True, ssl_test_support=SSL_TEST_SUPPORTED) as f:
        yield f


@pytest.fixture(scope="session")
def azurite_ssl_storage(
    azurite_ssl_storage_factory: AzuriteStorageFixtureFactory,
) -> Generator[AzureContainer, None, None]:
    with azurite_ssl_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def mongo_server():
    with auto_detect_server() as s:
        yield s


@pytest.fixture(scope="session")
def mongo_storage(mongo_server):
    with mongo_server.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def mem_storage() -> Generator[InMemoryStorageFixture, None, None]:
    with InMemoryStorageFixture() as f:
        yield f


# endregion
# region ==================================== `Arctic` API Fixtures ====================================


def filter_out_unwanted_mark(request, current_param):
    """Exclude specific fixture parameters or include only certain

    Provides ability tp filter out unwanted fixture parameter
    To do that add to test as many of the marks you want to stay using:

      @pytest.mark.only_fixture_params([<fixture_param_names_list>], )

    Or alternatively you can exclude as many as wanted:
      @pytest.mark.skip_fixture_params([<fixture_param_names_list>], )
    """
    only_fixtures = request.node.get_closest_marker("only_fixture_params")
    if only_fixtures:
        # Only when defined at least one only_ mark evaluate what should stay
        values_to_include = only_fixtures.args[0]
        if not current_param in values_to_include:
            pytest.skip(reason=f"Skipping {current_param} param as not included in test 'only_fixture_params' mark.")

    skip_fixture_value_mark = request.node.get_closest_marker("skip_fixture_params")
    if skip_fixture_value_mark:
        values_to_skip = skip_fixture_value_mark.args[0]
        if current_param in values_to_skip:
            pytest.skip(reason=f"Skipping param: {current_param} as per 'skip_fixture_params' mark")


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("s3", marks=SIM_S3_TESTS_MARK),
        pytest.param("nfs_backed_s3", marks=SIM_NFS_TESTS_MARK),
        pytest.param("gcp", marks=SIM_GCP_TESTS_MARK),
        pytest.param("lmdb", marks=LMDB_TESTS_MARK),
        pytest.param("mem", marks=MEM_TESTS_MARK),
        pytest.param("azurite", marks=AZURE_TESTS_MARK),
        pytest.param("mongo", marks=MONGO_TESTS_MARK),
        pytest.param("real_s3", marks=REAL_S3_TESTS_MARK),
        pytest.param("real_gcp", marks=REAL_GCP_TESTS_MARK),
    ],
)
def arctic_client(request, encoding_version) -> Arctic:
    filter_out_unwanted_mark(request, request.param)
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=encoding_version)
    return ac


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("s3", marks=SIM_S3_TESTS_MARK),
        pytest.param("nfs_backed_s3", marks=SIM_NFS_TESTS_MARK),
        pytest.param("gcp", marks=SIM_GCP_TESTS_MARK),
        pytest.param("lmdb", marks=LMDB_TESTS_MARK),
        pytest.param("mem", marks=MEM_TESTS_MARK),
        pytest.param("azurite", marks=AZURE_TESTS_MARK),
        pytest.param("mongo", marks=MONGO_TESTS_MARK),
        pytest.param("real_s3", marks=REAL_S3_TESTS_MARK),
        pytest.param("real_gcp", marks=REAL_GCP_TESTS_MARK),
    ],
)
def arctic_client_v1(request) -> Arctic:
    filter_out_unwanted_mark(request, request.param)
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=EncodingVersion.V1)
    return ac


@pytest.fixture(
    scope="function",
    params=["lmdb"],
)
def arctic_client_lmdb(request, encoding_version) -> Arctic:
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=encoding_version)
    return ac


@pytest.fixture
def arctic_library(arctic_client, lib_name) -> Generator[Library, None, None]:
    yield arctic_client.create_library(lib_name)
    arctic_client.delete_library(lib_name)


@pytest.fixture
def arctic_library_dynamic(arctic_client, lib_name) -> Generator[Library, None, None]:
    lib_opts = LibraryOptions(dynamic_schema=True)
    yield arctic_client.create_library(lib_name, library_options=lib_opts)
    arctic_client.delete_library(lib_name)


@pytest.fixture
def arctic_library_v1(arctic_client_v1, lib_name) -> Generator[Library, None, None]:
    yield arctic_client_v1.create_library(lib_name)
    arctic_client_v1.delete_library(lib_name)


@pytest.fixture
def arctic_library_lmdb(arctic_client_lmdb, lib_name) -> Generator[Library, None, None]:
    yield arctic_client_lmdb.create_library(lib_name)
    arctic_client_lmdb.delete_library(lib_name)


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("s3", marks=SIM_S3_TESTS_MARK),
        pytest.param("mem", marks=MEM_TESTS_MARK),
        pytest.param("real_s3", marks=REAL_S3_TESTS_MARK),
        pytest.param("real_gcp", marks=REAL_GCP_TESTS_MARK),
    ],
)
def basic_arctic_client(request, encoding_version) -> Arctic:
    filter_out_unwanted_mark(request, request.param)
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=encoding_version)
    return ac


@pytest.fixture
def arctic_client_lmdb_map_size_100gb(lmdb_storage) -> Arctic:
    storage_fixture: LmdbStorageFixture = lmdb_storage
    storage_fixture.arctic_uri = storage_fixture.arctic_uri + "?map_size=100GB"
    ac = storage_fixture.create_arctic(encoding_version=EncodingVersion.V2)
    return ac


@pytest.fixture
def arctic_library_lmdb_100gb(arctic_client_lmdb_map_size_100gb, lib_name) -> Library:
    return arctic_client_lmdb_map_size_100gb.create_library(lib_name)


@pytest.fixture
def basic_arctic_library(basic_arctic_client, lib_name) -> Library:
    return basic_arctic_client.create_library(lib_name)


# endregion
# region ============================ `NativeVersionStore` Fixture Factories ============================
def _store_factory(lib_name, bucket) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield bucket.create_version_store_factory(lib_name)
    bucket.slow_cleanup()


@pytest.fixture
def version_store_factory(lib_name, lmdb_storage) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, lmdb_storage)


@pytest.fixture
def s3_store_factory_mock_storage_exception(lib_name, s3_storage):
    lib = s3_storage.create_version_store_factory(lib_name)
    endpoint = s3_storage.factory.endpoint
    # `rate_limit` in the uri will trigger the code injected to moto to give http 503 slow down response
    # The following interger is for how many requests until 503 repsonse is sent
    # -1 means the 503 response is disabled
    # Setting persisted throughout the lifetime of moto server, so it needs to be reset
    requests.post(endpoint + "/rate_limit", b"0", verify=False).raise_for_status()
    yield lib
    requests.post(endpoint + "/rate_limit", b"-1", verify=False).raise_for_status()


@pytest.fixture
def s3_store_factory(lib_name, s3_storage) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, s3_storage)


@pytest.fixture
def s3_no_ssl_store_factory(lib_name, s3_no_ssl_storage) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, s3_no_ssl_storage)

@pytest.fixture
def mock_s3_store_with_error_simulation_factory(
    lib_name, mock_s3_storage_with_error_simulation
)  -> Callable[..., NativeVersionStore]:
    # NOTE: this store simulates errors, therefore there is no way to delete it
    return mock_s3_storage_with_error_simulation.create_version_store_factory(lib_name)


@pytest.fixture
def real_s3_store_factory(lib_name, real_s3_storage) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, real_s3_storage)


@pytest.fixture
def nfs_backed_s3_store_factory(lib_name, nfs_backed_s3_storage) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, nfs_backed_s3_storage)


@pytest.fixture
def real_gcp_store_factory(lib_name, real_gcp_storage) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, real_gcp_storage)


@pytest.fixture
def real_s3_sts_store_factory(lib_name, real_s3_sts_storage) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, real_s3_sts_storage)


@pytest.fixture
def azure_store_factory(lib_name, azurite_storage) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, azurite_storage)


@pytest.fixture
def mongo_store_factory(mongo_storage, lib_name) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, mongo_storage)


@pytest.fixture
def in_memory_store_factory(mem_storage, lib_name) -> Generator[Callable[..., NativeVersionStore], None, None]:
    yield from _store_factory(lib_name, mem_storage)


# endregion
# region ================================ `NativeVersionStore` Fixtures =================================
@pytest.fixture
def real_s3_version_store(real_s3_store_factory):
    return real_s3_store_factory()


@pytest.fixture
def real_s3_version_store_dynamic_schema(real_s3_store_factory) -> NativeVersionStore:
    return real_s3_store_factory(dynamic_strings=True, dynamic_schema=True)


@pytest.fixture
def real_gcp_version_store(real_gcp_store_factory) -> NativeVersionStore:
    return real_gcp_store_factory()


@pytest.fixture
def real_gcp_version_store_dynamic_schema(real_gcp_store_factory) -> NativeVersionStore:
    return real_gcp_store_factory(dynamic_strings=True, dynamic_schema=True)


@pytest.fixture
def real_s3_sts_version_store(real_s3_sts_store_factory) -> NativeVersionStore:
    return real_s3_sts_store_factory()


@pytest.fixture
def mock_s3_store_with_error_simulation(mock_s3_store_with_error_simulation_factory) -> NativeVersionStore:
    return mock_s3_store_with_error_simulation_factory()


@pytest.fixture
def mock_s3_store_with_mock_storage_exception(s3_store_factory_mock_storage_exception) -> NativeVersionStore:
    return s3_store_factory_mock_storage_exception()


@pytest.fixture
def nfs_backed_s3_version_store_v1(nfs_backed_s3_store_factory) -> NativeVersionStore:
    return nfs_backed_s3_store_factory(dynamic_strings=True)


@pytest.fixture
def nfs_backed_s3_version_store_v2(nfs_backed_s3_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return nfs_backed_s3_store_factory(
        dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def s3_version_store_v1(s3_store_factory) -> NativeVersionStore:
    return s3_store_factory(dynamic_strings=True)


@pytest.fixture
def s3_version_store_v2(s3_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return s3_store_factory(dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name)


@pytest.fixture
def s3_version_store_dynamic_schema_v1(s3_store_factory) -> NativeVersionStore:
    return s3_store_factory(dynamic_strings=True, dynamic_schema=True)


@pytest.fixture
def s3_version_store_dynamic_schema_v2(s3_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return s3_store_factory(
        dynamic_strings=True, dynamic_schema=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def s3_version_store(s3_version_store_v1, s3_version_store_v2, encoding_version) -> NativeVersionStore:
    if encoding_version == EncodingVersion.V1:
        return s3_version_store_v1
    elif encoding_version == EncodingVersion.V2:
        return s3_version_store_v2
    else:
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


@pytest.fixture
def nfs_backed_s3_version_store_dynamic_schema_v1(nfs_backed_s3_store_factory) -> NativeVersionStore:
    return nfs_backed_s3_store_factory(dynamic_strings=True, dynamic_schema=True)


@pytest.fixture
def nfs_backed_s3_version_store_dynamic_schema_v2(nfs_backed_s3_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return nfs_backed_s3_store_factory(
        dynamic_strings=True, dynamic_schema=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def nfs_backed_s3_version_store(nfs_backed_s3_version_store_v1, nfs_backed_s3_version_store_v2, encoding_version) -> NativeVersionStore:
    if encoding_version == EncodingVersion.V1:
        return nfs_backed_s3_version_store_v1
    elif encoding_version == EncodingVersion.V2:
        return nfs_backed_s3_version_store_v2
    else:
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


@pytest.fixture(scope="function")
def mongo_version_store(mongo_store_factory) -> NativeVersionStore:
    return mongo_store_factory()


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("s3_store_factory", marks=SIM_S3_TESTS_MARK),
        pytest.param("nfs_backed_s3_store_factory", marks=SIM_NFS_TESTS_MARK),
        pytest.param("azure_store_factory", marks=AZURE_TESTS_MARK),
        pytest.param("real_s3_store_factory", marks=REAL_S3_TESTS_MARK),
        pytest.param("real_gcp_store_factory", marks=REAL_GCP_TESTS_MARK),
    ],
)
def object_store_factory(request) -> Callable[..., NativeVersionStore]:
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def object_version_store(object_store_factory) -> NativeVersionStore:
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    return object_store_factory()


@pytest.fixture
def object_version_store_prune_previous(object_store_factory) -> NativeVersionStore:
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    return object_store_factory(prune_previous_version=True)


@pytest.fixture(
    scope="function", params=["s3_store_factory", pytest.param("azure_store_factory", marks=AZURE_TESTS_MARK)]
)
def local_object_store_factory(request):
    """
    Designed to test all local object stores and their simulations
    Doesn't support LMDB or persistent storages
    """
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def local_object_version_store(local_object_store_factory) -> NativeVersionStore:
    """
    Designed to test all local object stores and their simulations
    Doesn't support LMDB or persistent storages
    """
    return local_object_store_factory()


@pytest.fixture
def local_object_version_store_prune_previous(local_object_store_factory) -> NativeVersionStore:
    """
    Designed to test all local object stores and their simulations
    Doesn't support LMDB or persistent storages
    """
    return local_object_store_factory(prune_previous_version=True)


@pytest.fixture(
    params=[
        pytest.param("version_store_factory", marks=LMDB_TESTS_MARK),
        pytest.param("real_gcp_store_factory", marks=REAL_GCP_TESTS_MARK),
        pytest.param("real_s3_store_factory", marks=REAL_S3_TESTS_MARK),
    ]
)
def version_store_and_real_s3_basic_store_factory(request):
    """
    Just the version_store and real_s3 specifically for the test test_interleaved_store_read
    where the in_memory_store_factory is not designed to have this functionality.
    """
    filter_out_unwanted_mark(request, request.param)
    return request.getfixturevalue(request.param)


@pytest.fixture(
    params=[
        pytest.param("version_store_factory", marks=LMDB_TESTS_MARK),
        pytest.param("in_memory_store_factory", marks=MEM_TESTS_MARK),
        pytest.param("real_s3_store_factory", marks=REAL_S3_TESTS_MARK),
        pytest.param("real_gcp_store_factory", marks=REAL_GCP_TESTS_MARK),
    ]
)
def basic_store_factory(request) -> Callable[..., NativeVersionStore]:
    filter_out_unwanted_mark(request, request.param)
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def basic_store(basic_store_factory) -> NativeVersionStore:
    """
    Designed to test the bare minimum of stores
     - LMDB for local storage
     - mem for in-memory storage
     - AWS S3 for persistent storage, if enabled
    """
    return basic_store_factory()


@pytest.fixture
def azure_version_store(azure_store_factory) -> NativeVersionStore:
    return azure_store_factory()


@pytest.fixture
def azure_version_store_dynamic_schema(azure_store_factory) -> NativeVersionStore:
    return azure_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_string_coercion(version_store_factory) -> NativeVersionStore:
    return version_store_factory()


@pytest.fixture
def lmdb_version_store_v1(version_store_factory) -> NativeVersionStore:
    return version_store_factory(dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name)


@pytest.fixture(scope="function", params=("lmdb_version_store_v1", "lmdb_version_store_v2"))
def lmdb_version_store(request) -> Generator[NativeVersionStore, None, None]:
    yield request.getfixturevalue(request.param)


@pytest.fixture
def lmdb_version_store_prune_previous(version_store_factory) -> NativeVersionStore:
    return version_store_factory(dynamic_strings=True, prune_previous_version=True, use_tombstones=True)


@pytest.fixture
def lmdb_version_store_big_map(version_store_factory) -> NativeVersionStore:
    return version_store_factory(lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_very_big_map(version_store_factory) -> NativeVersionStore:
    return version_store_factory(lmdb_config={"map_size": 2**35})


@pytest.fixture
def lmdb_version_store_column_buckets(version_store_factory) -> NativeVersionStore:
    return version_store_factory(dynamic_schema=True, column_group_size=3, segment_row_size=2, bucketize_dynamic=True)


@pytest.fixture
def lmdb_version_store_dynamic_schema_v1(version_store_factory, lib_name) -> NativeVersionStore:
    return version_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_dynamic_schema_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_schema=True, dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def lmdb_version_store_dynamic_schema(
    lmdb_version_store_dynamic_schema_v1, lmdb_version_store_dynamic_schema_v2, encoding_version
) -> NativeVersionStore:
    if encoding_version == EncodingVersion.V1:
        return lmdb_version_store_dynamic_schema_v1
    elif encoding_version == EncodingVersion.V2:
        return lmdb_version_store_dynamic_schema_v2
    else:
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


@pytest.fixture
def lmdb_version_store_empty_types_v1(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v1"
    return version_store_factory(dynamic_strings=True, empty_types=True, name=library_name)


@pytest.fixture
def lmdb_version_store_empty_types_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True, empty_types=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def lmdb_version_store_empty_types_dynamic_schema_v1(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v1"
    return version_store_factory(dynamic_strings=True, empty_types=True, dynamic_schema=True, name=library_name)


@pytest.fixture
def lmdb_version_store_empty_types_dynamic_schema_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True,
        empty_types=True,
        dynamic_schema=True,
        encoding_version=int(EncodingVersion.V2),
        name=library_name,
    )


@pytest.fixture
def lmdb_version_store_delayed_deletes_v1(version_store_factory) -> NativeVersionStore:
    return version_store_factory(
        delayed_deletes=True, dynamic_strings=True, empty_types=True, prune_previous_version=True
    )


@pytest.fixture
def lmdb_version_store_delayed_deletes_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True,
        delayed_deletes=True,
        empty_types=True,
        encoding_version=int(EncodingVersion.V2),
        name=library_name,
    )


@pytest.fixture
def lmdb_version_store_tombstones_no_symbol_list(version_store_factory) -> NativeVersionStore:
    return version_store_factory(use_tombstones=True, dynamic_schema=True, symbol_list=False, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_allows_pickling(version_store_factory, lib_name) -> NativeVersionStore:
    return version_store_factory(use_norm_failure_handler_known_types=True, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_no_symbol_list(version_store_factory) -> NativeVersionStore:
    return version_store_factory(col_per_group=None, row_per_segment=None, symbol_list=False)


@pytest.fixture
def lmdb_version_store_tombstone_and_pruning(version_store_factory) -> NativeVersionStore:
    return version_store_factory(use_tombstones=True, prune_previous_version=True)


@pytest.fixture
def lmdb_version_store_tombstone(version_store_factory) -> NativeVersionStore:
    return version_store_factory(use_tombstones=True)


@pytest.fixture
def lmdb_version_store_tombstone_and_sync_passive(version_store_factory) -> NativeVersionStore:
    return version_store_factory(use_tombstones=True, sync_passive=True)


@pytest.fixture
def lmdb_version_store_ignore_order(version_store_factory) -> NativeVersionStore:
    return version_store_factory(ignore_sort_order=True)


@pytest.fixture
def lmdb_version_store_small_segment(version_store_factory) -> NativeVersionStore:
    return version_store_factory(column_group_size=1000, segment_row_size=1000, lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_tiny_segment(version_store_factory) -> NativeVersionStore:
    return version_store_factory(column_group_size=2, segment_row_size=2, lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_tiny_segment_dynamic(version_store_factory) -> NativeVersionStore:
    return version_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=True)


@pytest.fixture
def basic_store_prune_previous(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(dynamic_strings=True, prune_previous_version=True, use_tombstones=True)


@pytest.fixture
def basic_store_large_data(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(lmdb_config={"map_size": 2**30})


@pytest.fixture
def basic_store_column_buckets(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(dynamic_schema=True, column_group_size=3, segment_row_size=2, bucketize_dynamic=True)


@pytest.fixture
def basic_store_dynamic_schema_v1(basic_store_factory, lib_name) -> NativeVersionStore:
    return basic_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
def basic_store_dynamic_schema_v2(basic_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return basic_store_factory(
        dynamic_schema=True, dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def basic_store_dynamic_schema(
    basic_store_dynamic_schema_v1, basic_store_dynamic_schema_v2, encoding_version
) -> NativeVersionStore:
    if encoding_version == EncodingVersion.V1:
        return basic_store_dynamic_schema_v1
    elif encoding_version == EncodingVersion.V2:
        return basic_store_dynamic_schema_v2
    else:
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


@pytest.fixture
def basic_store_delayed_deletes(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(delayed_deletes=True)


@pytest.fixture
def basic_store_delayed_deletes_v1(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(delayed_deletes=True, dynamic_strings=True, prune_previous_version=True)


@pytest.fixture
def basic_store_delayed_deletes_v2(basic_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return basic_store_factory(
        dynamic_strings=True, delayed_deletes=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def basic_store_tombstones_no_symbol_list(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(use_tombstones=True, dynamic_schema=True, symbol_list=False, dynamic_strings=True)


@pytest.fixture
def basic_store_allows_pickling(basic_store_factory, lib_name) -> NativeVersionStore:
    return basic_store_factory(use_norm_failure_handler_known_types=True, dynamic_strings=True)


@pytest.fixture
def basic_store_no_symbol_list(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(symbol_list=False)


@pytest.fixture
def basic_store_tombstone_and_pruning(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(use_tombstones=True, prune_previous_version=True)


@pytest.fixture
def basic_store_tombstone(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(use_tombstones=True)


@pytest.fixture
def basic_store_tombstone_and_sync_passive(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(use_tombstones=True, sync_passive=True)


@pytest.fixture
def basic_store_ignore_order(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(ignore_sort_order=True)


@pytest.fixture
def basic_store_small_segment(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(column_group_size=1000, segment_row_size=1000, lmdb_config={"map_size": 2**30})


@pytest.fixture
def basic_store_tiny_segment(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(column_group_size=2, segment_row_size=2, lmdb_config={"map_size": 2**30})


@pytest.fixture
def basic_store_tiny_segment_dynamic(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=True)


# endregion
@pytest.fixture
def one_col_df():
    return partial(create_df, columns=1)


@pytest.fixture
def two_col_df():
    return partial(create_df, columns=2)


@pytest.fixture
def three_col_df():
    return partial(create_df, columns=3)


def get_val(col):
    d_type = col % 3
    if d_type == 0:
        return random.random() * 10
    elif d_type == 1:
        return random.random()
    else:
        return str(random.random() * 10)


@pytest.fixture
def get_wide_df():
    def get_df(ts, width, max_col_width):
        cols = random.sample(range(max_col_width), width)
        return pd.DataFrame(index=[pd.Timestamp(ts)], data={str(col): get_val(col) for col in cols})

    return get_df


@pytest.fixture(
    scope="function",
    params=(
        "lmdb_version_store_empty_types_v1",
        "lmdb_version_store_empty_types_v2",
        "lmdb_version_store_empty_types_dynamic_schema_v1",
        "lmdb_version_store_empty_types_dynamic_schema_v2",
    ),
)
def lmdb_version_store_static_and_dynamic(request) -> Generator[NativeVersionStore, None, None]:
    """
    Designed to test all combinations between schema and encoding version for LMDB
    """
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    scope="function",
    params=(
        pytest.param("lmdb_version_store_v1", marks=LMDB_TESTS_MARK),
        pytest.param("lmdb_version_store_v2", marks=LMDB_TESTS_MARK),
        pytest.param("s3_version_store_v1", marks=SIM_S3_TESTS_MARK),
        pytest.param("s3_version_store_v2", marks=SIM_S3_TESTS_MARK),
        pytest.param("in_memory_version_store", marks=MEM_TESTS_MARK),
        pytest.param("nfs_backed_s3_version_store_v1", marks=SIM_NFS_TESTS_MARK),
        pytest.param("nfs_backed_s3_version_store_v2", marks=SIM_NFS_TESTS_MARK),
        pytest.param("azure_version_store", marks=AZURE_TESTS_MARK),
        pytest.param("mongo_version_store", marks=MONGO_TESTS_MARK),
        pytest.param("real_s3_version_store", marks=REAL_S3_TESTS_MARK),
        pytest.param("real_gcp_version_store", marks=REAL_GCP_TESTS_MARK),
    ),
)
def object_and_mem_and_lmdb_version_store(request) -> Generator[NativeVersionStore, None, None]:
    """
    Designed to test all supported stores
    """
    filter_out_unwanted_mark(request, request.param)
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    scope="function",
    params=(
        pytest.param("lmdb_version_store_dynamic_schema_v1", marks=LMDB_TESTS_MARK),
        pytest.param("lmdb_version_store_dynamic_schema_v2", marks=LMDB_TESTS_MARK),
        pytest.param("nfs_backed_s3_version_store_dynamic_schema_v1", marks=SIM_NFS_TESTS_MARK),
        pytest.param("nfs_backed_s3_version_store_dynamic_schema_v2", marks=SIM_NFS_TESTS_MARK),
        pytest.param("s3_version_store_dynamic_schema_v1", marks=SIM_S3_TESTS_MARK),
        pytest.param("s3_version_store_dynamic_schema_v2", marks=SIM_S3_TESTS_MARK),
        pytest.param("in_memory_version_store_dynamic_schema", marks=MEM_TESTS_MARK),
        pytest.param("azure_version_store_dynamic_schema", marks=AZURE_TESTS_MARK),
        pytest.param("real_s3_version_store_dynamic_schema", marks=REAL_S3_TESTS_MARK),
        pytest.param("real_gcp_version_store_dynamic_schema", marks=REAL_GCP_TESTS_MARK),
    ),
)
def object_and_mem_and_lmdb_version_store_dynamic_schema(request) -> Generator[NativeVersionStore, None, None]:
    filter_out_unwanted_mark(request, request.param)
    """
    Designed to test all supported stores
    """
    yield request.getfixturevalue(request.param)


@pytest.fixture
def in_memory_version_store(in_memory_store_factory) -> NativeVersionStore:
    return in_memory_store_factory()


@pytest.fixture
def in_memory_version_store_dynamic_schema(in_memory_store_factory) -> NativeVersionStore:
    return in_memory_store_factory(dynamic_schema=True)


@pytest.fixture
def in_memory_version_store_tiny_segment(in_memory_store_factory) -> NativeVersionStore:
    return in_memory_store_factory(column_group_size=2, segment_row_size=2)


@pytest.fixture(params=["lmdb_version_store_tiny_segment", "in_memory_version_store_tiny_segment"])
def lmdb_or_in_memory_version_store_tiny_segment(request) -> NativeVersionStore:
    return request.getfixturevalue(request.param)


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("1.6.2", marks=VENV_COMPAT_TESTS_MARK),
        pytest.param("4.5.1", marks=VENV_COMPAT_TESTS_MARK),
        pytest.param("5.0.0", marks=VENV_COMPAT_TESTS_MARK),
    ],  # TODO: Extend this list with other old versions
)
def old_venv(request, tmp_path_factory):
    version = request.param

    venvs_dir = tmp_path_factory.mktemp("venvs")
    venv_dir = venvs_dir / version
    requirements_file = os.path.join(os.path.dirname(__file__), "compat", f"requirements-{version}.txt")

    with Venv(venv_dir, requirements_file, version) as old_venv:
        yield old_venv


@pytest.fixture(scope="session", params=[pytest.param("tmp_path_factory", marks=PANDAS_2_COMPAT_TESTS_MARK)])
def pandas_v1_venv(request):
    """A venv with Pandas v1 installed (and an old ArcticDB version). To help test compat across Pandas versions."""
    version = "1.6.2"
    tmp_path_factory = request.getfixturevalue("tmp_path_factory")
    venvs_dir = tmp_path_factory.mktemp("venvs")
    venv_dir = venvs_dir / version
    requirements_file = os.path.join(os.path.dirname(__file__), "compat", f"requirements-{version}.txt")

    with Venv(venv_dir, requirements_file, version) as old_venv:
        yield old_venv


@pytest.fixture(
    scope="session",
    params=[
        "lmdb_shared",
        "s3_ssl_disabled",
        pytest.param("azurite", marks=AZURE_TESTS_MARK),
        pytest.param("mongo", marks=MONGO_TESTS_MARK),
    ],
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


@pytest.fixture(scope="session")
def old_venv_and_arctic_uri(old_venv, arctic_uri):
    if arctic_uri.startswith("mongo") and "1.6.2" in old_venv.version:
        pytest.skip("Mongo storage backend is not supported in 1.6.2")

    if arctic_uri.startswith("lmdb") and Version(old_venv.version) < Version("5.0.0"):
        pytest.skip("LMDB storage backed has a bug in versions before 5.0.0 which leads to flaky segfaults")

    yield old_venv, arctic_uri


@pytest.fixture
def clear_query_stats():
    yield
    query_stats.disable()
    query_stats.reset_stats()
