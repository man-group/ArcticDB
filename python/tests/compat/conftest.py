import pytest
import subprocess
import os
import venv
import tempfile
import logging
import shutil
from typing import Union, Optional, Dict, List
from ..util.mark import (
    AZURE_TESTS_MARK,
    MONGO_TESTS_MARK,
    VENV_COMPAT_TESTS_MARK,
)
from packaging.version import Version

logger = logging.getLogger("Compatibility tests")

class ErrorInVenv(Exception):
    """To signal errors occuring from within the venv"""

def is_running_on_windows():
    return os.name == 'nt'

def run_shell_command(command : List[Union[str, os.PathLike]], cwd : Optional[os.PathLike] = None) -> subprocess.CompletedProcess:
    logger.info(f"Executing command: {command}")
    result = None
    if is_running_on_windows():
        # shell=True is required for running the correct python executable on Windows
        result = subprocess.run(command, cwd=cwd, capture_output=True, shell=True)
    else:
        # On linux we need shell=True for conda feedstock runners (because otherwise they fail to expand path variables)
        # But to correctly work with shell=True we need a single command string.
        command_string = ' '.join(command)
        result = subprocess.run(command_string, cwd=cwd, capture_output=True, shell=True, stdin=subprocess.DEVNULL)
    if result.returncode != 0:
        logger.warning(f"Command failed, stdout: {str(result.stdout)}, stderr: {str(result.stderr)}")
    return result

def get_os_specific_venv_python() -> str:
    if is_running_on_windows():
        return os.path.join("Scripts", "python.exe")
    else:
        return os.path.join("bin", "python")

class Venv:
    def __init__(self, venv_path, requirements_file, version):
        self.path = venv_path
        self.requirements_file = requirements_file
        self.version = version

    def __enter__(self):
        self.init_venv()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tear_down_venv()

    def init_venv(self):
        venv.create(self.path, with_pip=True, clear=True)
        command = [get_os_specific_venv_python(), "-m", "pip", "install", "-r", self.requirements_file]
        run_shell_command(command, self.path)

    def tear_down_venv(self):
        shutil.rmtree(self.path)

    def execute_python_file(self, python_path : Union[str, os.PathLike]) -> subprocess.CompletedProcess:
        command = [get_os_specific_venv_python(), python_path]
        return run_shell_command(command, self.path)

    def create_arctic(self, uri : str) -> 'VenvArctic':
        return VenvArctic(self, uri)


class VenvArctic:
    def __init__(self, venv, uri):
        self.venv = venv
        self.uri = uri
        self.init_storage()

    def execute(self, python_commands : List[str], dfs : Optional[Dict] = None) -> None:
        """
        Prepares the dataframe parquet files and the python script to be run from within the venv.
        """
        if dfs is None:
            dfs = {}

        with tempfile.TemporaryDirectory() as dir:
            df_load_commands = []
            for df_name, df_value in dfs.items():
                parquet_file = os.path.join(dir, f"{df_name}.parquet")
                df_value.to_parquet(parquet_file)
                df_load_commands.append(f"{df_name} = pd.read_parquet({repr(parquet_file)})")

            python_commands = [
                "from arcticdb import Arctic",
                "import pandas as pd",
                "import numpy as np",
                f"ac = Arctic({repr(self.uri)})"
            ] + df_load_commands + python_commands

            python_path = os.path.join(dir, "run.py")
            with open(python_path, "w") as python_file:
                python_file.write("\n".join(python_commands))

            result = self.venv.execute_python_file(python_path)
            if result.returncode != 0:
                raise ErrorInVenv(f"Executing {python_commands} failed with return code {result.returncode}")

    def init_storage(self):
        self.execute([])

    def create_library(self, lib_name : str) -> 'VenvLib':
        return VenvLib(self, lib_name, create_if_not_exists=True)

    def get_library(self, lib_name : str) -> 'VenvLib':
        return VenvLib(self, lib_name, create_if_not_exists=False)


class VenvLib:
    def __init__(self, arctic, lib_name, create_if_not_exists=True):
        self.arctic = arctic
        self.lib_name = lib_name
        if create_if_not_exists:
            self.create_lib()

    def create_lib(self):
        self.arctic.execute([f"ac.create_library('{self.lib_name}')"])

    def execute(self, python_commands : List[str], dfs : Optional[Dict] = None) -> None:
        python_commands = [
            f"lib = ac.get_library('{self.lib_name}')",
        ] + python_commands
        return self.arctic.execute(python_commands, dfs)

    def write(self, sym : str, df) -> None:
        return self.execute([f"lib.write('{sym}', df)"], {"df": df})

    def assert_read(self, sym : str, df) -> None:
        python_commands = [
            f"read_df = lib.read('{sym}').data",
            "pd.testing.assert_frame_equal(read_df, expected_df)"
        ]
        return self.execute(python_commands, {"expected_df": df})


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("1.6.2", marks=VENV_COMPAT_TESTS_MARK),
        pytest.param("4.5.1", marks=VENV_COMPAT_TESTS_MARK),
    ] # TODO: Extend this list with other old versions
)
def old_venv(request):
    version = request.param
    path = os.path.join("venvs", version)
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


@pytest.fixture()
def old_venv_and_arctic_uri(old_venv, arctic_uri):
    if Version(old_venv.version) <= Version("4.5.0") and arctic_uri.startswith("mongo"):
        pytest.skip("Mongo storage backend has a desctruction bug present until 4.5.0, which can cause flaky segfaults.")
    if Version(old_venv.version) <= Version("4.5.0") and arctic_uri.startswith("lmdb"):
        pytest.skip("LMDB storage backend has a desctruction bug present until 4.5.0, which can cause flaky segfaults.")
    if arctic_uri.startswith("azure"):
        # TODO: Once #1979 is understood and fixed reenable azure on versions which have the fix.
        pytest.skip("Azure storage backend has probable a desctruction bug, which can cause flaky segfaults.")

    return old_venv, arctic_uri
