import logging
import os
import re
import shutil
import subprocess
import tempfile
import venv
import pandas as pd
import numpy as np


from typing import Dict, List, Optional, Union

from arcticdb_ext.exceptions import StorageException
from packaging.version import Version
from arcticdb_ext import set_config_int, unset_config_int
from arcticdb.arctic import Arctic

logger = logging.getLogger("Compatibility tests")


class ErrorInVenv(Exception):
    """To signal errors occuring from within the venv"""


def is_running_on_windows():
    return os.name == "nt"


def run_shell_command(
    command: List[Union[str, os.PathLike]], cwd: Optional[os.PathLike] = None
) -> subprocess.CompletedProcess:
    logger.info(f"Executing command: {command}")
    result = None
    if is_running_on_windows():
        # shell=True is required for running the correct python executable on Windows
        result = subprocess.run(command, cwd=cwd, capture_output=True, shell=True)
    else:
        # On linux we need shell=True for conda feedstock runners (because otherwise they fail to expand path variables)
        # But to correctly work with shell=True we need a single command string.
        command_string = " ".join(command)
        result = subprocess.run(
            command_string,
            cwd=cwd,
            capture_output=True,
            shell=True,
            stdin=subprocess.DEVNULL,
        )
    if result.returncode != 0:
        logger.error(
            f"Command '{command_string}' failed with return code {result.returncode}\n"
            f"stdout:\n{result.stdout.decode('utf-8')}\nstderr:\n{result.stderr.decode('utf-8')}"
        )
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
        command = [
            get_os_specific_venv_python(),
            "-m",
            "pip",
            "install",
            "-r",
            self.requirements_file,
        ]
        run_shell_command(command, self.path)

    def tear_down_venv(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def execute_python_file(
        self, python_path: Union[str, os.PathLike]
    ) -> subprocess.CompletedProcess:
        command = [get_os_specific_venv_python(), python_path]
        return run_shell_command(command, self.path)

    def create_arctic(self, uri: str) -> "VenvArctic":
        return VenvArctic(self, uri)


class VenvArctic:
    def __init__(self, venv, uri):
        self.venv = venv
        self.uri = uri
        self.init_storage()
        self.parquet = False # True is for parquet format False is for CSV

    def add_traceability_prints(self, python_commands):
        """
        Adds prints before and after each python command. This will help with debugging segfaults occurring within the
        venv as it will show which operation failed in the logs.
        """
        result = []
        for command in python_commands:
            result.append(f"print('About to run:', {repr(command)})")
            result.append(command)
            result.append(f"print('Done with:', {repr(command)})")
        return result

    def execute(self, python_commands: List[str], dfs: Optional[Dict] = None) -> None:
        """
        Prepares the dataframe parquet files and the python script to be run from within the venv.
        """

        def get_column_dtypes(df: pd.DataFrame) -> dict:
            """
            Returns a dictionary mapping column names to their data types in the DataFrame.
            """
            return df.dtypes.astype(str).to_dict()

        if dfs is None:
            dfs = {}

        with tempfile.TemporaryDirectory() as dir:
            df_load_commands = []
            index_load_commands = []
            for df_name, df_val in dfs.items():
                # For each dataframe we are writing we create a deep copy which we pass
                df: pd.DataFrame = df_val.copy(deep=True)
                if self.parquet:
                    file = os.path.join(dir, f"{df_name}.parquet")
                    df.to_parquet(file)
                    command = f"{df_name} = pd.read_parquet({repr(file)})"
                else: 
                    # CSV format does not preserve metadata of DatetimeIndex and RangeIndex
                    # Therefore after the load from CSV we have to reconstruct the index metadata
                    # so that dataframes become fully equal
                    file = os.path.join(dir, f"{df_name}.gz")
                    df.to_csv(file, index=True)
                    dtypes_dict = get_column_dtypes(df)
                    if df.index is not None:
                        if isinstance(df.index, pd.DatetimeIndex) and np.issubdtype(df.index.dtype, np.datetime64):
                            index_load_commands.append(f"{df_name}.index.freq = {repr(df.index.freqstr)}")
                        elif isinstance(df.index, pd.RangeIndex):
                            index_load_commands = [ 
                                                   f"{df_name}.index.start = {df.index.start}",
                                                   f"{df_name}.index.stop = {df.index.stop}",
                                                   f"{df_name}.index.step = {df.index.step}",
                                                ]
                        else:
                            index_load_commands
                        command = f"{df_name} = pd.read_csv({repr(file)}, dtype={dtypes_dict}, parse_dates=True, index_col=0)"
                    else:
                        command = f"{df_name} = pd.read_csv({repr(file)}, dtype={dtypes_dict}, parse_dates=True)"
                df_load_commands.append(command)

            python_commands = (
                [
                    "from arcticdb import Arctic",
                    "import pandas as pd",
                    "import numpy as np",
                    f"ac = Arctic({repr(self.uri)})",
                ]
                + df_load_commands
                + index_load_commands
                + python_commands
            )

            python_commands = self.add_traceability_prints(python_commands)

            python_path = os.path.join(dir, "run.py")
            with open(python_path, "w") as python_file:
                python_file.write("\n".join(python_commands))

            result = self.venv.execute_python_file(python_path)
            if result.returncode != 0:
                raise ErrorInVenv(
                    f"Executing {python_commands} failed with return code {result.returncode}: {result}"
                )

    def init_storage(self):
        self.execute([])

    def create_library(self, lib_name: str) -> "VenvLib":
        return VenvLib(self, lib_name, create_if_not_exists=True)

    def get_library(self, lib_name: str) -> "VenvLib":
        return VenvLib(self, lib_name, create_if_not_exists=False)


class VenvLib:
    def __init__(self, arctic, lib_name, create_if_not_exists=True):
        self.arctic = arctic
        self.lib_name = lib_name
        if create_if_not_exists:
            self.create_lib()

    def create_lib(self):
        self.arctic.execute([f"ac.create_library('{self.lib_name}')"])

    def execute(self, python_commands: List[str], dfs: Optional[Dict] = None) -> None:
        python_commands = [
            f"lib = ac.get_library('{self.lib_name}')",
        ] + python_commands
        return self.arctic.execute(python_commands, dfs)

    def write(self, sym: str, df) -> None:
        return self.execute([f"lib.write('{sym}', df)"], {"df": df})

    def update(self, sym: str, df, date_range: str):
        return self.execute([f"lib.update('{sym}', df, date_range={date_range})"], {"df": df})

    def assert_read(self, sym: str, df) -> None:
        python_commands = [
            f"read_df = lib.read('{sym}').data",
            "print(read_df)",
            "pd.testing.assert_frame_equal(read_df, expected_df)",
        ]
        return self.execute(python_commands, {"expected_df": df})


class CurrentVersion:
    """
    For many of the compatibility tests we need to maintain a single open connection to the library.
    For example LMDB on Windows starts to fail if we at the same time we use an old_venv and current connection.

    So we use `with CurrentVersion` construct to ensure we delete all our outstanding references to the library.
    """
    def __init__(self, uri, lib_name):
        self.uri = uri
        self.lib_name = lib_name

    def __enter__(self):
        set_config_int("VersionMap.ReloadInterval", 0) # We disable the cache to be able to read the data written from old_venv
        self.ac = Arctic(self.uri)
        self.lib = self.ac.get_library(self.lib_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        unset_config_int("VersionMap.ReloadInterval")
        del self.lib
        del self.ac


class CompatLibrary:
    """
    Responsible for creating and cleaning up a library (or multiple libraries) when writing compatibility tests.

    Usually the library cleanup is managed by pytest fixtures, but it is hard to do so for compatibility tests because
    they are responsible for maintaining their own arctic instances across several processes. As long as libraries
    within the test are wrapped in a CompatLibrary cleanup will be dealt with.
    """
    def __init__(self, old_venv : Venv, uri : str, lib_names : Union[str, List[str]], create_with_current_version=False):
        self.old_venv = old_venv
        self.uri = uri
        self.lib_names = lib_names if isinstance(lib_names, list) else [lib_names]
        assert len(self.lib_names) > 0
        self.create_with_current_version = create_with_current_version

    def __enter__(self):
        for lib_name in self.lib_names:
            if self.create_with_current_version:
                ac = Arctic(self.uri)
                ac.create_library(lib_name)
                del ac
            else:
                old_ac = self.old_venv.create_arctic(self.uri)
                old_ac.create_library(lib_name)

        self.old_ac = self.old_venv.create_arctic(self.uri)
        self.old_libs = {lib_name : self.old_ac.get_library(lib_name) for lib_name in self.lib_names}
        self.old_lib = self.old_libs[self.lib_names[0]]
        return self

    def current_version(self):
        return CurrentVersion(self.uri, self.lib_names[0])

    def __exit__(self, exc_type, exc_val, exc_tb):
        ac = Arctic(self.uri)
        for lib_name in self.lib_names:
            try:
                ac.delete_library(lib_name)
            except StorageException as e:
                if self.uri.startswith("lmdb"):
                    # For lmdb on windows sometimes the venv process keeps a dangling reference to the library for some
                    # time. Since we use temporary directories for lmdb it's fine to not retry because the temporary
                    # directory will be cleaned up in the end of the pytest process.
                    logger.info(f"Failed to delete an lmdb library due to a Storage Exception: {e}")
                else:
                    raise
        del ac
