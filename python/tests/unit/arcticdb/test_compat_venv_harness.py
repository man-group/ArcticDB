"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, Version 2.0.
"""

import sys

import pytest

from arcticdb.util.venv import ErrorInVenv, VenvArctic, is_running_on_windows, run_shell_command

CRASH_AT_EXIT = "import atexit, os, signal; atexit.register(lambda: os.kill(os.getpid(), signal.SIGSEGV))"


class CurrentPythonVenv:
    """Runs the harness scripts with this interpreter instead of an old-client venv."""

    def __init__(self, path):
        self.path = path
        self.last_stdout = None

    def execute_python_file(self, python_path):
        self.last_stdout = run_shell_command([sys.executable, python_path], self.path).stdout.decode()


@pytest.fixture
def venv_arctic(tmp_path):
    return VenvArctic(CurrentPythonVenv(tmp_path), f"lmdb://{tmp_path / 'db'}")


@pytest.mark.skipif(is_running_on_windows(), reason="The harness only skips finalization on POSIX")
def test_crash_during_interpreter_shutdown_is_not_a_failure(venv_arctic):
    venv_arctic.execute([CRASH_AT_EXIT, "x = 1"])
    assert venv_arctic.venv.last_stdout.rstrip().endswith("Done with: x = 1")


@pytest.mark.parametrize("failing_command, return_code", [("assert False", 1), ("import sys; sys.exit(3)", 3)])
def test_failing_command_still_fails(venv_arctic, failing_command, return_code):
    with pytest.raises(ErrorInVenv, match=f"failed with return code {return_code}\\b"):
        venv_arctic.execute([failing_command, "print('unreachable')"])
