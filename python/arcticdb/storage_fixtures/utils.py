"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import multiprocessing
import subprocess
import os
import platform
import requests
import signal
import socketserver
import time
import warnings
from typing import Union, Any
from contextlib import contextmanager

_WINDOWS = platform.system() == "Windows"


def get_ephemeral_port():  # https://stackoverflow.com/a/61685162/
    with socketserver.TCPServer(("localhost", 0), None) as s:
        return s.server_address[1]


ProcessUnion = Union[multiprocessing.Process, subprocess.Popen]


class GracefulProcessUtils:
    """Static util functions to start & terminate a process gracefully."""

    @staticmethod
    def start(cmd, **kwargs):
        """Start the subprocess with flags so it can be gracefully ``terminate``d even on Windows"""
        if isinstance(cmd, str) and not kwargs.get("shell"):
            cmd = cmd.split()
        print("About to run:", cmd)
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if _WINDOWS else 0
        return subprocess.Popen(cmd, creationflags=creation_flags, **kwargs)

    @staticmethod
    def wait(p: ProcessUnion, timeout_sec: int):
        try:
            if isinstance(p, multiprocessing.Process):
                p.join(timeout=timeout_sec)
                exitcode = p.exitcode
            else:
                exitcode = p.wait(timeout=timeout_sec)
        except:
            exitcode = None
        return exitcode

    @staticmethod
    def terminate(p: Union[multiprocessing.Process, subprocess.Popen]):
        """If the argument is a ``subprocess``, it must be created using ``start()``.
        Otherwise, on Windows, the CTRL_BREAK_EVENT will terminate all processes connected to the same terminal,
        including any parent process."""
        if _WINDOWS and isinstance(p, subprocess.Popen):
            # On windows p.terminate() == p.kill(), so close the console first to give the process a chance to clean up
            # https://learn.microsoft.com/en-us/windows/console/generateconsolectrlevent
            os.kill(p.pid, signal.CTRL_BREAK_EVENT)
            GracefulProcessUtils.wait(p, 2)
        try:
            p.terminate()
        except:
            pass
        if not _WINDOWS:
            exitcode = GracefulProcessUtils.wait(p, 2)
            if exitcode is None:
                os.kill(p.pid, signal.SIGKILL)  # TODO (python37): use Process.kill()


def wait_for_server_to_come_up(url: str, service: str, process: ProcessUnion, *, timeout=20, sleep=0.2, req_timeout=1):
    deadline = time.time() + timeout
    alive = (lambda: process.poll() is None) if isinstance(process, subprocess.Popen) else process.is_alive
    while True:
        assert time.time() < deadline, f"Timed out waiting for {service} process to start"
        assert alive(), service + " process died shortly after start up"
        time.sleep(sleep)
        try:
            response = requests.get(url, timeout=req_timeout)  # head() confuses Mongo
            if response.status_code < 500:  # We might not have permission
                break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pass


class ExceptionInCleanUpWarning(Warning):
    pass


@contextmanager
def handle_cleanup_exception(fixture, item: Any = "", consequence=""):
    """Provides uniform warning containing the given arguments for exceptions in cleanup/__exit__ calls."""
    try:
        yield
    except Exception as e:
        warning = ExceptionInCleanUpWarning(
            f"Error while cleaning up {item}{' in ' if item else ''}{fixture}. {consequence}{type(e).__qualname__}: {e}"
        )
        warning.__cause__ = e
        warnings.warn(warning)
