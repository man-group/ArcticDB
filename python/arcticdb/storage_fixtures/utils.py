"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import multiprocessing
import shutil
import subprocess
import os
import platform
import sys
import requests
import signal
import itertools
import re
import socketserver
import time
import warnings
from typing import Union, Any
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
import trustme

_WINDOWS = platform.system() == "Windows"
_MACOS = sys.platform.lower().startswith("darwin")
_LINUX = sys.platform.lower().startswith("linux")
_DEBUG = os.getenv("ACTIONS_RUNNER_DEBUG", default=None) in (1, "True")

import logging

logger = logging.getLogger("Utils")


_PORT_RANGE_START = 10000
_PORT_BLOCK_SIZE = 100
_MAX_XDIST_WORKERS = 16
_SEED_BLOCKS = 32
_port_call_counter = itertools.count()


def _xdist_worker_index():
    """0 for gw0, 1 for gw1, ... and 0 when not running under xdist."""
    match = re.fullmatch(r"gw(\d+)", os.getenv("PYTEST_XDIST_WORKER", ""))
    return int(match.group(1)) % _MAX_XDIST_WORKERS if match else 0


def get_ephemeral_port(seed=0):
    """A probably-free port, distinct from every other port this test run hands out.

    Two callers used to be able to pick the same number, so this held the socket open for 20s to give the loser a
    chance to notice. That sleep ran on every moto, azurite and mongod startup and cost ~22% of the integration
    suite. Instead, give each (seed, xdist worker) a disjoint block and walk it, which makes a collision within a
    run impossible by construction rather than merely unlikely. The bind is still only a probe - the caller binds
    for real afterwards - so callers keep their existing retries for the case where something outside this run
    takes the port in between.
    """
    block_start = (
        _PORT_RANGE_START + ((seed % _SEED_BLOCKS) * _MAX_XDIST_WORKERS + _xdist_worker_index()) * _PORT_BLOCK_SIZE
    )
    # Offset by the pid so two independent runs on one machine do not start at the same end of the block
    first = (os.getpid() + next(_port_call_counter)) % _PORT_BLOCK_SIZE
    for probe in range(_PORT_BLOCK_SIZE):
        port = block_start + (first + probe) % _PORT_BLOCK_SIZE
        try:
            with socketserver.TCPServer(("localhost", port), None):
                pass
        except OSError as e:
            print(repr(e), file=sys.stderr)
            continue
        return port
    raise Exception(f"Cannot find a free port in {block_start}-{block_start + _PORT_BLOCK_SIZE} for seed {seed}")


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
    def start_with_retry(url: str, service_name: str, num_retries: int, timeout: int, process_start_cmd: str, **kwargs):
        """Attempts to start the process up to specified times.

        Each time will wait for service to be avil at specified url up to the specified timeout"""
        for i in range(num_retries):  # retry in case of connection problems
            try:
                p = GracefulProcessUtils.start(process_start_cmd, **kwargs)
                wait_for_server_to_come_up(url, service_name, p, timeout=timeout)
                return p
            except AssertionError as ex:
                logger.error(ex)
                try:
                    p.terminate()
                except:
                    pass

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
    if process is None:
        alive = lambda: True
    else:
        alive = (lambda: process.poll() is None) if isinstance(process, subprocess.Popen) else process.is_alive
    while True:
        assert time.time() < deadline, f"Timed out waiting for {service} process to start"
        assert alive(), service + " process died shortly after start up"
        time.sleep(sleep)
        try:
            response = requests.get(url, timeout=req_timeout, verify=False)  # head() confuses Mongo
            if response.status_code < 500:  # We might not have permission, so not requiring 2XX response
                break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pass


class ExceptionInCleanUpWarning(Warning):
    pass


@dataclass
class handle_cleanup_exception(AbstractContextManager):
    """Provides uniform warning containing the given arguments for exceptions in cleanup/__exit__ calls."""

    fixture: Any
    item: Any = ""
    consequence: str = ""
    had_exception: bool = field(default=False, repr=False)

    def __exit__(self, exc_type, e, _):
        if exc_type:
            self.had_exception = True
            warning = ExceptionInCleanUpWarning(f"Error while cleaning up {self}: {exc_type.__qualname__}: {e}")
            warning.__cause__ = e
            warnings.warn(warning)
            return not _DEBUG


def safer_rmtree(fixture, path):
    """Compared to ``shutil.rmtree(ignore_errors=False)`` this will log a warning, so we know something is buggy"""
    handler = handle_cleanup_exception(fixture, "files", consequence="Disk might fill up")
    with handler:
        shutil.rmtree(path, ignore_errors=False)
    if handler.had_exception:
        time.sleep(1)
        with handler:  # Even with ignore_errors=True, rmtree might still throw on Windows....
            shutil.rmtree(path, ignore_errors=True)


def get_ca_cert_for_testing(working_dir):
    key_file = os.path.join(working_dir, "key.pem")
    cert_file = os.path.join(working_dir, "cert.pem")
    client_cert_file = os.path.join(working_dir, "client.pem")
    ca = trustme.CA()
    # 127.0.0.1 as well as the name: fixtures connect by IPv4 literal to avoid the ~2s ::1 fallback on Windows
    server_cert = ca.issue_cert("localhost", "127.0.0.1")
    server_cert.private_key_pem.write_to_path(key_file)
    server_cert.cert_chain_pems[0].write_to_path(cert_file)
    ca.cert_pem.write_to_path(client_cert_file)
    # Create the sym link for curl CURLOPT_CAPATH option; rehash only available on openssl >=1.1.1
    subprocess.run(
        f'ln -s "{client_cert_file}" "$(openssl x509 -hash -noout -in "{client_cert_file}")".0',
        cwd=working_dir,
        shell=True,
    )
    return ca, key_file, cert_file, client_cert_file  # Need to keep ca alive to authenticate the cert
