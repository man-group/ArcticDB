"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import subprocess
import sys
import textwrap

from arcticdb_ext.log import log, LogLevel, LoggerId


def test_cpp_log_lines_follow_fd_capture(tmp_path, get_stderr):
    # pytest's capfd dup2()s a temp file over fd 2 through Python's CRT. On Windows arcticdb_ext has its own static
    # CRT, so its fd 2 still pointed at the original stderr handle, which Python's CRT had closed and Windows had reused
    # for the next opened file (LMDB's data.mdb) - log lines were written into the database. The log line must land
    # in the capture.
    #
    # The decoy is a file opened before the line is logged and checked to be untouched afterwards. It is best effort
    # here: the handle value the static CRT still holds for fd 2 was freed when pytest started capturing, long before
    # this test, so whichever file has it by now is not under this test's control. test_cpp_log_lines_follow_dup2
    # below sets the sequence up itself.
    marker = "arcticdb-log-capture-marker-7e1c"
    decoy_path = tmp_path / "decoy.bin"
    with open(decoy_path, "wb"):
        log(LoggerId.ROOT, LogLevel.ERROR, marker)
        # get_stderr flushes the loggers before reading, as the other stderr-asserting tests do
        assert marker in get_stderr()
    assert decoy_path.stat().st_size == 0


def test_cpp_log_lines_follow_dup2(tmp_path):
    # The production sequence, in a process this test controls: the console sink exists (first log line), then fd 2
    # is dup2()d over through Python's CRT, which closes the handle it held; the very next file opened - the decoy -
    # is the first candidate to be given that handle value, as LMDB's data.mdb was under pytest. The line logged after
    # that must be in the capture and the decoy must be empty.
    marker = "arcticdb-log-dup2-marker-3b9f"
    capture_path = tmp_path / "capture.txt"
    decoy_path = tmp_path / "decoy.bin"
    script = textwrap.dedent(f"""
        import os
        from arcticdb_ext.log import log, LogLevel, LoggerId, flush_all

        log(LoggerId.ROOT, LogLevel.ERROR, "sink created before the redirect")
        flush_all()
        capture_fd = os.open({str(capture_path)!r}, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        os.dup2(capture_fd, 2)
        decoy_fd = os.open({str(decoy_path)!r}, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        os.close(capture_fd)
        log(LoggerId.ROOT, LogLevel.ERROR, {marker!r})
        flush_all()
        os.close(decoy_fd)
        """)
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert marker in capture_path.read_text()
    assert os.path.getsize(decoy_path) == 0
