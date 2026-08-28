"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb_ext.log import log, LogLevel, LoggerId


def test_cpp_log_lines_follow_fd_capture(capfd):
    # pytest's capfd dup2()s a temp file over fd 2 through Python's CRT. On Windows arcticdb_ext has its own static
    # CRT, so its fd 2 still pointed at the original stderr handle, which Python's CRT had closed and Windows had reused
    # for the next opened file (LMDB's data.mdb) - log lines were written into the database. The log line must land
    # in the capture.
    marker = "arcticdb-log-capture-marker-7e1c"
    log(LoggerId.ROOT, LogLevel.ERROR, marker)
    err = capfd.readouterr().err
    assert marker in err
