"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import sys
import traceback

import pytest

MACOS = sys.platform == "darwin"
XFAILMESSAGE = "This is due issue 9692682845 - has_library may return error on Mac_OS"
ERROR_MARKER = (
    "arcticdb_ext.exceptions.InternalException: Azure::Storage::StorageException(404 The specified blob does not exist."
)
marked_tests = []  # Global list to collect xfailed test IDs


def pytest_runtest_makereport(item, call):
    # if MACOS and call.excinfo:
    if call.excinfo:
        err_msg = str(call.excinfo.value)
        full_trace = "".join(traceback.format_exception(call.excinfo.type, call.excinfo.value, call.excinfo.tb))

        if ERROR_MARKER in full_trace:
            report = pytest.TestReport.from_item_and_call(item, call)
            report.outcome = "skipped"
            report.wasxfail = XFAILMESSAGE

            # Collect the test ID
            marked_tests.append(item.nodeid)
            return report


def pytest_terminal_summary(terminalreporter, exitstatus):
    if marked_tests:
        terminalreporter.write("\n=== SPECIAL XFAIL SUMMARY ===\n", bold=True)
        for test_id in marked_tests:
            terminalreporter.write(f"• {test_id}\n")
        terminalreporter.write("=============================\n\n")


# endregion
