"""Tests for parse_ci_failures.py — focuses on log parsing logic."""
import json
import os
import sys
import textwrap
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from parse_ci_failures import (
    parse_gtest_failures,
    parse_pytest_failures,
    filter_infra_steps,
)


# ---------------------------------------------------------------------------
# parse_gtest_failures
# ---------------------------------------------------------------------------
class TestParseGtestFailures:
    def test_single_failure(self):
        log = "some output\n[  FAILED  ] TestSuite.TestName\nmore output"
        assert parse_gtest_failures(log) == {"TestSuite.TestName"}

    def test_parameterised_suffix_stripped(self):
        log = "[  FAILED  ] TestSuite.TestName/0\n[  FAILED  ] TestSuite.TestName/1"
        assert parse_gtest_failures(log) == {"TestSuite.TestName"}

    def test_multiple_failures(self):
        log = (
            "[  FAILED  ] SuiteA.Test1\n"
            "[  FAILED  ] SuiteB.Test2\n"
            "[  FAILED  ] SuiteA.Test1\n"  # duplicate
        )
        assert parse_gtest_failures(log) == {"SuiteA.Test1", "SuiteB.Test2"}

    def test_no_failures(self):
        log = "[  PASSED  ] TestSuite.TestName\nAll tests passed."
        assert parse_gtest_failures(log) == set()

    def test_extra_whitespace_in_brackets(self):
        log = "[   FAILED   ] TestSuite.TestName"
        assert parse_gtest_failures(log) == {"TestSuite.TestName"}

    def test_mixed_with_other_output(self):
        log = textwrap.dedent("""\
            2024-01-15T10:00:00Z Build step output...
            [==========] Running 5 tests from 2 test suites.
            [----------] 3 tests from VersionMap
            [ RUN      ] VersionMap.WriteAndRead
            [  FAILED  ] VersionMap.WriteAndRead (150 ms)
            [ RUN      ] VersionMap.DeleteKey
            [       OK ] VersionMap.DeleteKey (10 ms)
            [  FAILED  ] Codec.RoundtripLz4
        """)
        assert parse_gtest_failures(log) == {
            "VersionMap.WriteAndRead",
            "Codec.RoundtripLz4",
        }

    def test_non_numeric_param_suffix_kept(self):
        """Suffixes like /MyParam should NOT be stripped (only /digits)."""
        log = "[  FAILED  ] TestSuite.TestName/MyParam"
        assert parse_gtest_failures(log) == {"TestSuite.TestName/MyParam"}


# ---------------------------------------------------------------------------
# parse_pytest_failures
# ---------------------------------------------------------------------------
class TestParsePytestFailures:
    def test_single_failure(self):
        log = "FAILED tests/test_foo.py::TestClass::test_method"
        assert parse_pytest_failures(log) == {
            "tests/test_foo.py::TestClass::test_method"
        }

    def test_multiple_failures(self):
        log = (
            "FAILED tests/test_a.py::test_one\n"
            "FAILED tests/test_b.py::test_two\n"
        )
        assert parse_pytest_failures(log) == {
            "tests/test_a.py::test_one",
            "tests/test_b.py::test_two",
        }

    def test_no_failures(self):
        log = "PASSED tests/test_foo.py::test_method\n3 passed in 1.5s"
        assert parse_pytest_failures(log) == set()

    def test_parametrized(self):
        log = "FAILED tests/test_foo.py::test_method[param1-param2]"
        assert parse_pytest_failures(log) == {
            "tests/test_foo.py::test_method[param1-param2]"
        }

    def test_error_lines(self):
        log = "ERROR tests/test_foo.py::test_setup - fixture 'db' not found"
        assert parse_pytest_failures(log) == {
            "tests/test_foo.py::test_setup"
        }

    def test_mixed_failed_and_error(self):
        log = (
            "FAILED tests/test_a.py::test_one\n"
            "ERROR tests/test_b.py::test_two - RuntimeError: boom\n"
        )
        assert parse_pytest_failures(log) == {
            "tests/test_a.py::test_one",
            "tests/test_b.py::test_two",
        }

    def test_mixed_output(self):
        log = textwrap.dedent("""\
            ============================= FAILURES =============================
            tests/test_arctic.py::TestArcticBasic::test_list_libraries FAILED
            FAILED tests/test_arctic.py::TestArcticBasic::test_list_libraries
            FAILED tests/test_arctic.py::TestArcticBasic::test_delete_library
            = 2 failed, 50 passed in 120.3s =
        """)
        assert parse_pytest_failures(log) == {
            "tests/test_arctic.py::TestArcticBasic::test_list_libraries",
            "tests/test_arctic.py::TestArcticBasic::test_delete_library",
        }


# ---------------------------------------------------------------------------
# filter_infra_steps
# ---------------------------------------------------------------------------
class TestFilterInfraSteps:
    def test_filters_test_steps_when_tests_found(self):
        steps = [
            "Install deps",
            "Run pytest",
            "Run ctest",
            "Upload artifacts",
        ]
        result = filter_infra_steps(steps, has_test_failures=True)
        assert result == [
            "Install deps",
            "Upload artifacts",
        ]

    def test_keeps_all_when_no_test_failures(self):
        steps = [
            "Install deps",
            "Run pytest",
        ]
        result = filter_infra_steps(steps, has_test_failures=False)
        assert result == steps

    def test_empty_list(self):
        assert filter_infra_steps([], has_test_failures=True) == []
        assert filter_infra_steps([], has_test_failures=False) == []

    def test_case_insensitive(self):
        steps = ["Run Tests", "Install"]
        result = filter_infra_steps(steps, has_test_failures=True)
        assert result == ["Install"]
