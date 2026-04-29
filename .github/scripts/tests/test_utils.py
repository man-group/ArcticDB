"""Tests for utils.py — shared gh CLI wrapper."""
import os
import subprocess
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import run_gh


class TestRunGh:
    @patch("subprocess.run")
    def test_returns_stdout_stripped(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh", "issue", "list"],
            returncode=0,
            stdout="  some output\n",
            stderr="",
        )
        assert run_gh("issue", "list") == "some output"

    @patch("subprocess.run")
    def test_raises_on_failure_when_check_true(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh", "api", "repos/owner/repo"],
            returncode=1,
            stdout="",
            stderr="not found",
        )
        with pytest.raises(subprocess.CalledProcessError):
            run_gh("api", "repos/owner/repo")

    @patch("subprocess.run")
    def test_no_raise_when_check_false(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh", "run", "view"],
            returncode=1,
            stdout="partial",
            stderr="error msg",
        )
        result = run_gh("run", "view", check=False)
        assert result == "partial"

    @patch("subprocess.run")
    def test_error_message_includes_command(self, mock_run, capsys):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh", "api", "repos/owner/repo"],
            returncode=1,
            stdout="",
            stderr="Bad credentials",
        )
        with pytest.raises(subprocess.CalledProcessError):
            run_gh("api", "repos/owner/repo")
        captured = capsys.readouterr()
        assert "gh command failed" in captured.err
        assert "Bad credentials" in captured.err
