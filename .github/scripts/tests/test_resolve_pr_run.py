"""Tests for resolve_pr_run.py."""
import json
import os
import sys
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from resolve_pr_run import get_pr_head, find_latest_failed_run, TRACKED_WORKFLOWS


class TestGetPrHead:
    @patch("resolve_pr_run.run_gh")
    def test_returns_branch_and_sha(self, mock_gh):
        mock_gh.return_value = json.dumps({
            "branch": "feature/foo",
            "sha": "abc123def456",
        })
        branch, sha = get_pr_head("owner/repo", "42")
        mock_gh.assert_called_once_with(
            "api", "repos/owner/repo/pulls/42",
            "--jq", '{branch: .head.ref, sha: .head.sha}',
        )
        assert branch == "feature/foo"
        assert sha == "abc123def456"


class TestFindLatestFailedRun:
    @patch("resolve_pr_run.run_gh")
    def test_finds_most_recent_failure(self, mock_gh):
        mock_gh.return_value = json.dumps([
            {"id": 100, "created_at": "2025-01-01T00:00:00Z", "conclusion": "failure"},
            {"id": 200, "created_at": "2025-01-02T00:00:00Z", "conclusion": "failure"},
        ])
        result = find_latest_failed_run("owner/repo", "feature/foo")
        assert result == "200"

    @patch("resolve_pr_run.run_gh")
    def test_prefers_most_recent_across_workflows(self, mock_gh):
        # Each call to run_gh is for a different workflow
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return json.dumps([
                    {"id": 100, "created_at": "2025-01-01T00:00:00Z", "conclusion": "failure"},
                ])
            elif call_count == 2:
                return json.dumps([
                    {"id": 300, "created_at": "2025-01-03T00:00:00Z", "conclusion": "timed_out"},
                ])
            else:
                return ""

        mock_gh.side_effect = side_effect
        result = find_latest_failed_run("owner/repo", "feature/foo")
        assert result == "300"

    @patch("resolve_pr_run.run_gh")
    def test_no_failures_returns_none(self, mock_gh):
        mock_gh.return_value = ""
        result = find_latest_failed_run("owner/repo", "feature/foo")
        assert result is None

    @patch("resolve_pr_run.run_gh")
    def test_ignores_success_conclusion(self, mock_gh):
        mock_gh.return_value = json.dumps([
            {"id": 100, "created_at": "2025-01-01T00:00:00Z", "conclusion": "success"},
        ])
        result = find_latest_failed_run("owner/repo", "feature/foo")
        assert result is None

    def test_tracked_workflows_not_empty(self):
        assert len(TRACKED_WORKFLOWS) > 0
        assert "Build and Test" in TRACKED_WORKFLOWS
