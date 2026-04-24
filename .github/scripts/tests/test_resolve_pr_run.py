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
        # All calls return the same data; the function picks the most recent
        mock_gh.return_value = json.dumps([
            {"id": 100, "created_at": "2025-01-01T00:00:00Z", "conclusion": "failure"},
            {"id": 200, "created_at": "2025-01-02T00:00:00Z", "conclusion": "failure"},
        ])
        result = find_latest_failed_run("owner/repo", "feature/foo")
        assert result == "200"

    @patch("resolve_pr_run.run_gh")
    def test_finds_timed_out_runs(self, mock_gh):
        mock_gh.return_value = json.dumps([
            {"id": 500, "created_at": "2025-01-05T00:00:00Z", "conclusion": "timed_out"},
        ])
        result = find_latest_failed_run("owner/repo", "feature/foo")
        assert result == "500"

    @patch("resolve_pr_run.run_gh")
    def test_queries_both_failure_and_timed_out(self, mock_gh):
        """Verify that both status=failure and status=timed_out are queried."""
        mock_gh.return_value = ""
        find_latest_failed_run("owner/repo", "feature/foo")
        # Each workflow generates 2 calls (failure + timed_out)
        statuses_queried = set()
        for call in mock_gh.call_args_list:
            url = call[0][2]  # Third positional arg is the API URL
            if "status=failure" in url:
                statuses_queried.add("failure")
            if "status=timed_out" in url:
                statuses_queried.add("timed_out")
        assert statuses_queried == {"failure", "timed_out"}

    @patch("resolve_pr_run.run_gh")
    def test_url_encodes_branch_name(self, mock_gh):
        mock_gh.return_value = ""
        find_latest_failed_run("owner/repo", "feat/foo&bar")
        # All API calls should have the branch URL-encoded
        for call in mock_gh.call_args_list:
            url = call[0][2]
            assert "feat%2Ffoo%26bar" in url
            assert "feat/foo&bar" not in url

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
