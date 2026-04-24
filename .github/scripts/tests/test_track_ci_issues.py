"""Tests for track_ci_issues.py — focuses on issue body generation and grouping logic."""
import os
import sys
import tempfile
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from track_ci_issues import IssueTracker, GROUPING_THRESHOLD, read_lines


# ---------------------------------------------------------------------------
# read_lines
# ---------------------------------------------------------------------------
class TestReadLines:
    def test_reads_non_empty_lines(self, tmp_path):
        f = tmp_path / "tests.txt"
        f.write_text("test_a\n\ntest_b\n  \ntest_c\n")
        assert read_lines(str(f)) == ["test_a", "test_b", "test_c"]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        assert read_lines(str(f)) == []

    def test_missing_file(self):
        assert read_lines("/nonexistent/path.txt") == []


# ---------------------------------------------------------------------------
# IssueTracker body generation
# ---------------------------------------------------------------------------
class TestIssueTrackerBodies:
    @pytest.fixture
    def tracker(self):
        return IssueTracker(
            repo="owner/repo",
            run_id="99999",
            run_url="https://github.com/owner/repo/actions/runs/99999",
            commit_sha="abcdef1234567890abcdef1234567890abcdef12",
        )

    def test_test_issue_body_contains_test_name(self, tracker):
        body = tracker.test_issue_body("SuiteA.TestFoo")
        assert "`SuiteA.TestFoo`" in body
        assert "Flaky test failure" in body
        assert "Run #99999" in body
        assert "`abcdef1234`" in body

    def test_step_issue_body_contains_step_name(self, tracker):
        body = tracker.step_issue_body("Build (Linux) / Install deps")
        assert "`Build (Linux) / Install deps`" in body
        assert "Intermittent CI step failure" in body
        assert "transient infrastructure" in body

    def test_issue_body_has_failure_table(self, tracker):
        body = tracker.test_issue_body("Test.Name")
        assert "| Date | Run | Commit |" in body
        assert "Run #99999" in body

    def test_short_sha_is_10_chars(self, tracker):
        assert tracker.short_sha == "abcdef1234"


# ---------------------------------------------------------------------------
# Grouping threshold
# ---------------------------------------------------------------------------
class TestGroupingThreshold:
    def test_threshold_value(self):
        assert GROUPING_THRESHOLD == 10

    @patch("track_ci_issues.create_issue", return_value="https://github.com/owner/repo/issues/1")
    def test_creates_grouped_issue_for_tests(self, mock_create):
        tracker = IssueTracker(
            repo="owner/repo",
            run_id="123",
            run_url="https://github.com/owner/repo/actions/runs/123",
            commit_sha="abc123",
        )
        items = [f"Test.Name{i}" for i in range(15)]
        tracker.create_grouped_issue("test", items)

        mock_create.assert_called_once()
        call_args = mock_create.call_args
        title = call_args[1]["title"] if "title" in call_args[1] else call_args[0][1]
        assert "Grouped CI test failures (15 tests)" in title
        assert len(tracker.slack_lines) == 1
        assert "15 tests failed" in tracker.slack_lines[0]

    @patch("track_ci_issues.create_issue", return_value="https://github.com/owner/repo/issues/2")
    def test_creates_grouped_issue_for_steps(self, mock_create):
        tracker = IssueTracker(
            repo="owner/repo",
            run_id="456",
            run_url="https://github.com/owner/repo/actions/runs/456",
            commit_sha="def456",
        )
        items = [f"Job / Step{i}" for i in range(20)]
        tracker.create_grouped_issue("step", items)

        call_args = mock_create.call_args
        title = call_args[1]["title"] if "title" in call_args[1] else call_args[0][1]
        assert "Grouped CI step failures (20 steps)" in title
        assert "20 steps failed" in tracker.slack_lines[0]


# ---------------------------------------------------------------------------
# track_item (with mocked gh calls)
# ---------------------------------------------------------------------------
class TestTrackItem:
    @patch("track_ci_issues.create_issue", return_value="https://github.com/owner/repo/issues/10")
    @patch("track_ci_issues.find_existing_issue", return_value=None)
    def test_creates_new_issue_when_none_exists(self, mock_find, mock_create):
        tracker = IssueTracker("owner/repo", "1", "https://url", "abc123")
        tracker.track_item("Test.Foo", "Flaky test", "flaky-test", "body text")

        mock_find.assert_called_once_with("owner/repo", "flaky-test", "Flaky test: Test.Foo")
        mock_create.assert_called_once()
        assert ":rotating_light:" in tracker.slack_lines[0]
        assert "Test.Foo" in tracker.slack_lines[0]

    @patch("track_ci_issues.get_issue_url", return_value="https://github.com/owner/repo/issues/5")
    @patch("track_ci_issues.comment_on_issue")
    @patch("track_ci_issues.find_existing_issue", return_value=5)
    def test_comments_on_existing_issue(self, mock_find, mock_comment, mock_url):
        tracker = IssueTracker("owner/repo", "1", "https://url", "abc123")
        tracker.track_item("Test.Foo", "Flaky test", "flaky-test", "body text")

        mock_comment.assert_called_once()
        assert ":warning:" in tracker.slack_lines[0]
        assert "#5" in tracker.slack_lines[0]


# ---------------------------------------------------------------------------
# handle_timeout
# ---------------------------------------------------------------------------
class TestHandleTimeout:
    @patch("track_ci_issues.create_issue", return_value="https://github.com/owner/repo/issues/20")
    @patch("track_ci_issues.find_existing_issue", return_value=None)
    def test_creates_timeout_issue(self, mock_find, mock_create):
        tracker = IssueTracker("owner/repo", "1", "https://url", "abc123")
        tracker.handle_timeout()

        mock_find.assert_called_once_with("owner/repo", "ci-failure", "CI timeout")
        mock_create.assert_called_once()
        title = mock_create.call_args[0][1]
        assert title == "CI timeout"
        body = mock_create.call_args[0][3]
        assert "exceeded its time limit" in body
        assert ":hourglass:" in tracker.slack_lines[0]
        assert "Timeout" in tracker.slack_lines[0]

    @patch("track_ci_issues.get_issue_url", return_value="https://github.com/owner/repo/issues/20")
    @patch("track_ci_issues.comment_on_issue")
    @patch("track_ci_issues.find_existing_issue", return_value=20)
    def test_comments_on_existing_timeout_issue(self, mock_find, mock_comment, mock_url):
        tracker = IssueTracker("owner/repo", "1", "https://url", "abc123")
        tracker.handle_timeout()

        mock_comment.assert_called_once()
        comment_body = mock_comment.call_args[0][2]
        assert "Another timeout" in comment_body
        assert ":hourglass:" in tracker.slack_lines[0]
        assert "#20" in tracker.slack_lines[0]
