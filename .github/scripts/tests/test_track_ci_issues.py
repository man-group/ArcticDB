"""Tests for track_ci_issues.py — focuses on issue body generation and grouping logic."""
import os
import sys
import tempfile
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from track_ci_issues import IssueTracker, SummaryEntry, GROUPING_THRESHOLD, main, read_lines


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
# main() early exit when no failure_kind.txt
# ---------------------------------------------------------------------------
class TestMainNoFailedJobs:
    def test_exits_early_when_no_failure_kind(self, tmp_path):
        """When parse_ci_failures found no failed jobs, failure_kind.txt
        is absent and main() should exit without creating any issues."""
        output_file = tmp_path / "slack_summary.txt"
        with patch("sys.argv", [
            "track_ci_issues.py",
            "--input-dir", str(tmp_path),
            "--run-id", "123",
            "--run-url", "https://example.com",
            "--repo", "owner/repo",
            "--commit-sha", "abc123",
            "--conclusion", "failure",
            "--output-file", str(output_file),
        ]):
            main()
        assert output_file.read_text() == ""

    def test_exits_early_creates_github_output_file(self, tmp_path):
        """When --github-output-file is provided, it should also be created on early exit."""
        output_file = tmp_path / "slack_summary.txt"
        github_file = tmp_path / "github_summary.txt"
        with patch("sys.argv", [
            "track_ci_issues.py",
            "--input-dir", str(tmp_path),
            "--run-id", "123",
            "--run-url", "https://example.com",
            "--repo", "owner/repo",
            "--commit-sha", "abc123",
            "--conclusion", "failure",
            "--output-file", str(output_file),
            "--github-output-file", str(github_file),
        ]):
            main()
        assert output_file.read_text() == ""
        assert github_file.read_text() == ""


# ---------------------------------------------------------------------------
# SummaryEntry — dual-format output
# ---------------------------------------------------------------------------
class TestSummaryEntry:
    def test_new_issue_slack(self):
        e = SummaryEntry("alert", "New", "— `Test.Foo`",
                         "https://github.com/o/r/issues/1", "issue")
        assert e.to_slack() == ":rotating_light: *New* — `Test.Foo` (<https://github.com/o/r/issues/1|issue>)"

    def test_new_issue_github(self):
        e = SummaryEntry("alert", "New", "— `Test.Foo`",
                         "https://github.com/o/r/issues/1", "issue")
        assert e.to_github() == "🚨 **New** — `Test.Foo` ([issue](https://github.com/o/r/issues/1))"

    def test_known_issue_slack(self):
        e = SummaryEntry("warning", "Known", "— `Test.Bar`",
                         "https://github.com/o/r/issues/5", "#5")
        assert e.to_slack() == ":warning: *Known* — `Test.Bar` (<https://github.com/o/r/issues/5|#5>)"

    def test_known_issue_github(self):
        e = SummaryEntry("warning", "Known", "— `Test.Bar`",
                         "https://github.com/o/r/issues/5", "#5")
        assert e.to_github() == "⚠️ **Known** — `Test.Bar` ([#5](https://github.com/o/r/issues/5))"

    def test_timeout_slack(self):
        e = SummaryEntry("timeout", "Timeout", "",
                         "https://github.com/o/r/issues/20", "issue")
        assert e.to_slack() == ":hourglass: *Timeout* (<https://github.com/o/r/issues/20|issue>)"

    def test_timeout_github(self):
        e = SummaryEntry("timeout", "Timeout", "",
                         "https://github.com/o/r/issues/20", "issue")
        assert e.to_github() == "⏳ **Timeout** ([issue](https://github.com/o/r/issues/20))"

    def test_empty_label(self):
        """Unparseable failures have no label — should not produce empty bold markers."""
        e = SummaryEntry("question", "", "Could not identify specific failures",
                         "https://url", "#10")
        assert e.to_slack() == ":question: Could not identify specific failures (<https://url|#10>)"
        assert e.to_github() == "❓ Could not identify specific failures ([#10](https://url))"

    def test_grouped_slack(self):
        e = SummaryEntry("alert", "15 tests failed",
                         "— likely correlated", "https://url", "issue")
        assert "*15 tests failed*" in e.to_slack()
        assert ":rotating_light:" in e.to_slack()

    def test_grouped_github(self):
        e = SummaryEntry("alert", "15 tests failed",
                         "— likely correlated", "https://url", "issue")
        assert "**15 tests failed**" in e.to_github()
        assert "🚨" in e.to_github()
        assert "[issue](https://url)" in e.to_github()


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
            run_date="2025-03-15T10:30:00Z",
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

    def test_run_date_used_in_body(self, tracker):
        body = tracker.test_issue_body("Test.Name")
        assert "2025-03-15T10:30:00Z" in body

    def test_run_date_defaults_to_unknown(self):
        tracker = IssueTracker("owner/repo", "1", "https://url", "abc123")
        assert tracker.run_date == "unknown"


# ---------------------------------------------------------------------------
# Grouping threshold
# ---------------------------------------------------------------------------
class TestGroupingThreshold:
    def test_threshold_value(self):
        assert GROUPING_THRESHOLD == 10

    @patch("track_ci_issues.create_issue", return_value="https://github.com/owner/repo/issues/1")
    @patch("track_ci_issues.find_existing_issue", return_value=None)
    def test_creates_grouped_issue_for_tests(self, mock_find, mock_create):
        tracker = IssueTracker(
            repo="owner/repo",
            run_id="123",
            run_url="https://github.com/owner/repo/actions/runs/123",
            commit_sha="abc123",
        )
        items = [f"Test.Name{i}" for i in range(15)]
        tracker.create_grouped_issue("test", items)

        mock_find.assert_called_once_with("owner/repo", "flaky-test", "Grouped CI test failures")
        mock_create.assert_called_once()
        title = mock_create.call_args[0][1]
        assert title == "Grouped CI test failures"
        assert len(tracker.entries) == 1
        assert "15 tests failed" in tracker.format_summary("slack")

    @patch("track_ci_issues.create_issue", return_value="https://github.com/owner/repo/issues/2")
    @patch("track_ci_issues.find_existing_issue", return_value=None)
    def test_creates_grouped_issue_for_steps(self, mock_find, mock_create):
        tracker = IssueTracker(
            repo="owner/repo",
            run_id="456",
            run_url="https://github.com/owner/repo/actions/runs/456",
            commit_sha="def456",
        )
        items = [f"Step{i}" for i in range(20)]
        tracker.create_grouped_issue("step", items)

        title = mock_create.call_args[0][1]
        assert title == "Grouped CI step failures"
        assert "20 steps failed" in tracker.format_summary("slack")

    @patch("track_ci_issues.get_issue_url", return_value="https://github.com/owner/repo/issues/7")
    @patch("track_ci_issues.comment_on_issue")
    @patch("track_ci_issues.find_existing_issue", return_value=7)
    def test_comments_on_existing_grouped_issue(self, mock_find, mock_comment, mock_url):
        tracker = IssueTracker(
            repo="owner/repo",
            run_id="789",
            run_url="https://github.com/owner/repo/actions/runs/789",
            commit_sha="def789",
        )
        items = [f"Test.Name{i}" for i in range(15)]
        tracker.create_grouped_issue("test", items)

        mock_comment.assert_called_once()
        assert "#7" in tracker.format_summary("slack")
        assert "15 tests failed" in tracker.format_summary("slack")


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
        assert ":rotating_light:" in tracker.format_summary("slack")
        assert "Test.Foo" in tracker.format_summary("slack")
        # GitHub format should use unicode emoji and markdown links
        assert "🚨" in tracker.format_summary("github")
        assert "[issue](" in tracker.format_summary("github")

    @patch("track_ci_issues.get_issue_url", return_value="https://github.com/owner/repo/issues/5")
    @patch("track_ci_issues.comment_on_issue")
    @patch("track_ci_issues.find_existing_issue", return_value=5)
    def test_comments_on_existing_issue(self, mock_find, mock_comment, mock_url):
        tracker = IssueTracker("owner/repo", "1", "https://url", "abc123")
        tracker.track_item("Test.Foo", "Flaky test", "flaky-test", "body text")

        mock_comment.assert_called_once()
        assert ":warning:" in tracker.format_summary("slack")
        assert "#5" in tracker.format_summary("slack")


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
        assert ":hourglass:" in tracker.format_summary("slack")
        assert "Timeout" in tracker.format_summary("slack")

    @patch("track_ci_issues.get_issue_url", return_value="https://github.com/owner/repo/issues/20")
    @patch("track_ci_issues.comment_on_issue")
    @patch("track_ci_issues.find_existing_issue", return_value=20)
    def test_comments_on_existing_timeout_issue(self, mock_find, mock_comment, mock_url):
        tracker = IssueTracker("owner/repo", "1", "https://url", "abc123")
        tracker.handle_timeout()

        mock_comment.assert_called_once()
        comment_body = mock_comment.call_args[0][2]
        assert "Another timeout" in comment_body
        assert ":hourglass:" in tracker.format_summary("slack")
        assert "#20" in tracker.format_summary("slack")
