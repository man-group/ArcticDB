"""Tests for send_slack_notification.py — focuses on payload construction."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from send_slack_notification import build_payload


class TestBuildPayload:
    BASE_ARGS = dict(
        workflow_name="Build and Test",
        conclusion="failure",
        branch="master",
        run_url="https://github.com/owner/repo/actions/runs/123",
        repo="owner/repo",
        repo_url="https://github.com/owner/repo",
        summary=None,
        tracker_result=None,
    )

    def test_basic_failure_payload(self):
        payload = build_payload(**self.BASE_ARGS)
        blocks = payload["blocks"]

        assert len(blocks) == 2  # header + context, no summary
        header_text = blocks[0]["text"]["text"]
        assert ":fire:" in header_text
        assert "Build and Test" in header_text
        assert "failure" in header_text
        assert "`master`" in header_text

    def test_timed_out_uses_hourglass_icon(self):
        args = {**self.BASE_ARGS, "conclusion": "timed_out"}
        payload = build_payload(**args)
        header = payload["blocks"][0]["text"]["text"]
        assert ":hourglass:" in header
        assert ":fire:" not in header

    def test_with_summary(self):
        args = {
            **self.BASE_ARGS,
            "summary": ":rotating_light: *New* — `Test.Foo` (<url|issue>)",
        }
        payload = build_payload(**args)
        blocks = payload["blocks"]

        assert len(blocks) == 3  # header + summary + context
        summary_block = blocks[1]
        assert summary_block["type"] == "section"
        assert "Test.Foo" in summary_block["text"]["text"]

    def test_tracker_failure_shows_warning(self):
        args = {
            **self.BASE_ARGS,
            "summary": "this should be ignored",
            "tracker_result": "failure",
        }
        payload = build_payload(**args)
        blocks = payload["blocks"]

        assert len(blocks) == 3
        warning_block = blocks[1]
        assert ":warning:" in warning_block["text"]["text"]
        assert "Issue tracking failed" in warning_block["text"]["text"]
        # Summary is NOT shown when tracker failed
        assert "this should be ignored" not in warning_block["text"]["text"]

    def test_context_block_has_links(self):
        payload = build_payload(**self.BASE_ARGS)
        context = payload["blocks"][-1]
        assert context["type"] == "context"
        text = context["elements"][0]["text"]
        assert "owner/repo" in text
        assert "View Failure" in text

    def test_no_summary_no_tracker_failure(self):
        """When there's no summary and no tracker failure, only 2 blocks."""
        payload = build_payload(**self.BASE_ARGS)
        assert len(payload["blocks"]) == 2

    def test_special_characters_in_workflow_name(self):
        """Ensure special characters don't break the payload structure."""
        args = {**self.BASE_ARGS, "workflow_name": 'Build "with" quotes & <angles>'}
        payload = build_payload(**args)
        header = payload["blocks"][0]["text"]["text"]
        assert 'Build "with" quotes & <angles>' in header
