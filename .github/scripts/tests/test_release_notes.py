"""Tests for release_notes.py — generates the docs Release Notes page from GitHub Releases."""

import json
import os
import subprocess
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import release_notes as rn


def _release(tag, body="", draft=False, prerelease=False, published_at="2026-01-02T03:04:05Z"):
    return {
        "tag_name": tag,
        "body": body,
        "draft": draft,
        "prerelease": prerelease,
        "published_at": published_at,
        "created_at": published_at,
        "html_url": f"https://github.com/man-group/ArcticDB/releases/tag/{tag}",
    }


def test_only_stable_published_releases_newest_first():
    releases = [
        _release("v6.9.0"),
        _release("v6.10.0"),
        _release("v6.10.1rc0"),
        _release("v6.10.1", prerelease=True),
        _release("v6.11.0", draft=True),
        _release("v4.2.0-docs"),
        _release("v5.0.0+man1"),
        _release("v1.1.0"),
    ]
    assert [r["tag_name"] for r in rn.stable_releases(releases)] == ["v6.10.0", "v6.9.0", "v1.1.0"]


def test_fetch_flattens_paginated_pages():
    pages = [[_release("v2.0.0")], [_release("v1.0.0")]]
    with patch.object(rn, "run_gh", return_value=json.dumps(pages)) as gh:
        releases = rn.fetch_releases()
    assert [r["tag_name"] for r in releases] == ["v2.0.0", "v1.0.0"]
    assert gh.call_args.args[:3] == ("api", "--paginate", "--slurp")


def test_footer_and_full_changelog_are_stripped():
    body = (
        "## Fixes\r\n\r\n- A fix\r\n\r\n"
        "**Full Changelog**: https://github.com/man-group/ArcticDB/compare/v1.0.0...v1.0.1\r\n"
        "---\r\n> The wheels are on [Pypi](https://pypi.org/project/arcticdb/). Below are for debugging:\r\n"
        "[wheel.whl](https://example.com)\r\n"
    )
    cleaned = rn.clean_body(body)
    assert "\r" not in cleaned
    assert "wheels" not in cleaned
    assert "Full Changelog" not in cleaned
    assert "example.com" not in cleaned
    assert cleaned.endswith("- A fix")


def test_headings_are_demoted_below_the_sidebar_depth():
    cleaned = rn.clean_body("# Top\n\n## Section\n\n### Sub\n\n#### Deeper")
    assert cleaned.split("\n\n") == ["#### Top", "#### Section", "##### Sub", "##### Deeper"]


def test_hashes_in_code_fences_are_not_headings():
    cleaned = rn.clean_body("```python\n# a comment\nlib.write('x', df)\n```")
    assert "# a comment" in cleaned
    assert "#### a comment" not in cleaned


def test_setext_heading_is_demoted():
    assert rn.clean_body("Intro\n\nFixes\n---\n- a fix") == "Intro\n\n#### Fixes\n\n- a fix"


def test_list_after_paragraph_gets_blank_line():
    assert rn.clean_body("Caveats:\n* one\n* two") == "Caveats:\n\n* one\n* two"


def test_two_space_nested_list_is_reindented():
    assert rn.clean_body("- outer\n  - inner") == "- outer\n    - inner"


def test_single_newlines_become_hard_breaks():
    assert rn.clean_body("line one\nline two") == "line one  \nline two"


def test_details_blocks_render_markdown():
    assert '<details markdown="1">' in rn.clean_body("<details>\n<summary>More</summary>\n\n* a\n</details>")


def test_stray_less_than_is_escaped_but_html_is_kept():
    cleaned = rn.clean_body("Not readable by <v6.7.0 releases. See <code>x</code><br>")
    assert "&lt;v6.7.0" in cleaned
    assert "<code>x</code><br>" in cleaned


def test_issue_refs_and_mentions_are_linked_outside_code():
    cleaned = rn.clean_body("Fix (#3345) by @alice, mail bob@example.com, `#12 @x` and https://github.com/o/r/pull/9#1")
    assert "[#3345](https://github.com/man-group/ArcticDB/issues/3345)" in cleaned
    assert "[@alice](https://github.com/alice)" in cleaned
    assert "bob@example.com" in cleaned and "[@example" not in cleaned
    assert "`#12 @x`" in cleaned
    assert "https://github.com/o/r/pull/9#1" in cleaned


def test_render_page_has_one_heading_per_release_with_stable_anchor():
    page = rn.render_page([_release("v6.26.0", "## Fixes\n- a fix"), _release("v6.25.0", "")])
    assert page.startswith("# Release Notes\n")
    assert "## v6.26.0 { #v6-26-0 }" in page
    assert (
        "*Released 2026-01-02* · [GitHub release](https://github.com/man-group/ArcticDB/releases/tag/v6.26.0)" in page
    )
    assert "#### Fixes" in page
    assert "## v6.25.0 { #v6-25-0 }\n\n*Released 2026-01-02*" in page
    assert "*No release notes.*" in page


def test_main_writes_page(tmp_path):
    out = tmp_path / "release_notes.md"
    with patch.object(rn, "fetch_releases", return_value=[_release("v1.0.0", "Hello")]):
        rn.main(["--out", str(out)])
    assert "## v1.0.0 { #v1-0-0 }" in out.read_text()


def test_fetch_retries_transient_failures():
    error = subprocess.CalledProcessError(1, ["gh"])
    with patch.object(rn, "run_gh", side_effect=[error, error, "[[]]"]) as gh, patch.object(rn.time, "sleep"):
        assert rn.fetch_releases() == []
    assert gh.call_count == 3


def test_fetch_gives_up_after_retries():
    error = subprocess.CalledProcessError(1, ["gh"])
    with patch.object(rn, "run_gh", side_effect=error), patch.object(rn.time, "sleep"):
        with pytest.raises(subprocess.CalledProcessError):
            rn.fetch_releases()


@pytest.mark.parametrize("error", [subprocess.CalledProcessError(1, ["gh"]), FileNotFoundError("gh")])
def test_main_writes_placeholder_outside_ci_when_gh_fails(tmp_path, monkeypatch, error):
    monkeypatch.delenv("CI", raising=False)
    out = tmp_path / "release_notes.md"
    with patch.object(rn, "fetch_releases", side_effect=error):
        rn.main(["--out", str(out)])
    assert out.read_text().startswith("# Release Notes")
    assert "generated when the docs are published" in out.read_text()


def test_main_fails_in_ci_when_gh_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("CI", "true")
    with patch.object(rn, "fetch_releases", side_effect=subprocess.CalledProcessError(1, ["gh"])):
        with pytest.raises(subprocess.CalledProcessError):
            rn.main(["--out", str(tmp_path / "release_notes.md")])


def test_issue_ref_at_line_start_is_not_a_heading():
    assert rn.clean_body("#3085 Changes NaT handling") == (
        "[#3085](https://github.com/man-group/ArcticDB/issues/3085) Changes NaT handling"
    )


def test_fence_after_paragraph_gets_blank_line():
    assert rn.clean_body("Apply this:\n```python\nx = 1\n```") == "Apply this:\n\n```python\nx = 1\n```"


def test_fence_only_closes_on_matching_fence():
    cleaned = rn.clean_body("```\n~~~\n# inside\n```\n# outside")
    assert "# inside" in cleaned and "#### inside" not in cleaned
    assert cleaned.endswith("#### outside")
    cleaned = rn.clean_body("````\n```\n# inside\n```\n````\n# outside")
    assert "#### inside" not in cleaned and cleaned.endswith("#### outside")


def test_inline_triple_backticks_do_not_open_a_fence():
    assert rn.clean_body("```df.head()``` shows rows\n\n## Next").endswith("#### Next")


def test_deeply_nested_two_space_lists_are_reindented():
    assert rn.clean_body("- a\n  - b\n    - c\n      - d") == "- a\n    - b\n        - c\n            - d"


def test_four_space_nesting_is_kept():
    assert rn.clean_body("- a\n    - b\n        - c") == "- a\n    - b\n        - c"


def test_indented_top_level_list_moves_to_column_zero():
    assert rn.clean_body("Intro\n\n  - a\n  - b") == "Intro\n\n- a\n- b"


def test_tab_nested_list_is_reindented():
    assert rn.clean_body("- a\n\t- b") == "- a\n    - b"


def test_autolinks_comments_and_block_html_are_not_escaped():
    cleaned = rn.clean_body("See <https://pypi.org/project/arcticdb/> <!-- hidden --> <div>x</div><hr>")
    assert "<https://pypi.org/project/arcticdb/>" in cleaned
    assert "<!--" not in cleaned and "hidden" not in cleaned
    assert "<div>x</div><hr>" in cleaned


def test_indented_code_is_left_alone():
    assert rn.clean_body("Example:\n\n    if x < y: #123") == "Example:\n\n    if x < y: #123"


def test_refs_are_not_linked_inside_links_html_or_double_backtick_code():
    body = '[fix #123 by @bob](https://x.com) <img alt="@bob #12"> `` `x` #123 `` uses @staticmethod'
    assert rn.clean_body(body) == body


def test_new_contributor_mentions_are_linked():
    assert rn.clean_body("* @alice made their first contribution") == (
        "* [@alice](https://github.com/alice) made their first contribution"
    )


def test_details_with_attributes_render_markdown():
    assert '<details open markdown="1">' in rn.clean_body("<details open>\n\n* a\n</details>")


def test_old_debugging_footer_is_stripped():
    assert rn.clean_body("- A fix\r\nBelow are artifacts for debugging:\r\n") == "- A fix"


def test_fence_after_list_item_ends_the_list():
    assert rn.clean_body("- item\n```\ncode\n```") == "- item\n\n```\ncode\n```"


def test_paragraph_after_fence_gets_blank_line():
    assert rn.clean_body("```\ncode\n```\nOutput:") == "```\ncode\n```\n\nOutput:"


def test_fence_nested_in_list_item_gets_blank_line():
    assert rn.clean_body("- install with:\n    ```bash\n    pip install arcticdb\n    ```") == (
        "- install with:\n\n    ```bash\n    pip install arcticdb\n    ```"
    )


@pytest.mark.parametrize("before", ["- One fix", "> Requires numpy 2", "</details>"])
def test_rule_after_list_quote_or_html_stays_a_rule(before):
    assert rn.clean_body(f"{before}\n---\nThanks") == f"{before}\n\n---\n\nThanks"


def test_indented_paragraph_in_list_item_is_reindented():
    assert rn.clean_body("- item\n\n  second para\n- next") == "- item\n\n    second para\n\n- next"


def test_indented_fence_in_list_item_is_reindented_with_its_body():
    assert rn.clean_body("- item\n  ```py\n  code\n  ```\n- next") == (
        "- item\n\n    ```py\n    code\n    ```\n\n- next"
    )


def test_lazy_continuation_keeps_list_nesting_and_breaks_line():
    assert rn.clean_body("- item\ncontinued\n  - sub") == "- item  \ncontinued\n    - sub"


@pytest.mark.parametrize("output", ["", "not json"])
def test_unparseable_gh_output_is_retried(output):
    with patch.object(rn, "run_gh", side_effect=[output, "[[]]"]) as gh, patch.object(rn.time, "sleep"):
        assert rn.fetch_releases() == []
    assert gh.call_count == 2


def test_main_writes_placeholder_when_gh_output_is_unparseable(tmp_path, monkeypatch):
    monkeypatch.delenv("CI", raising=False)
    out = tmp_path / "release_notes.md"
    with patch.object(rn, "run_gh", return_value=""), patch.object(rn.time, "sleep"):
        rn.main(["--out", str(out)])
    assert "generated when the docs are published" in out.read_text()


def test_leading_indented_code_keeps_its_indentation():
    assert rn.clean_body("    code\n\ntext") == "    code\n\ntext"
