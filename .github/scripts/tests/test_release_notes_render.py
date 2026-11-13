"""Renders release_notes.py output with the docs' markdown extensions to catch structural breakage."""

import os
import re
import sys

import pytest

markdown = pytest.importorskip("markdown")
pytest.importorskip("pymdownx")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import release_notes as rn

# Mirrors markdown_extensions in docs/mkdocs/mkdocs.yml, minus the Material-only emoji index
EXTENSIONS = [
    "admonition",
    "pymdownx.details",
    "pymdownx.superfences",
    "attr_list",
    "md_in_html",
    "def_list",
    "pymdownx.magiclink",
    "toc",
]

AWKWARD_BODY = "\r\n".join(
    [
        "# Big heading",
        "#3085 Changes NaT handling",
        "Apply this:",
        "```python",
        "# a comment",
        "~~~",
        "```",
        "Fixes",
        "---",
        "- a",
        "  - b",
        "    - c",
        "- One fix",
        "---",
        "> Requires numpy 2",
        "---",
        "- item",
        "",
        "  second para",
        "- next",
        "<details open>",
        "<summary>More</summary>",
        "",
        "* hidden",
        "</details>",
        "Not readable by <v6.7.0 <!-- hidden-comment -->",
        "---",
        "> The wheels are on [PyPI](https://pypi.org/project/arcticdb/). Below are for debugging:",
    ]
)


def _render(body):
    release = {
        "tag_name": "v1.0.0",
        "body": body,
        "draft": False,
        "prerelease": False,
        "published_at": "2026-01-01T00:00:00Z",
        "created_at": "2026-01-01T00:00:00Z",
        "html_url": "https://github.com/man-group/ArcticDB/releases/tag/v1.0.0",
    }
    return markdown.markdown(rn.render_page([release]), extensions=EXTENSIONS)


def test_release_body_cannot_add_sidebar_headings():
    html = _render(AWKWARD_BODY)
    assert len(re.findall(r"<h1\b", html)) == 1
    assert re.findall(r"<h2 id=\"([^\"]+)\"", html) == ["v1-0-0"]
    assert not re.search(r"<h3\b", html)


def test_release_body_renders_valid_blocks():
    html = _render(AWKWARD_BODY)
    assert not re.search(r"<p>(?:(?!</p>).)*<(div|pre|ul|table)\b", html, re.S)
    assert "<li>c</li>" in html and html.index("<li>b") < html.index("<li>c</li>")
    assert "<li>hidden</li>" in html
    assert "<li>\n<p>next</p>\n</li>" in html or "<li>next</li>" in html
    assert "&lt;v6.7.0" in html and "hidden-comment" not in html and "wheels" not in html
