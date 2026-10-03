#!/usr/bin/env python3
"""Generate the docs "Release Notes" page from the published GitHub Releases.

Run before building the docs; the output is git-ignored. Needs an authenticated `gh` (GH_TOKEN in CI).

    python .github/scripts/release_notes.py [--out docs/mkdocs/docs/release_notes.md]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time

from utils import run_gh

REPO = "man-group/ArcticDB"
DEFAULT_OUT = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "mkdocs", "docs", "release_notes.md")
FETCH_ATTEMPTS = 3
PLACEHOLDER = (
    "# Release Notes\n\n"
    "This page is generated when the docs are published. To build it locally, run "
    "`python .github/scripts/release_notes.py` with an authenticated `gh`.\n"
)

STABLE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})(.*)$")
LIST_ITEM = re.compile(r"^(\s*)([-*+]|\d+\.)\s")
HEADING = re.compile(r"^(#{1,6})(?:[ \t]+(.*?))?[ \t]*$")
SETEXT_UNDERLINE = re.compile(r"^\s*(-{3,}|={3,})\s*$")
# Everything from the release template's debugging note onwards is CI output
FOOTER = re.compile(
    r"(^-{3,}\s*\n)?^(>\s*The wheels are on \[py ?pi\]|Below are (artifacts )?for debugging:).*", re.I | re.M | re.S
)
FULL_CHANGELOG = re.compile(r"^\*\*Full Changelog\*\*:.*$", re.M)
HTML_COMMENT = re.compile(r"<!--.*?-->", re.S)
HTML_TAGS = (
    "details|summary|table|thead|tbody|tr|th|td|br|hr|p|div|span|code|pre|img|picture|source|video|"
    "b|i|em|strong|s|del|ins|u|a|sub|sup|small|ul|ol|li|dl|dt|dd|kbd|blockquote|h[1-6]"
)
# Spans whose contents must not be rewritten: code spans, markdown links, HTML tags, autolinks
PROTECTED = re.compile(
    rf"(`+).+?(?<!`)\1(?!`)|!?\[[^\]]*\]\([^)]*\)|</?(?:{HTML_TAGS})\b[^>]*>|<(?:https?|mailto):[^>\s]*>", re.I
)
MENTION = r"@([A-Za-z0-9-]{2,39})\b"


def fetch_releases():
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            pages = json.loads(run_gh("api", "--paginate", "--slurp", f"repos/{REPO}/releases?per_page=100"))
            return [release for page in pages for release in page]
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            if attempt == FETCH_ATTEMPTS:
                raise
            time.sleep(2**attempt)


def _version(release):
    return tuple(int(x) for x in STABLE_TAG.match(release["tag_name"]).groups())


def stable_releases(releases):
    stable = [r for r in releases if STABLE_TAG.match(r["tag_name"]) and not r["draft"] and not r["prerelease"]]
    return sorted(stable, key=_version, reverse=True)


def _link_refs(text):
    # Any < left outside PROTECTED is literal text
    text = text.replace("<", "&lt;")
    # GitHub redirects /issues/N to the PR when N is a PR
    text = re.sub(r"(?<![\w/&\[])#(\d{2,5})\b", rf"[#\1](https://github.com/{REPO}/issues/\1)", text)
    return re.sub(rf"\bby {MENTION}", r"by [@\1](https://github.com/\1)", text)


def _protected(span):
    if span.lower().startswith("<details") and "markdown=" not in span:
        return span[:8] + span[8:-1].rstrip() + ' markdown="1">'
    return span


def _inline(line):
    line = re.sub(rf"^(\s*[-*+] ){MENTION}", r"\1[@\2](https://github.com/\2)", line)
    out, pos = [], 0
    for m in PROTECTED.finditer(line):
        out += [_link_refs(line[pos : m.start()]), _protected(m.group(0))]
        pos = m.end()
    out.append(_link_refs(line[pos:]))
    return "".join(out)


def _is_paragraph_line(line):
    return bool(line.strip()) and not (
        LIST_ITEM.match(line) or line.startswith((" ", "#")) or line.lstrip().startswith("|")
    )


def _needs_blank_before_block(prev):
    return _is_paragraph_line(prev) or prev.startswith("#")


def _list_content_indent(indent, list_indents):
    # Python-Markdown expects the content of a level-k list item at 4*k spaces
    level = sum(1 for list_indent in list_indents if list_indent < indent)
    return max(indent, 4 * level)


def clean_body(body):
    """Make a GitHub-flavoured release body render the same way under Python-Markdown."""
    body = (body or "").replace("\r\n", "\n")
    body = FOOTER.sub("", body)
    body = FULL_CHANGELOG.sub("", body)
    body = HTML_COMMENT.sub("", body)
    out = []
    fence = None
    fence_shift = 0
    blank_before_next = False
    in_indented_code = False
    list_indents = []
    for line in body.split("\n"):
        line = line.expandtabs(4)
        prev = out[-1] if out else ""
        fence_match = FENCE.match(line)
        if fence:
            out.append(" " * fence_shift + line if line.strip() else line)
            if fence_match and fence_match.group(1).startswith(fence) and not fence_match.group(2).strip():
                fence = None
                blank_before_next = True
            continue
        if blank_before_next and line.strip():
            out.append("")
            prev = ""
        blank_before_next = False
        indent = len(line) - len(line.lstrip())
        if fence_match and not (fence_match.group(1)[0] == "`" and "`" in fence_match.group(2)):
            fence = fence_match.group(1)
            fence_shift = 0
            if not indent:
                # An unindented fence ends any open list on GitHub
                list_indents = []
            elif list_indents:
                fence_shift = _list_content_indent(indent, list_indents) - indent
            if prev.strip():
                out.append("")
            out.append(" " * fence_shift + line)
            continue
        if line.strip() and not line.startswith("    "):
            in_indented_code = False
        if not list_indents and line.startswith("    ") and (in_indented_code or not prev.strip()):
            in_indented_code = True
        if in_indented_code:
            out.append(line)
            continue
        if SETEXT_UNDERLINE.match(line):
            if _is_paragraph_line(prev) and not prev.startswith((">", "<")):
                out[-1] = "#### " + prev.rstrip()
                if len(out) > 1 and out[-2].strip():
                    out.insert(-1, "")
                continue
            # A horizontal rule; without blank lines Python-Markdown would read it as a setext underline
            if prev.strip():
                out.append("")
            out.append(line.strip())
            blank_before_next = True
            list_indents = []
            continue
        heading = HEADING.match(line)
        list_item = LIST_ITEM.match(line)
        if heading:
            # Releases are h2; body headings start at h4 so they stay out of the sidebar (toc_depth 1-3)
            line = "#" * (4 if len(heading.group(1)) <= 2 else 5) + " " + (heading.group(2) or "")
            list_indents = []
            if prev.strip():
                out.append("")
        elif list_item:
            continues_list = bool(list_indents) and bool(prev.strip())
            # GitHub nests lists at 2+ spaces, Python-Markdown needs 4 per level
            while list_indents and indent < list_indents[-1]:
                list_indents.pop()
            if not list_indents or indent > list_indents[-1]:
                list_indents.append(indent)
            line = " " * (4 * (len(list_indents) - 1)) + line.lstrip()
            after_item_paragraph = prev.startswith(" ") and not LIST_ITEM.match(prev)
            if after_item_paragraph or (not continues_list and _needs_blank_before_block(prev)):
                out.append("")
        elif line.lstrip().startswith("|"):
            if _needs_blank_before_block(prev):
                out.append("")
        elif line.strip():
            # HTML blocks interrupt a list item on GitHub; other unindented text continues it lazily
            in_list_item = bool(list_indents) and bool(prev.strip()) and not line.startswith("<")
            if indent and list_indents:
                line = " " * _list_content_indent(indent, list_indents) + line.lstrip()
            elif not indent and not in_list_item:
                list_indents = []
            # GitHub renders single newlines in release bodies as line breaks
            if (in_list_item and not prev.startswith("#")) or _is_paragraph_line(prev):
                out[-1] = prev + "  "
        out.append(_inline(line))
    text = "\n".join(out).strip("\n").rstrip()
    text = re.sub(r"(\n\s*-{3,}\s*)+$", "", text)
    return re.sub(r"\n{3,}", "\n\n", text)


def render_page(releases):
    lines = [
        "# Release Notes",
        "",
        f"Release notes for every stable ArcticDB release, from [GitHub Releases](https://github.com/{REPO}/releases).",
        "",
    ]
    for release in releases:
        tag = release["tag_name"]
        date = (release["published_at"] or release["created_at"])[:10]
        lines += [
            f"## {tag} {{ #{tag.replace('.', '-')} }}",
            "",
            f"*Released {date}* · [GitHub release]({release['html_url']})",
            "",
            clean_body(release["body"]) or "*No release notes.*",
            "",
        ]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args(argv)
    try:
        releases = stable_releases(fetch_releases())
        page = render_page(releases)
        print(f"Wrote {len(releases)} releases to {args.out}", file=sys.stderr)
    except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError):
        if os.environ.get("CI"):
            raise
        page = PLACEHOLDER
        print(f"Could not fetch releases with gh; wrote a placeholder to {args.out}", file=sys.stderr)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(page)


if __name__ == "__main__":
    main()
