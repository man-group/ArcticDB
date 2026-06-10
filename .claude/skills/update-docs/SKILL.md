---
name: update-docs
description: Check and update documentation for ArcticDB code changes
---

Review the current branch or working-tree changes and ensure all documentation is up to date.

## Step 1 — Identify what changed

Determine the scope of changes (from the diff, staged files, or PR):

- Which Python public APIs were added or modified?
- Which areas of the C++ or Python codebase were touched?
- Were any new features, behaviours, or options introduced?

## Step 2 — User-facing documentation (`docs/mkdocs/docs/`)

ArcticDB user docs are built with MkDocs. The source tree:

| Directory | Content |
|-----------|---------|
| `docs/mkdocs/docs/tutorials/` | Step-by-step feature guides (e.g., `sql_queries.md`) |
| `docs/mkdocs/docs/api/` | API reference — auto-generated from docstrings via mkdocstrings |
| `docs/mkdocs/docs/technical/` | Architecture and implementation details |

### Docstrings (NumPy format)

When **authoring code**, add or update complete docstrings on any public method you change,
regardless of which file it's in.

When **reviewing** (PR review or `/review` skill), only **require** complete docstrings for
public methods in the primary API files:

- `python/arcticdb/version_store/library.py`
- `python/arcticdb/arctic.py`
- `python/arcticdb/version_store/_store.py`

A complete docstring has all applicable sections:

- `Parameters` — every parameter, with type and description
- `Returns` — return type and what it contains
- `Raises` — exceptions the caller should expect
- `Examples` — at least one runnable example

"Accurate" is not enough — missing sections are a gap.

### Tutorials

Features with multiple use cases or non-obvious behaviour need a tutorial in
`docs/mkdocs/docs/tutorials/`. A docstring alone is insufficient for complex features.

### mkdocs.yml nav

Any new documentation page must be added to the `nav` section of `docs/mkdocs/mkdocs.yml`.
Without a nav entry the page is unreachable from the site.

### Edge cases and disambiguation

- Behaviour that may surprise users (performance cliffs, unsupported dtypes, ordering
  guarantees) must be documented.
- When multiple similar features coexist (e.g., `update` vs `append`), explain
  "when to use A vs B".

### Checklist

- [ ] Public API has complete NumPy-format docstrings (Parameters, Returns, Raises, Examples)
- [ ] Complex or multi-use features have a tutorial in `docs/mkdocs/docs/tutorials/`
- [ ] `docs/mkdocs/mkdocs.yml` nav updated for any new pages
- [ ] Edge cases, limitations, and "when to use A vs B" guidance documented where applicable

## Step 3 — Claude-maintained technical docs (`docs/claude/`)

These docs are **owned and maintained by Claude**. The area→document mapping table is in
`CLAUDE.md` under "Claude-Maintained Technical Docs".

- **Read** the relevant doc when starting work in an area (e.g., read `CACHING.md` before
  modifying version map cache).
- **Update on change**: If the code change touches an area covered by a `docs/claude/`
  document, update that document to reflect the changes.
- **Scope**: Only update documents for areas actually changed. Do not touch unrelated docs.
- Keep docs **high-level and terse**: reference `file_path:ClassName:method_name` instead of
  copying code; use tables and bullet points over code blocks; avoid duplicating what's
  already in source code.

### Checklist

- [ ] `docs/claude/` technical doc updated for each area of code that was changed

## Step 4 — Build and verify (if user-facing docs changed)

```bash
cd docs/mkdocs && mkdocs serve
```

Check that new pages render correctly and links resolve.
