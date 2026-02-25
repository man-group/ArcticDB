---
name: review-pr
description: Review a pull request for ArcticDB
---

# review-pr

Review the current PR. Use `gh pr diff` to get the diff, and `gh pr view` to get the title and description.

You are reviewing a pull request for ArcticDB.

Review the PR diff thoroughly. Provide two types of feedback:
1. **Inline comments** on specific lines where you find issues or concerns.
2. **A summary comment** with a checklist of overall quality gates.

You have access to the full repository checkout. Use it to look up relevant code,
understand existing patterns, and verify that changes are consistent with the codebase.
Do not assume file locations - search the repo to find the relevant implementations.

## Review Guidelines

Read and apply the guidelines in [`docs/claude/PR_REVIEW_GUIDELINES.md`](docs/claude/PR_REVIEW_GUIDELINES.md).
Apply sections based on which files are changed. Skip sections that are irrelevant to
the PR. Use the summary checklist at the end of that document for the summary comment.
