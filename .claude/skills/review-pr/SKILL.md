---
name: review-pr
description: Review a pull request for ArcticDB
---

Perform a thorough code review of an ArcticDB pull request.

## Context variables

When invoked from a GitHub Actions workflow the following variables are injected before
this prompt: `PR_NUMBER`, `REPO`, `EVENT_ACTION`, `BEFORE_SHA`, `AFTER_SHA`.
When invoked locally as a slash command the PR number is `$ARGUMENTS`
(e.g. `/review-pr 2933`). If `$ARGUMENTS` is empty, detect from the current branch.

---

## Step 1 — Resolve PR number and repository

```bash
# If PR_NUMBER is not set and $ARGUMENTS is empty, detect from current branch:
gh pr view --json number -q .number

# If REPO is not set, detect:
gh repo view --json nameWithOwner -q .nameWithOwner
```

---

## Step 2 — Fetch diffs

**If `EVENT_ACTION` is `synchronize` and both `BEFORE_SHA` and `AFTER_SHA` are set:**

Attempt to produce the delta diff (new commits only):

```bash
git fetch --depth=1 origin <BEFORE_SHA>
git diff <BEFORE_SHA>..<AFTER_SHA>
```

- If the delta diff is non-empty, use it as the **delta diff**. Do not fetch the full
  diff — it is not needed (the existing sticky summary covers the earlier commits).
- If the fetch fails (e.g. force-push removed `BEFORE_SHA`) or the result is empty,
  fall back: fetch the full diff and use it for both inline comments and summary.

```bash
gh pr diff <PR_NUMBER>   # fallback only
```

**Otherwise** (event is `opened`, `reopened`, `ready_for_review`, or this is a local run):

Fetch the full diff and use it as both the full diff and the delta diff:

```bash
gh pr diff <PR_NUMBER>
```

---

## Step 3 — Fetch existing inline review comments and sticky summary

Fetch all existing inline review comments (for deduplication):

```bash
gh api --paginate "repos/<REPO>/pulls/<PR_NUMBER>/comments" \
  --jq '[.[] | {path: .path, line: (.line // .original_line), author: .user.login, body: (.body | .[0:300])}]'
```

Also fetch the existing sticky summary comment, if any (for incremental updates):

```bash
gh api "repos/<REPO>/issues/<PR_NUMBER>/comments" \
  --jq '[.[] | select(.body | startswith("## ArcticDB Code Review Summary"))] | last | {id: .id, body: .body}'
```

---

## Step 4 — Deduplication rule

Before posting any inline comment, check the existing comments for an entry with the
same `path` and `line`:

- **Human comment exists**: skip the inline comment. Note the finding in the summary
  checklist if relevant.
- **Previous Claude comment exists**: skip the inline comment. Ensure the finding is
  still reflected in the summary checklist.
- **No existing comment**: post the inline comment normally.

---

## Step 5 — Review outputs

### Inline comments
Post only on lines present in the **delta diff**, subject to the deduplication rule above.

### Summary checklist

**If `EVENT_ACTION` is `synchronize` and a previous sticky summary exists:**

- Evaluate only the **delta diff** against the guidelines.
- Read the previous summary checklist.
- Update individual checklist items that are affected by the delta:
  - A new commit that **introduces** an issue: change the item to ❌ and add a note.
  - A new commit that **fixes** a previously flagged issue: change the item to ✅ and
    note the fix.
  - Items unaffected by the delta: leave unchanged.
- Post the amended summary as the updated sticky comment.

**Otherwise** (event is `opened`, `reopened`, `ready_for_review`, or no previous summary):

Evaluate the **full diff** and post a fresh summary checklist.

---

## Step 6 — Load review guidelines

Read the file `docs/claude/PR_REVIEW_GUIDELINES.md` from the repository root using the
Read tool. Apply all sections relevant to the files changed in the PR. The file also
contains the summary checklist format to use when posting the sticky comment.
