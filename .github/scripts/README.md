# CI Failure Tracking Scripts

These scripts are called by the `CI failure notification` workflow
(`.github/workflows/failure_notification.yaml`) to parse CI failures,
create/update GitHub issues, and send Slack notifications.

All scripts are Python 3 (stdlib only, no third-party packages) and use the
`gh` CLI for GitHub API access.

## Triggers

| Trigger | When | What happens |
|---------|------|--------------|
| `workflow_run` | A tracked CI workflow fails on `master` | Parse → track issues → Slack notification |
| `workflow_dispatch` | Manual run with a specific run ID | Parse → track issues (no Slack) |
| `issue_comment` | `/analyse-failures` comment on a PR | Resolve latest failed run → parse → track issues → PR comment |

## Pipeline overview

```
  /analyse-failures ──▶ ┌───────────────────┐
       on PR             │resolve_pr_run.py  │
                         │ IN:  PR number     │
                         │ OUT: run ID        │
                         └────────┬──────────┘
                                  │ run ID
                                  ▼
                     ┌──────────────────────┐     ┌─────────────────────┐
  workflow_run /     │parse_ci_failures.py  │ ──▶ │ track_ci_issues.py  │
  workflow_dispatch  │                      │     │                     │
  also feed in here  │ IN:  run ID          │     │ IN:  text files      │
          ──────────▶│ OUT: text files      │     │ OUT: GH issues +     │
                     │      (test names,    │     │      slack summary   │
                     │       step names)    │     │      file            │
                     └──────────────────────┘     └──────────┬──────────┘
                                                             │ summary
                                                             │
                          master ┌───────────────────────┐   │   PR ┌────────────┐
                          only   │send_slack_notif..py   │◀──┴────▶│ PR comment │
                          ──────▶│ IN: slack summary     │         │            │
                                 │ OUT: Slack message    │         └────────────┘
                                 └───────────────────────┘
```

## Scripts

### parse_ci_failures.py

Fetches job/step metadata and logs for a failed GitHub Actions run,
extracts individual test names and infrastructure step failures.

Recognised test failure patterns:
- GoogleTest / RapidCheck: `[  FAILED  ] TestSuite.TestName/Param` (parameterised suffix stripped).
  RapidCheck tests use GoogleTest integration (`RC_GTEST_PROP`) so failures appear in the same format.
- pytest: `FAILED path/to/test.py::TestClass::test_method`
- pytest errors: `ERROR path/to/test.py::test_name - ...` (collection errors, fixture failures, etc.)

```
python3 parse_ci_failures.py --run-id <RUN_ID> --repo <OWNER/REPO> --output-dir <DIR>
```

| Argument | Required | Description |
|----------|----------|-------------|
| `--run-id` | yes | Numeric workflow run ID (from the URL) |
| `--repo` | yes | Repository in `owner/repo` format |
| `--output-dir` | yes | Directory to write output files into (created if missing) |

**Output files** (written to `--output-dir`):

`failing_tests.txt` — one test per line, deduplicated and sorted:
```
TestVersionMap.TestWriteAndReadVersion
TestCodecVersion1.RoundtripLz4
python/tests/unit/arcticdb/test_arctic.py::TestArcticBasic::test_list_libraries
python/tests/unit/arcticdb/test_arctic.py::TestArcticBasic::test_delete_library
```

`failed_steps.txt` — one step name per line, deduplicated across jobs.
Test-runner steps (matching `test`, `pytest`, `ctest`) are always filtered out
— when test names were parsed they're redundant, and when no test names were
parsed their presence indicates a timeout rather than an infra failure:
```
Install system dependencies
Fetch vcpkg cache
```

`failure_kind.txt` — a single word classifying the failure:

| Value | Meaning |
|-------|---------|
| `test_failure` | Individual test names were parsed from logs |
| `infra_failure` | Only infrastructure steps failed (no test names) |
| `timeout` | Only test-runner steps failed with no test names (likely timed out) |
| `unknown` | No failed steps or tests found at all |

When no failures are found, the text files are empty (0 bytes) and
`failure_kind.txt` is `unknown`.

---

### track_ci_issues.py

Reads the parsed failure files, creates or updates GitHub issues (one per
failing test/step), and writes a Slack summary.

```
python3 track_ci_issues.py --input-dir <DIR> --run-id <RUN_ID> --run-url <URL> \
                           --repo <OWNER/REPO> --commit-sha <SHA> \
                           --conclusion <CONCLUSION> --output-file <FILE>
```

| Argument | Required | Description |
|----------|----------|-------------|
| `--input-dir` | yes | Directory containing `failing_tests.txt`, `failed_steps.txt`, `failure_kind.txt` |
| `--run-id` | yes | Numeric workflow run ID |
| `--run-url` | yes | Full URL to the workflow run |
| `--repo` | yes | Repository in `owner/repo` format |
| `--commit-sha` | yes | Full commit SHA of the failing run |
| `--conclusion` | no | Run conclusion from GitHub (`failure`, `timed_out`). If `timed_out`, forces timeout handling regardless of `failure_kind.txt`. |
| `--output-file` | yes | Path to write the Slack summary |

**Behaviour:**

- For each failing test, searches for an existing open issue titled
  `Flaky test: <test_name>` with the `flaky-test` label.
  - **Found:** adds a comment with the new failure details.
  - **Not found:** creates a new issue with labels `flaky-test` + `autogenerated`.
- Same logic for steps with `Flaky step: <step_name>` and `flaky-step` label.
- If >10 tests (or >10 steps) fail in one run, a single grouped issue is
  created instead of individual ones (correlated failures).
- **Timeout:** if `failure_kind.txt` says `timeout` (or `--conclusion` is
  `timed_out`), creates or updates a single `CI timeout` issue.
- **Unparseable:** if neither tests, steps, nor timeout are detected, creates
  or updates a single `CI failures (unparseable)` issue.

**Output file** (`--output-file`) — Slack-formatted summary, one line per item:
```
:rotating_light: *New* — `TestVersionMap.TestWriteAndReadVersion` (<url|issue>)
:warning: *Known* — `TestCodecVersion1.RoundtripLz4` (<url|#98>)
:warning: *Known* — `Fetch vcpkg cache` (<url|#101>)
```

When >10 failures are grouped:
```
:rotating_light: *42 tests failed* — likely correlated (<url|issue>)
```

When timeout:
```
:hourglass: *Timeout* (<url|issue>)
```

When unparseable:
```
:question: Could not identify specific failures (<url|issue>)
```

---

### send_slack_notification.py

Builds and sends a Slack Block Kit message via an incoming webhook.
Uses only the Python standard library (`urllib.request`, `json`).

```
python3 send_slack_notification.py --webhook-url <URL> --workflow-name <NAME> \
  --conclusion <CONCLUSION> --branch <BRANCH> --run-url <URL> \
  --repo <OWNER/REPO> --repo-url <URL> \
  [--summary-file <FILE>] [--tracker-result <RESULT>]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `--webhook-url` | yes | Slack incoming webhook URL |
| `--workflow-name` | yes | Name of the triggering workflow (e.g. `Build and Test`) |
| `--conclusion` | yes | Run conclusion: `failure` or `timed_out` |
| `--branch` | yes | Branch name (e.g. `master`) |
| `--run-url` | yes | Full URL to the workflow run |
| `--repo` | yes | Repository in `owner/repo` format |
| `--repo-url` | yes | Full URL to the repository |
| `--summary-file` | no | Path to the Slack summary file from `track_ci_issues.py` |
| `--tracker-result` | no | Result of the `track-failures` job (`success`, `failure`, etc.) |

If `--tracker-result` is `failure`, the message includes a warning that issue
tracking itself failed instead of the summary.

**Example Slack message (rendered):**

> :fire: **Build and Test** failure on `master`
>
> :rotating_light: *New* — `TestVersionMap.TestWriteAndReadVersion` ([issue](https://...))
> :warning: *Known* — `TestCodecVersion1.RoundtripLz4` ([#98](https://...))
>
> man-group/ArcticDB | [View Failure](https://...)

---

### resolve_pr_run.py

Finds the most recent failed workflow run for a pull request by looking up
the PR's head branch and searching across all tracked CI workflows.

```
python3 resolve_pr_run.py --pr-number <N> --repo <OWNER/REPO>
```

| Argument | Required | Description |
|----------|----------|-------------|
| `--pr-number` | yes | PR number |
| `--repo` | yes | Repository in `owner/repo` format |

Prints the run ID to stdout if a failed run is found. Prints nothing (empty
stdout) if no failures exist. Diagnostic messages go to stderr.

The list of tracked workflows is defined in `TRACKED_WORKFLOWS` inside the
script and must stay in sync with the `workflow_run` trigger in the workflow
YAML.

---

## Using `/analyse-failures` on a PR

Comment `/analyse-failures` on any pull request to analyse its most recent
failed CI run. The workflow will:

1. React to your comment with :eyes:
2. Look up the PR's head branch and find the latest failed run
3. Parse test and step failures from the run logs
4. Create or update tracking issues (same as the master flow)
5. Post a summary comment on the PR
6. React with :rocket: (success) or :-1: (failure)

If no failed runs are found, a comment saying so is posted instead.

---

## Manual usage

You can run the parsing script locally against any failed run to verify output:

```bash
export GH_TOKEN="$(gh auth token)"

# 1. Parse failures
python3 .github/scripts/parse_ci_failures.py \
  --run-id 12345678 \
  --repo man-group/ArcticDB \
  --output-dir /tmp/ci_failures

# 2. Inspect intermediate files
cat /tmp/ci_failures/failing_tests.txt
cat /tmp/ci_failures/failed_steps.txt
cat /tmp/ci_failures/failure_kind.txt

# 3. Create/update issues (will create real GitHub issues!)
python3 .github/scripts/track_ci_issues.py \
  --input-dir /tmp/ci_failures \
  --run-id 12345678 \
  --run-url "https://github.com/man-group/ArcticDB/actions/runs/12345678" \
  --repo man-group/ArcticDB \
  --commit-sha abc123def456 \
  --conclusion failure \
  --output-file /tmp/ci_failures/slack_summary.txt

cat /tmp/ci_failures/slack_summary.txt
```

**Note:** `track_ci_issues.py` will create real GitHub issues — use a test
repository if you want to avoid that.
