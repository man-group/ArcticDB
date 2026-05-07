#!/usr/bin/env python3
"""
Parse a failed GitHub Actions run to extract individual test failures
and infrastructure step failures.

Usage:
    parse_ci_failures.py --run-id <RUN_ID> --repo <OWNER/REPO> --output-dir <DIR>

Outputs (written to --output-dir):
    failing_tests.txt  - One failing test per line (deduplicated, sorted)
    failed_steps.txt   - One "job / step" entry per line (infra-only when tests found)

Requirements: gh (GitHub CLI), jq
"""
import argparse
import json
import os
import re
import subprocess
import sys

from utils import run_gh


def fetch_failed_jobs(repo: str, run_id: str) -> list[dict]:
    """Fetch all failed jobs for a run (handles pagination)."""
    raw = run_gh(
        "api", "--paginate",
        f"repos/{repo}/actions/runs/{run_id}/jobs?filter=latest&per_page=100",
        "--jq", '[.jobs[] | select(.conclusion == "failure") | {id, name}]',
    )
    # --paginate emits one JSON array per page; merge them
    jobs = []
    for line in raw.splitlines():
        line = line.strip()
        if line:
            jobs.extend(json.loads(line))
    return jobs


def fetch_failed_steps(repo: str, job_id: int) -> list[str]:
    """Return names of failed steps within a single job."""
    raw = run_gh(
        "api", f"repos/{repo}/actions/jobs/{job_id}",
        "--jq", '.steps[] | select(.conclusion == "failure") | .name',
    )
    return [s for s in raw.splitlines() if s.strip()]


def download_failed_logs(repo: str, run_id: str, output_path: str) -> bool:
    """Download logs for the failed run. Returns True on success."""
    try:
        output = run_gh("run", "view", run_id, "--repo", repo, "--log-failed")
    except subprocess.CalledProcessError:
        print(
            "WARNING: gh run view --log-failed failed; "
            "test name parsing will be skipped",
            file=sys.stderr,
        )
        return False
    with open(output_path, "w") as f:
        f.write(output)
    return True


def parse_gtest_failures(log_text: str) -> set[str]:
    """Extract GoogleTest failures: [  FAILED  ] TestSuite.TestName/Param"""
    pattern = r"\[\s+FAILED\s+\]\s+([A-Za-z0-9_]+\.[A-Za-z0-9_/]+)"
    results = set()
    for match in re.finditer(pattern, log_text):
        test = match.group(1)
        # Strip parameterised suffix like /0, /1
        test = re.sub(r"/\d+$", "", test)
        results.add(test)
    return results


def normalise_path_separators(test_id: str) -> str:
    """Normalise Windows backslash path separators to forward slashes.

    Windows CI runners produce paths like ``tests\\stress\\arcticdb\\...``
    while Linux/macOS produce ``tests/stress/arcticdb/...``. Normalising
    ensures the same test produces the same ID regardless of platform.
    """
    return test_id.replace("\\", "/")


def strip_parametrize(test_id: str) -> str:
    """Strip pytest parametrize suffix ``[...]`` from a test node ID.

    ``tests/test_foo.py::test_bar[nfs_backed_s3--prefix]``
    → ``tests/test_foo.py::test_bar``

    This groups all parametrizations of the same test into a single
    tracking issue.
    """
    return re.sub(r"\[.*\]$", "", test_id)


def parse_pytest_failures(log_text: str) -> set[str]:
    """Extract pytest failures and errors from the summary section.

    Matches both:
      FAILED path/to/test.py::TestClass::test_method
      ERROR  path/to/test.py::test_name - FixtureError: ...

    Parametrize suffixes (``[...]``) are stripped so that all
    parametrizations of the same test are grouped together.
    """
    pattern = r"(?:FAILED|ERROR)\s+(\S+::\S+)"
    return {normalise_path_separators(strip_parametrize(m.group(1)))
            for m in re.finditer(pattern, log_text)}


def filter_infra_steps(all_steps: list[str]) -> list[str]:
    """Filter out test-runner and benchmark steps, keeping only infrastructure steps.

    Test-runner steps (e.g. "Run test", "Run pytest") and benchmark steps
    (e.g. "Benchmark against master") are always removed:
    - When test names were parsed, test steps are redundant.
    - Benchmark failures are expected outcomes (regressions), not infra issues.
    - When no test names were parsed (e.g. timeout), removing them lets the
      pipeline fall through to the unparseable/timeout fallback rather than
      creating a useless "Flaky step: Run test" issue.
    """
    test_keywords = re.compile(r"(test|pytest|ctest|benchmark)", re.IGNORECASE)
    return [s for s in all_steps if not test_keywords.search(s)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="Workflow run ID")
    parser.add_argument("--repo", required=True, help="owner/repo")
    parser.add_argument("--output-dir", required=True, help="Directory for output files")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # 1. Fetch failed jobs
    print(f"Fetching failed jobs for run {args.run_id}...")
    failed_jobs = fetch_failed_jobs(args.repo, args.run_id)

    if not failed_jobs:
        print("No failed jobs found.")
        return

    print(f"Failed jobs: {json.dumps(failed_jobs, indent=2)}")

    # 2. Collect failed steps from each job (step name only, no job prefix)
    all_failed_steps: list[str] = []
    for job in failed_jobs:
        step_names = fetch_failed_steps(args.repo, job["id"])
        all_failed_steps.extend(step_names)

    all_failed_steps = sorted(set(all_failed_steps))
    print(f"Failed steps:\n" + "\n".join(all_failed_steps))

    # 3. Download logs and parse test failures
    log_path = os.path.join(args.output_dir, "failed_logs.txt")
    log_ok = download_failed_logs(args.repo, args.run_id, log_path)

    failing_tests: set[str] = set()
    if log_ok:
        with open(log_path) as f:
            log_text = f.read()
        failing_tests |= parse_gtest_failures(log_text)
        failing_tests |= parse_pytest_failures(log_text)

    failing_tests_sorted = sorted(failing_tests)
    print(f"Failing tests:\n" + "\n".join(failing_tests_sorted))

    # 4. Filter infra steps (always strip test-runner steps)
    infra_steps = filter_infra_steps(all_failed_steps)
    stripped_test_steps = len(all_failed_steps) - len(infra_steps)
    print(f"Infrastructure steps:\n" + "\n".join(infra_steps))

    # 5. Determine failure kind
    if failing_tests:
        failure_kind = "test_failure"
    elif infra_steps:
        failure_kind = "infra_failure"
    elif stripped_test_steps > 0:
        # All failed steps were test runners but no test names were parsed — timeout
        failure_kind = "timeout"
    else:
        failure_kind = "unknown"

    print(f"Failure kind: {failure_kind}")

    # 6. Write outputs
    with open(os.path.join(args.output_dir, "failing_tests.txt"), "w") as f:
        f.write("\n".join(failing_tests_sorted) + "\n" if failing_tests_sorted else "")

    with open(os.path.join(args.output_dir, "failed_steps.txt"), "w") as f:
        f.write("\n".join(infra_steps) + "\n" if infra_steps else "")

    with open(os.path.join(args.output_dir, "failure_kind.txt"), "w") as f:
        f.write(failure_kind + "\n")

    print(f"Done: {len(failing_tests_sorted)} failing test(s), {len(infra_steps)} failed step(s).")


if __name__ == "__main__":
    main()
