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


def run_gh(*args: str, check: bool = True) -> str:
    """Run a gh CLI command and return stdout."""
    result = subprocess.run(
        ["gh", *args],
        capture_output=True,
        text=True,
        check=check,
    )
    return result.stdout.strip()


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
    result = subprocess.run(
        ["gh", "run", "view", run_id, "--repo", repo, "--log-failed"],
        capture_output=True,
        text=True,
        check=False,
    )
    with open(output_path, "w") as f:
        f.write(result.stdout)
    if result.returncode != 0:
        print(
            f"WARNING: gh run view --log-failed failed (exit {result.returncode}); "
            "test name parsing will be skipped",
            file=sys.stderr,
        )
        if result.stderr:
            print(f"stderr: {result.stderr[:500]}", file=sys.stderr)
        return False
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


def parse_pytest_failures(log_text: str) -> set[str]:
    """Extract pytest failures and errors from the summary section.

    Matches both:
      FAILED path/to/test.py::TestClass::test_method
      ERROR  path/to/test.py::test_name - FixtureError: ...
    """
    pattern = r"(?:FAILED|ERROR)\s+(\S+::\S+)"
    return {m.group(1) for m in re.finditer(pattern, log_text)}


def filter_infra_steps(
    all_steps: list[str], has_test_failures: bool
) -> list[str]:
    """When test names are known, filter out test-runner steps."""
    if not has_test_failures:
        return all_steps
    test_keywords = re.compile(r"(test|pytest|ctest)", re.IGNORECASE)
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
        for name in ("failing_tests.txt", "failed_steps.txt"):
            open(os.path.join(args.output_dir, name), "w").close()
        return

    print(f"Failed jobs: {json.dumps(failed_jobs, indent=2)}")

    # 2. Collect failed steps from each job
    all_failed_steps: list[str] = []
    for job in failed_jobs:
        step_names = fetch_failed_steps(args.repo, job["id"])
        for step in step_names:
            all_failed_steps.append(f"{job['name']} / {step}")

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

    # 4. Filter infra steps
    infra_steps = filter_infra_steps(all_failed_steps, bool(failing_tests))
    print(f"Infrastructure steps:\n" + "\n".join(infra_steps))

    # 5. Write outputs
    with open(os.path.join(args.output_dir, "failing_tests.txt"), "w") as f:
        f.write("\n".join(failing_tests_sorted) + "\n" if failing_tests_sorted else "")

    with open(os.path.join(args.output_dir, "failed_steps.txt"), "w") as f:
        f.write("\n".join(infra_steps) + "\n" if infra_steps else "")

    print(f"Done: {len(failing_tests_sorted)} failing test(s), {len(infra_steps)} failed step(s).")


if __name__ == "__main__":
    main()
