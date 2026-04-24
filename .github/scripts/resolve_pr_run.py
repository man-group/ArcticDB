#!/usr/bin/env python3
"""
Find the most recent failed workflow run for a pull request.

Usage:
    resolve_pr_run.py --pr-number <N> --repo <OWNER/REPO>

Prints the run ID to stdout if a failed run is found, or exits with code 0
and empty output if no failed run exists.
"""
import argparse
import json
import subprocess
import sys
from urllib.parse import quote

# The workflows we track — must stay in sync with the workflow_run trigger list
TRACKED_WORKFLOWS = [
    "Build and Test",
    "Build with conda",
    "Build with analysis tools",
    "Coverity Static Analysis",
    "Installation Tests Execution",
]


def run_gh(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["gh", *args],
        capture_output=True,
        text=True,
        check=check,
    )
    return result.stdout.strip()


def get_pr_head(repo: str, pr_number: str) -> tuple[str, str]:
    """Return (head_branch, head_sha) for a PR."""
    raw = run_gh(
        "api", f"repos/{repo}/pulls/{pr_number}",
        "--jq", '{branch: .head.ref, sha: .head.sha}',
    )
    data = json.loads(raw)
    return data["branch"], data["sha"]


def find_latest_failed_run(repo: str, branch: str) -> str | None:
    """Find the most recent failed/timed_out run on the given branch.

    Searches across all tracked workflows and returns the run ID of the
    most recent failure, or None if no failures are found.
    """
    best_run_id = None
    best_run_date = ""
    branch_encoded = quote(branch, safe="")

    for workflow_name in TRACKED_WORKFLOWS:
        # GitHub treats "failure" and "timed_out" as separate status values,
        # so we must query for both.
        for status in ("failure", "timed_out"):
            raw = run_gh(
                "api", "--paginate",
                f"repos/{repo}/actions/runs"
                f"?branch={branch_encoded}&status={status}&per_page=5",
                "--jq", (
                    f'[.workflow_runs[] | select(.name == "{workflow_name}") '
                    f'| {{id, created_at, conclusion}}]'
                ),
                check=False,
            )
            if not raw:
                continue

            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                runs = json.loads(line)
                for run in runs:
                    if run["conclusion"] in ("failure", "timed_out"):
                        if run["created_at"] > best_run_date:
                            best_run_date = run["created_at"]
                            best_run_id = str(run["id"])

    return best_run_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pr-number", required=True, help="PR number")
    parser.add_argument("--repo", required=True, help="owner/repo")
    args = parser.parse_args()

    print(f"Looking up PR #{args.pr_number}...", file=sys.stderr)
    branch, sha = get_pr_head(args.repo, args.pr_number)
    print(f"PR head: branch={branch} sha={sha[:10]}", file=sys.stderr)

    print(f"Searching for failed runs on branch '{branch}'...", file=sys.stderr)
    run_id = find_latest_failed_run(args.repo, branch)

    if run_id:
        print(f"Found failed run: {run_id}", file=sys.stderr)
        print(run_id)
    else:
        print("No failed runs found for this PR.", file=sys.stderr)


if __name__ == "__main__":
    main()
