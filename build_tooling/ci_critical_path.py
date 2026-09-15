#!/usr/bin/env python3
"""Summarise what set the wall-clock time of a CI run.

Wall time is not the sum of the work: with runners to spare it is the longest *chain* of jobs. This prints, as
GitHub-flavoured markdown, when the run became merge-ready, which jobs ended last, and a gantt of the tail, so the
next thing worth optimising is obvious from the run page.

    python3 build_tooling/ci_critical_path.py                      # the current run, in Actions
    python3 build_tooling/ci_critical_path.py --run-id 33304573296 # any run, locally

In a workflow, append it to the step summary:

    python3 build_tooling/ci_critical_path.py >> $GITHUB_STEP_SUMMARY
"""

import argparse
import datetime as dt
import json
import os
import subprocess
import sys

GATE_JOBS = ("can_merge",)  # jobs that mark "the run has said what a developer needs to know"


def gh_api(path):
    out = subprocess.run(["gh", "api", path, "--paginate"], capture_output=True, text=True)
    if out.returncode != 0:
        print(f"could not read {path}: {out.stderr.strip()[:200]}", file=sys.stderr)
        return []
    decoder, text, results, idx = json.JSONDecoder(), out.stdout.strip(), [], 0
    while idx < len(text):
        obj, idx = decoder.raw_decode(text, idx)
        results.append(obj)
        while idx < len(text) and text[idx] in " \n\r\t":
            idx += 1
    return results


def parse(ts):
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "man-group/ArcticDB"))
    ap.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID"))
    ap.add_argument("--top", type=int, default=10, help="how many jobs to show")
    args = ap.parse_args()
    if not args.run_id:
        sys.exit("no run id: pass --run-id or set GITHUB_RUN_ID")

    run = next(iter(gh_api(f"/repos/{args.repo}/actions/runs/{args.run_id}")), None)
    if not run:
        sys.exit("could not read the run")
    started = parse(run.get("run_started_at") or run["created_at"])

    jobs = []
    for page in gh_api(f"/repos/{args.repo}/actions/runs/{args.run_id}/jobs?per_page=100"):
        jobs.extend(page.get("jobs", []))
    done = [
        (
            (parse(j["completed_at"]) - started).total_seconds() / 60,
            (parse(j["started_at"]) - started).total_seconds() / 60,
            j["name"].split(" (")[0],
            j["conclusion"],
        )
        for j in jobs
        if j.get("started_at") and j.get("completed_at") and j.get("conclusion") != "skipped"
    ]
    if not done:
        sys.exit("no completed jobs yet")
    done.sort(reverse=True)

    end = done[0][0]
    gate = [row for row in done if any(g in row[2] for g in GATE_JOBS)]
    print("## Critical path\n")
    if gate:
        print(f"**Merge-ready after {gate[0][0]:.0f} min**, run ends at {end:.0f} min "
              f"(the difference is post-processing nobody waits for).\n")
    else:
        print(f"**Run ends at {end:.0f} min.**\n")

    print("| ends | starts | duration | job |")
    print("|-----:|-------:|---------:|-----|")
    for ends, starts, name, conclusion in done[: args.top]:
        mark = "" if conclusion == "success" else f" ({conclusion})"
        print(f"| {ends:.0f}m | {starts:.0f}m | **{ends - starts:.0f}m** | {name}{mark} |")

    tail = [row for row in done[: args.top] if row[0] > end - 25]
    if tail:
        print("\n```mermaid\ngantt")
        print("    title The last jobs to finish (minutes from run start)")
        print("    dateFormat X\n    axisFormat %M")
        for ends, starts, name, _ in reversed(tail):
            label = "".join(" " if c in ":,{}#;" else c for c in name)[:44].strip()
            print(f"    {label} :{int(starts * 60)}, {int(ends * 60)}")
        print("```")

    longest = done[0]
    contenders = [row for row in done if row[2] != longest[2]]
    if contenders:
        print(f"\nWithout `{longest[2]}` the run would end at {contenders[0][0]:.0f} min "
              f"({longest[0] - contenders[0][0]:.0f} min sooner), and `{contenders[0][2]}` would set it instead.")
    print(f"\n{len(done)} jobs, {sum(row[0] - row[1] for row in done):.0f} job-minutes.")


if __name__ == "__main__":
    main()
