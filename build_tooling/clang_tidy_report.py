#!/usr/bin/env python3
"""Turn a raw clang-tidy log into a verdict and a GitHub step summary.

clang-tidy re-reports a header diagnostic once per translation unit that includes
the header, so the raw log over-counts badly on a header-heavy codebase. This
script deduplicates on (file, line, column, check, message) before counting.

It also separates three failure modes that clang-tidy's own exit code conflates:
findings, translation units that crashed or timed out, and clang-diagnostic-error
(which means the compilation database does not match the compiler in use).
"""

import argparse
import os
import re
import sys
from collections import Counter

DIAGNOSTIC = re.compile(
    r"^(?P<file>[^\s:][^:]*):(?P<line>\d+):(?P<col>\d+): "
    r"(?P<severity>warning|error): (?P<message>.*?) \[(?P<check>[\w.,-]+)\]\s*$"
)
CRASH_MARKERS = ("Terminated by signal", "Terminated by timeout", "Failed: ")

# GitHub truncates a step summary at 1 MiB, so cap the detail table and point at
# the job log for the rest.
MAX_SUMMARY_ROWS = 300


def normalise(path, workspace):
    """Make paths comparable and short: absolute -> repo-relative, '/../' collapsed."""
    path = os.path.normpath(path)
    if workspace:
        workspace = os.path.normpath(workspace)
        if path.startswith(workspace + os.sep):
            path = path[len(workspace) + 1 :]
    return path


def parse(log_path, workspace):
    findings = {}
    crashes = []
    with open(log_path, errors="replace") as handle:
        for raw in handle:
            line = raw.rstrip("\n")
            if any(marker in line for marker in CRASH_MARKERS):
                crashes.append(line.strip())
                continue
            match = DIAGNOSTIC.match(line)
            if not match:
                continue
            key = (
                normalise(match["file"], workspace),
                int(match["line"]),
                int(match["col"]),
                match["check"],
                match["message"],
            )
            findings.setdefault(key, match["severity"])
    return findings, crashes


def write_summary(handle, findings, crashes, compile_errors, mode):
    handle.write(f"## clang-tidy ({mode})\n\n")

    if compile_errors:
        handle.write(
            f"**{len(compile_errors)} compilation error(s).** The compilation database does not "
            "match the compiler clang-tidy is using; the analysis below is incomplete.\n\n"
        )
        for key in sorted(compile_errors)[:20]:
            handle.write(f"- `{key[0]}:{key[1]}` {key[4]}\n")
        handle.write("\n")

    if crashes:
        handle.write(f"**{len(crashes)} translation unit(s) crashed or timed out.**\n\n")
        for crash in crashes[:20]:
            handle.write(f"- {crash}\n")
        handle.write("\n")

    warnings = sorted(key for key, severity in findings.items() if severity == "warning")
    if not warnings:
        handle.write("No findings.\n")
        return

    handle.write(f"**{len(warnings)} finding(s)** (deduplicated).\n\n")
    handle.write("| Count | Check |\n|---:|:---|\n")
    for check, count in Counter(key[3] for key in warnings).most_common():
        url = f"https://clang.llvm.org/extra/clang-tidy/checks/{check.replace('-', '/', 1)}.html"
        handle.write(f"| {count} | [{check}]({url}) |\n")

    handle.write("\n| Location | Check | Message |\n|:---|:---|:---|\n")
    for path, line, col, check, message in warnings[:MAX_SUMMARY_ROWS]:
        message = message.replace("|", "\\|")
        handle.write(f"| `{path}:{line}:{col}` | {check} | {message} |\n")
    if len(warnings) > MAX_SUMMARY_ROWS:
        remaining = len(warnings) - MAX_SUMMARY_ROWS
        handle.write(f"\n{remaining} more finding(s) omitted; see the job log.\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", help="Raw clang-tidy output")
    parser.add_argument("--summary", help="Markdown summary destination, e.g. $GITHUB_STEP_SUMMARY")
    parser.add_argument("--workspace", default=os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
    parser.add_argument("--mode", default="diff", help="Label for the summary heading")
    parser.add_argument(
        "--tidy-exit",
        type=int,
        default=0,
        help="Exit code of the clang-tidy invocation, used to catch failures that produced no "
        "parseable diagnostic at all",
    )
    args = parser.parse_args()

    findings, crashes = parse(args.log, args.workspace)
    compile_errors = [key for key, severity in findings.items() if severity == "error"]
    warnings = [key for key, severity in findings.items() if severity == "warning"]

    write_summary(sys.stdout, findings, crashes, compile_errors, args.mode)
    if args.summary:
        with open(args.summary, "a") as handle:
            write_summary(handle, findings, crashes, compile_errors, args.mode)

    if compile_errors:
        print(f"::error::clang-tidy hit {len(compile_errors)} compilation error(s)")
        return 1
    if crashes:
        print(f"::error::{len(crashes)} translation unit(s) crashed or timed out under clang-tidy")
        return 1
    if warnings:
        print(f"::error::clang-tidy reported {len(warnings)} finding(s)")
        return 1
    if args.tidy_exit != 0:
        print(f"::error::clang-tidy exited {args.tidy_exit} without emitting a parseable diagnostic")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
