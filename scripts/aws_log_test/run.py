"""Run the harness three times in separate subprocesses, each with a different
AWS.LogLevel, capturing fd-level stdout and stderr to separate files. Then
report whether AWS SDK log lines appear, and on which stream.

AWS LogLevel enum: Off=0, Fatal=1, Error=2, Warn=3, Info=4, Debug=5, Trace=6.
"""

import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
HARNESS = os.path.join(HERE, "harness.py")

# Substrings that only the AWS SDK FormattedLogSystem emits. The SDK format is
# "[<Level>] <Timestamp> <Tag> <message>". We look for the bracketed level tags
# and common AWS client tags. ArcticDB's own logging uses a different format.
AWS_MARKERS = [
    "[TRACE]",
    "[DEBUG]",
    "[INFO]",
    "[WARN]",
    "[ERROR]",
    "AWSClient",
    "AWSAuthV4Signer",
    "CurlHttpClient",
    "S3Client",
    "Aws::",
    "ClientConfiguration",
]

SCENARIOS = [
    ("default", {}),
    ("level6_trace", {"ARCTICDB_AWS_LogLevel_int": "6"}),
    ("level5_debug", {"ARCTICDB_AWS_LogLevel_int": "5"}),
]


def count_markers(text):
    counts = {}
    for m in AWS_MARKERS:
        n = text.count(m)
        if n:
            counts[m] = n
    return counts


def run_one(name, extra_env, outdir):
    env = dict(os.environ)
    env.update(extra_env)
    out_path = os.path.join(outdir, f"{name}.stdout")
    err_path = os.path.join(outdir, f"{name}.stderr")
    with open(out_path, "wb") as out_f, open(err_path, "wb") as err_f:
        proc = subprocess.run(
            [sys.executable, HARNESS],
            env=env,
            stdout=out_f,
            stderr=err_f,
            cwd=outdir,
        )
    out = open(out_path, "r", errors="replace").read()
    err = open(err_path, "r", errors="replace").read()
    return proc.returncode, out, err, out_path, err_path


def first_lines(text, n=8):
    lines = [l for l in text.splitlines() if l.strip()]
    return lines[:n]


def main():
    outdir = tempfile.mkdtemp(prefix="aws_log_test_")
    print(f"Working dir: {outdir}\n")
    summary = {}
    for name, extra_env in SCENARIOS:
        rc, out, err, out_path, err_path = run_one(name, extra_env, outdir)
        out_markers = count_markers(out)
        err_markers = count_markers(err)
        ok_done = "HARNESS_DONE" in out
        summary[name] = (rc, ok_done, out_markers, err_markers)

        print("=" * 70)
        print(f"SCENARIO: {name}   env={extra_env}")
        print(f"  return code: {rc}   harness completed: {ok_done}")
        print(f"  stdout file: {out_path}  ({len(out)} bytes)")
        print(f"  stderr file: {err_path}  ({len(err)} bytes)")
        print(f"  AWS markers on STDOUT: {out_markers or 'none'}")
        print(f"  AWS markers on STDERR: {err_markers or 'none'}")
        # show a sample of stderr lines that look like AWS log lines
        aws_err_lines = [
            l for l in err.splitlines() if re.search(r"^\[(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\]", l.strip())
        ]
        if aws_err_lines:
            print("  sample AWS stderr lines:")
            for l in aws_err_lines[:6]:
                print(f"    {l}")
        print()

    # Also break down level5 vs level6 by which bracket levels appear, to answer
    # "what logs are shown with level 5".
    print("=" * 70)
    print("LEVEL BREAKDOWN (bracketed level tags seen on stderr):")
    for name in ("level6_trace", "level5_debug"):
        _, _, _, err_markers = summary[name]
        levels = {k: v for k, v in err_markers.items() if k.startswith("[")}
        print(f"  {name}: {levels or 'none'}")

    print("\n" + "=" * 70)
    print("ASSERTIONS:")
    rc, done, out_m, err_m = summary["default"]
    default_clean = done and not out_m and not err_m
    print(f"  [default] no AWS logs on stdout or stderr: {'PASS' if default_clean else 'FAIL'}")

    rc6, done6, out6, err6 = summary["level6_trace"]
    lvl6_has_err = done6 and bool(err6)
    lvl6_clean_stdout = not out6
    print(f"  [level6]  AWS logs present on stderr:        {'PASS' if lvl6_has_err else 'FAIL'}")
    print(f"  [level6]  no AWS logs on stdout:             {'PASS' if lvl6_clean_stdout else 'FAIL'}")

    all_pass = default_clean and lvl6_has_err and lvl6_clean_stdout
    print(f"\nOVERALL: {'PASS' if all_pass else 'FAIL'}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
