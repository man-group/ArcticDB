#!/usr/bin/env python3
"""Shared utilities for CI failure tracking scripts."""
import subprocess
import sys


def run_gh(*args: str, check: bool = True) -> str:
    """Run a gh CLI command and return stdout.

    On failure (when check=True), prints a meaningful error message to stderr
    before raising.
    """
    result = subprocess.run(
        ["gh", *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        cmd_str = " ".join(["gh", *args[:3]])  # Truncate for readability
        print(
            f"gh command failed (exit {result.returncode}): {cmd_str}",
            file=sys.stderr,
        )
        if result.stderr:
            print(f"  stderr: {result.stderr.strip()[:500]}", file=sys.stderr)
        if check:
            raise subprocess.CalledProcessError(
                result.returncode, ["gh", *args],
                output=result.stdout, stderr=result.stderr,
            )
    return result.stdout.strip()
