#!/usr/bin/env python3
"""
Filter sanitizer output to only show leaks related to arcticdb.
Saves the full output to a file and prints only arcticdb-related leaks to stdout.
"""

import sys
import re
from typing import List, Tuple


def is_arcticdb_leak(leak_text: str) -> bool:
    """
    Determine if a leak is from ArcticDB code (not third-party libraries).

    A leak is considered ArcticDB-related if:
    - It has arcticdb in function names or namespaces (e.g., arcticdb::, arcticdb/)
    - But NOT if it's only in:
      - Third-party paths (pybind11, folly, arrow, etc.)
      - Build paths (/__w/ArcticDB/ is the GitHub Actions workspace)
      - Python interpreter paths
    """
    # Exclude third-party and Python internals
    third_party_patterns = [
        r"pybind11::",
        r"third_party/pybind11",
        r"PyModule_",
        r"_PyBytes_",
        r"PyType_",
        r"/site-packages/",
        r"libarrow",
        r"folly::",
        r"/python3\.",
        r"libstdc\+\+",
    ]

    # If it matches third-party patterns, check if it's really arcticdb code
    for pattern in third_party_patterns:
        if re.search(pattern, leak_text):
            # This is third-party, but check if there's actual arcticdb code in the stack
            # Look for arcticdb namespace or source files (not just build paths)
            if re.search(r"arcticdb::[a-zA-Z_]", leak_text) or re.search(r"/arcticdb/[a-z_]+/", leak_text):
                # Has real arcticdb code in the stack, could be our leak
                break
            else:
                # Only third-party code, not our leak
                return False

    # Check for arcticdb namespace or source paths
    if re.search(r"arcticdb::", leak_text) or re.search(r"/cpp/arcticdb/(?!third_party)", leak_text):
        return True

    return False


def parse_leaks(output: str) -> List[Tuple[str, bool]]:
    """
    Parse the output and extract leak reports.
    Returns a list of tuples: (leak_text, is_arcticdb_related)
    """
    leaks = []

    # Split on leak markers
    # Leaks start with "Direct leak of" or "Indirect leak of"
    leak_pattern = r"^(Direct leak of|Indirect leak of)"

    lines = output.split("\n")
    current_leak = []
    in_leak = False

    for line in lines:
        # Check if this line starts a new leak
        if re.search(leak_pattern, line, re.MULTILINE):
            # Save previous leak if exists
            if current_leak:
                leak_text = "\n".join(current_leak)
                is_arcticdb = is_arcticdb_leak(leak_text)
                leaks.append((leak_text, is_arcticdb))

            # Start new leak
            current_leak = [line]
            in_leak = True
        elif in_leak:
            # Continue collecting leak lines
            # A leak continues until we hit a blank line or another leak starts
            if line.strip() == "":
                # End of current leak
                if current_leak:
                    leak_text = "\n".join(current_leak)
                    is_arcticdb = is_arcticdb_leak(leak_text)
                    leaks.append((leak_text, is_arcticdb))
                    current_leak = []
                in_leak = False
            else:
                current_leak.append(line)

    # Don't forget the last leak if there was no trailing blank line
    if current_leak:
        leak_text = "\n".join(current_leak)
        is_arcticdb = is_arcticdb_leak(leak_text)
        leaks.append((leak_text, is_arcticdb))

    return leaks


def main():
    # Read all input from stdin
    full_output = sys.stdin.read()

    # Save full output to file
    output_file = "sanitizer_full_output.txt"
    with open(output_file, "w") as f:
        f.write(full_output)
    print(f"Full sanitizer output saved to {output_file}", file=sys.stderr)

    # Parse and filter leaks
    leaks = parse_leaks(full_output)

    # Count total and arcticdb leaks
    total_leaks = len(leaks)
    arcticdb_leaks = [leak for leak, is_arcticdb in leaks if is_arcticdb]
    non_arcticdb_leaks = total_leaks - len(arcticdb_leaks)

    print(f"\n{'=' * 80}", file=sys.stderr)
    print(f"LEAK SUMMARY", file=sys.stderr)
    print(f"{'=' * 80}", file=sys.stderr)
    print(f"Total leaks found: {total_leaks}", file=sys.stderr)
    print(f"ArcticDB-related leaks: {len(arcticdb_leaks)}", file=sys.stderr)
    print(f"Non-ArcticDB leaks (filtered out): {non_arcticdb_leaks}", file=sys.stderr)
    print(f"{'=' * 80}\n", file=sys.stderr)

    # Print the original output but with a summary
    print(full_output)

    # If there are arcticdb leaks, print them at the end with highlighting
    if arcticdb_leaks:
        print("\n" + "=" * 80, file=sys.stderr)
        print("ARCTICDB-RELATED LEAKS DETECTED:", file=sys.stderr)
        print("=" * 80, file=sys.stderr)
        for leak in arcticdb_leaks:
            print(leak, file=sys.stderr)
            print("-" * 80, file=sys.stderr)

        # Exit with error code to fail the build
        sys.exit(1)
    else:
        print("\n" + "=" * 80, file=sys.stderr)
        print("No ArcticDB-related leaks detected!", file=sys.stderr)
        print("=" * 80, file=sys.stderr)
        sys.exit(0)


if __name__ == "__main__":
    main()
