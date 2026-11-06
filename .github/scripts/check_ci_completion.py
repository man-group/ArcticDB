"""Fail the aggregate check when a required CI job failed or skipped unexpectedly."""

import json
import os
import sys


def main():
    jobs = json.loads(os.environ["CI_JOB_RESULTS"])
    allowed_skips = set()
    if jobs["detect_changes"].get("outputs", {}).get("docs_only") == "true":
        allowed_skips.update(os.environ["CI_BUILD_JOBS"].split())
    if jobs.get("storage_type", {}).get("outputs", {}).get("storage") == "no":
        allowed_skips.update(os.environ.get("CI_STORAGE_JOBS", "").split())
    failed = False
    for name, job in jobs.items():
        result = job["result"]
        print(f"{name}: {result}")
        if result != "success" and not (result == "skipped" and name in allowed_skips):
            print(f"::error::{name} did not complete successfully ({result})")
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
