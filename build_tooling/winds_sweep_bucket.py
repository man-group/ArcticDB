#!/usr/bin/env python3
# ===========================================================================
# EXPERIMENT — NOT PART OF THE PIPELINE. Delete this file and the branch it
# lives on (`benchmark-winds-real-ab`) when the experiment is over.
# ===========================================================================
"""Empty and delete one throwaway bucket, fast enough to finish.

arctic-winds gha 99. `aws s3 rb --force` walks the bucket one 1,000-key page at a
time and issues one `delete-objects` per page, serially. r2 left 136,517 objects
and 88.7 GB behind, and the CLI did not get through them inside the teardown job's
30-minute timeout — so the job was cancelled mid-delete and the bucket survived
half-emptied, which is precisely the stranded-bucket failure this experiment must
never leave lying around.

This does the same work with the listing and the deleting overlapped across a
thread pool. It is idempotent: run it again on a bucket that is already gone and
it says so and exits 0, because the whole point is that a sweep must be safe to
repeat.

    python3 winds_sweep_bucket.py <bucket>
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import botocore.exceptions

THREADS = 16
PAGE = 1000


def main(bucket: str) -> int:
    s3 = boto3.client("s3", region_name="eu-west-1")

    try:
        s3.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket"):
            print(f"bucket `{bucket}` does not exist — nothing to sweep")
            return 0
        raise

    started = time.time()
    deleted = 0

    def drop(keys):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
        return len(keys)

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        pending = []
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, PaginationConfig={"PageSize": PAGE}
        ):
            contents = page.get("Contents", [])
            if not contents:
                continue
            pending.append(pool.submit(drop, [{"Key": item["Key"]} for item in contents]))
            # Keep the queue bounded so a huge bucket does not build a huge list of
            # futures before any of them are reaped.
            if len(pending) >= THREADS * 4:
                for future in pending:
                    deleted += future.result()
                pending = []
                print(f"  {deleted:,} deleted, {time.time() - started:,.0f}s", flush=True)
        for future in pending:
            deleted += future.result()

    s3.delete_bucket(Bucket=bucket)
    print(f"swept `{bucket}`: **{deleted:,} objects** deleted in "
          f"{time.time() - started:,.0f}s, bucket removed")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: winds_sweep_bucket.py <bucket>")
    raise SystemExit(main(sys.argv[1]))
