#!/usr/bin/env python3
"""Summarise a SeaweedFS master `/vol/status` document by collection.

SeaweedFS backs each S3 bucket with a collection of the same name, so the file count
and byte size of a collection are how much the benchmarks actually put on the server.
Taken before and after a benchmark run, the difference is direct evidence that the
suite wrote to SeaweedFS — independent of anything the S3 gateway chose to report
about itself, and of any wall time that happened to move.

Its own file rather than a `python3 -c` in the workflow: the nesting is three deep
(data centre, rack, node) and an inline heredoc inside a YAML block scalar has to be
indented to match the step, which a multi-line Python literal cannot be.

Usage: seaweed_collection_sizes.py <vol-status.json>
"""

import collections
import json
import sys


def summarise(status: dict) -> list[tuple[str, int, int]]:
    """(collection, file count, bytes) per collection, biggest first.

    The empty-string collection is SeaweedFS's default one — volumes that belong to no
    bucket. It is kept rather than filtered: "the named bucket has the files and the
    default collection is empty" is a stronger reading than the bucket's row alone.
    """
    files: collections.Counter = collections.Counter()
    size: collections.Counter = collections.Counter()
    for racks in (status.get("Volumes", {}).get("DataCenters") or {}).values():
        for nodes in (racks or {}).values():
            for volumes in (nodes or {}).values():
                for volume in volumes or []:
                    name = volume.get("Collection", "")
                    files[name] += volume.get("FileCount", 0)
                    size[name] += volume.get("Size", 0)
    return sorted(
        ((name, files[name], size[name]) for name in files),
        key=lambda row: row[2],
        reverse=True,
    )


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1]) as handle:
        status = json.load(handle)
    rows = summarise(status)
    if not rows:
        print("no volumes reported")
        return 0
    width = max(len(repr(name)) for name, _, _ in rows)
    for name, count, byte_count in rows:
        print(f"collection={repr(name):<{width}}  files={count:>8}  bytes={byte_count:>14,}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
