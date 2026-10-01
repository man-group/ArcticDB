"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import json
from types import SimpleNamespace
from unittest import mock

import process_pytest_artifacts as ppa


def _pages(*pages):
    """Stand in for `gh api --paginate`, which concatenates one JSON object per page."""
    return SimpleNamespace(stdout="".join(json.dumps({"artifacts": page}) for page in pages), stderr="")


def test_artifacts_are_deduplicated_across_pages():
    # The listing is paginated by offset and the run is still uploading while we walk it, so an artifact on a page
    # boundary comes back on both pages. Downloading it twice raced on one directory and failed the whole job.
    page_1 = [{"id": 3, "name": "pytest-c"}, {"id": 2, "name": "pytest-b"}]
    page_2 = [{"id": 2, "name": "pytest-b"}, {"id": 1, "name": "pytest-a"}]

    with mock.patch.object(ppa.subprocess, "run", return_value=_pages(page_1, page_2)):
        artifacts = ppa.get_artifacts_for_run("/repos/o/r/actions/runs/1/artifacts")

    assert [a["id"] for a in artifacts] == [3, 2, 1]


def test_all_pages_are_returned():
    with mock.patch.object(
        ppa.subprocess, "run", return_value=_pages([{"id": 2, "name": "b"}], [{"id": 1, "name": "a"}])
    ):
        artifacts = ppa.get_artifacts_for_run("/repos/o/r/actions/runs/1/artifacts")

    assert [a["id"] for a in artifacts] == [2, 1]
