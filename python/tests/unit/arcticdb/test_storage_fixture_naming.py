"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import re
import uuid
from unittest import mock

from arcticdb.storage_fixtures.azure import new_container_name


def test_container_name_is_not_derived_from_the_clock():
    # xdist workers are separate processes, so a clock-derived name collides whenever two of them create a
    # fixture within the same tick and azurite rejects the second with "container already exists".
    with mock.patch.object(uuid, "uuid1", return_value=uuid.UUID(int=0)):
        names = {new_container_name() for _ in range(100)}
    assert len(names) == 100


def test_container_name_is_a_valid_azure_container_name():
    # 3-63 characters, lowercase letters, digits and hyphens, starting with a letter or digit.
    assert re.fullmatch(r"[a-z0-9][a-z0-9-]{2,62}", new_container_name())
