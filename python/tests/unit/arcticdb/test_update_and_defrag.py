"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random

import numpy as np
import pandas as pd
import pytest

from arcticdb.util.update_and_defrag import _generate_levels, _update_and_defrag
from arcticdb.util.test import assert_frame_equal


def test_generate_levels():
    assert _generate_levels(64, 2) == [64, 32, 16, 8, 4, 2]
    assert _generate_levels(64, 4) == [64, 16, 4]
    assert _generate_levels(5_000, 2) == [5_000, 2_500, 1_250, 625, 312, 156, 78, 39, 19, 9, 4, 2]
    assert _generate_levels(5_000, 10) == [5_000, 500, 50, 5]
