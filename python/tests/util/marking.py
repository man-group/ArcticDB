"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from typing import List
import pytest

class Mark:
    """ Pytest mark wrapper class
    Helps create marks and have easy way to access its name
    """
    def __init__(self, name: str):
        self.name = name
        self.mark = getattr(pytest.mark, name)

    def __call__(self, func):
        return self.mark(func)

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"Mark('{self.name}')"


def marks(marks_list: List[Mark]):
    """Decorator allowing to set multiple pytest marks on one line

    NOTE: The list should be ordered the same way as you intend to apply marks if 
    on miltiple lines
    
    Usage:
    ------

        @pmark([Marks.abc, Marks.cde])
        def test_first():
            ....
    """
    def decorator(func):
        # reversed is needed in order to do what python does with marks
        for m in reversed(marks_list): 
            func = m(func)
        return func
    return decorator

