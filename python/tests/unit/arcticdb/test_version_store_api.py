"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import itertools
import inspect
from inspect import Parameter
from unittest.mock import patch, MagicMock, ANY

from arcticdb.version_store._store import NativeVersionStore


def nonempty_powerset(s):
    return itertools.chain.from_iterable(itertools.combinations(s, r + 1) for r in range(len(s)))


class Bail(Exception):
    pass


@pytest.mark.parametrize(
    "name",
    [
        n
        for n in dir(NativeVersionStore)
        if n.startswith("batch_")
        and n not in ["batch_read_metadata_multi", "batch_read_and_join", "batch_delete_symbols"]
    ],
)
def test_calling_batch_methods_with_non_batch_params(name):
    batch_method = getattr(NativeVersionStore, name)
    non_batch_method = getattr(NativeVersionStore, name[6:])
    non_batch_params = set(inspect.signature(non_batch_method).parameters)
    required_params = {}
    has_kw = False
    for p in inspect.signature(batch_method).parameters.values():
        non_batch_params.discard(p.name)
        if p.kind == Parameter.VAR_KEYWORD:
            has_kw = True
        if p.kind not in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD) and p.default is Parameter.empty:
            required_params[p.name] = MagicMock()
    if not has_kw:
        pytest.skip(batch_method.__name__ + " do not take kwargs")

    with patch("arcticdb.version_store._store.log") as logger:
        logger.warning.side_effect = Bail
        for extra_params in nonempty_powerset(non_batch_params):
            d = dict(((n, MagicMock()) for n in extra_params), **required_params)
            try:
                batch_method(**d)
            except Bail:
                pass
            logger.warning.assert_called_with(ANY, set(extra_params), batch_method.__name__)
