"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import gc
from contextlib import contextmanager


@contextmanager
def deferred_gc():
    """Suspend cyclic garbage collection for the duration of the block.

    For a loop that allocates a large number of container objects and keeps every one of them alive,
    CPython's generational collector is pure overhead: it is scheduled off the count of allocations, so a
    long enough loop runs it repeatedly, and each run re-traces a young generation that is still entirely
    reachable and so frees nothing. Deferring it turns that quadratic re-tracing into a single collection
    later, once the objects have been handed to the caller.

    Only use this where the block's allocations are known to survive it. Reference counting still reclaims
    everything that is not part of a cycle, so a block that churns through acyclic temporaries needs no
    help from this; a block that builds cyclic garbage will hold on to it until collection resumes.

    Restores the previous state on the way out, including when the block raises, and leaves collection off
    if it was already off - a caller that has deliberately disabled it must not have it switched back on.
    No collection is forced on exit: the objects the block allocated are live, so a collection there would
    cost exactly the trace this is avoiding.
    """
    was_enabled = gc.isenabled()
    if was_enabled:
        gc.disable()
    try:
        yield
    finally:
        if was_enabled:
            gc.enable()


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to True or False.

    If the string is not one of the values below we return False.

    This function raises if and only if `val` is not a `str`, in which case it raises an AttributeError.
    """
    if not isinstance(val, str):
        raise AttributeError(f"Expected isinstance(val, str) but type(val)=[{type(val)}]")
    val = val.lower()
    return val in ("y", "yes", "t", "true", "on", "1")
