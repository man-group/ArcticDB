import pytest
import pandas as pd

from arcticdb.version_store import NativeVersionStore


@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("mode", ("no_prune", "lib_config", "arg"))
def test_compact_incomplete_prune_previous(mode: str, append: bool, version_store_factory):
    lib: NativeVersionStore = version_store_factory(prune_previous_version=(mode == "lib_config"))
    lib.write("sym", pd.DataFrame({"col": [3]}, index=pd.DatetimeIndex([0])))
    lib.append("sym", pd.DataFrame({"col": [4]}, index=pd.DatetimeIndex([1])), incomplete=True)

    prune_arg = True if mode == "arg" else None
    lib.compact_incomplete("sym", append, convert_int_to_float=False, prune_previous_version=prune_arg)
    assert lib.read_metadata("sym").version == 1

    assert lib.has_symbol("sym", 0) == (mode == "no_prune")
