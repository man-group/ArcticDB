import pytest
import pandas as pd

from arcticdb.version_store import NativeVersionStore


@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("arg", (True, False, None))
@pytest.mark.parametrize("lib_config", (True, False))
def test_compact_incomplete_prune_previous(lib_config, arg, append, version_store_factory):
    lib: NativeVersionStore = version_store_factory(prune_previous_version=lib_config)
    lib.write("sym", pd.DataFrame({"col": [3]}, index=pd.DatetimeIndex([0])))
    lib.append("sym", pd.DataFrame({"col": [4]}, index=pd.DatetimeIndex([1])), incomplete=True)

    lib.compact_incomplete("sym", append, convert_int_to_float=False, prune_previous_version=arg)
    assert lib.read_metadata("sym").version == 1

    should_prune = lib_config if arg is None else arg
    assert lib.has_symbol("sym", 0) == (not should_prune)
