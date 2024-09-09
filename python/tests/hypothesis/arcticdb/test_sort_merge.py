from re import S
import pandas as pd
import pytest
from hypothesis import assume
import hypothesis.extra.pandas as hs_pd
import hypothesis.strategies as st
from hypothesis.stateful import RuleBasedStateMachine, rule, initialize, run_state_machine_as_test, precondition
from collections import namedtuple
from pandas.testing import assert_frame_equal
from arcticdb.version_store.library import StagedDataFinalizeMethod

ColumnInfo = namedtuple("ColumnInfo", ["name", "dtype"])

COLUMN_DESCRIPTIONS = [
    ColumnInfo("a", "float"),
    ColumnInfo("b", "int64"),
    ColumnInfo("c", "str"),
    ColumnInfo("d", "datetime64[ns]"),
]


@st.composite
def df(draw, column_list):
    column_infos = draw(st.lists(st.sampled_from(column_list), unique=True))
    columns = [hs_pd.column(name=ci.name, dtype=ci.dtype) for ci in column_infos]
    return draw(hs_pd.data_frames(columns, index=hs_pd.indexes(dtype="datetime64[ns]")))


class StagedWrite(RuleBasedStateMachine):

    lib = None

    @initialize()
    def init(self):
        self.df = pd.DataFrame([])

    @rule(df=df(COLUMN_DESCRIPTIONS))
    def stage(self, df):
        self.df = pd.concat([self.df, df])
        self.lib.write("sym", df, staged=True)

    @rule()
    def finalize_write(self):
        self.df.sort_index(inplace=True)
        self.lib.sort_and_finalize_staged_data("sym")
        arctic_df = self.lib.read("sym").data
        assert arctic_df.index.equals(self.df.index)
        assert_frame_equal(arctic_df, self.df, check_like=True)
        self.df = pd.DataFrame([])

    @rule()
    def finalize_append(self):
        self.df.sort_index(inplace=True)
        pre_append_arctic = self.lib.read("sym").data if "sym" in self.lib.list_symbols() else pd.DataFrame([])

        if len(pre_append_arctic) > 0 and len(self.df) > 0 and pre_append_arctic.index[-1] > self.df.index[0]:
            # Make sure appends keep the index ordered
            with pytest.raises(Exception) as exception_info:
                self.lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
            assert "append" in str(exception_info.value)
        else:
            self.lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
            post_append_arctic = self.lib.read("sym").data
            appended_arctic = post_append_arctic[len(pre_append_arctic) :]
            assert appended_arctic.index.equals(self.df.index)
            assert_frame_equal(appended_arctic, self.df, check_like=True)
        self.df = pd.DataFrame([])

    def teardown(self):
        self.df = pd.DataFrame([])
        for sym in self.lib.list_symbols():
            self.lib.delete(sym)


@pytest.mark.skip(reason="Needs to resolve the issues found in unit tests.")
def test_sort_and_finalize_staged_data(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    StagedWrite.lib = ac.create_library(lib_name)
    run_state_machine_as_test(StagedWrite)
