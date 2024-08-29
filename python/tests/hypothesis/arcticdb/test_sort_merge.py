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
from arcticdb.exceptions import UserInputException, StreamDescriptorMismatch, UnsortedDataException, NoSuchVersionException
import numpy as np
import string


ColumnInfo = namedtuple('ColumnInfo', ['name', 'dtype'])

COLUMN_DESCRIPTIONS = [ColumnInfo("a", "float"), ColumnInfo("b", "int32"), ColumnInfo("c", "object"), ColumnInfo("d", "datetime64[ns]")]
SYMBOL = "sym"

def string_column_strategy(name):
    ascii_alphabet = string.ascii_letters + string.digits + string.punctuation + string.whitespace
    return hs_pd.column(name=name, elements=st.text(alphabet=ascii_alphabet))

@st.composite
def df(draw, column_list):
    column_infos = draw(st.lists(st.sampled_from(column_list), unique=True, min_size=1))
    columns = [hs_pd.column(name=ci.name, dtype=ci.dtype) if ci.dtype != 'object' else string_column_strategy(ci.name) for ci in column_infos]
    index = hs_pd.indexes(dtype="datetime64[ns]")
    return draw(hs_pd.data_frames(columns, index=index))

def fix_dtypes(df):
    if 'b' in list(df.columns):
        df['b'] = df['b'].replace(np.NaN, 0)
    return df

class StagedWrite(RuleBasedStateMachine):

    lib = None
    staged_segments_count = 0
 
    @initialize()
    def init(self):
        self.staged = pd.DataFrame([])
        self.staged_segments_count = 0

    @rule(df=df(COLUMN_DESCRIPTIONS))
    def stage(self, df):
        # pd.concat promotes dtypes. If there are missing values in the int typed column
        # it will become float column and the missing values will be NaN.
        self.staged = fix_dtypes(pd.concat([self.staged, df]))
        self.lib.write(SYMBOL, df, staged=True)
        self.staged_segments_count += 1

    @rule()
    def finalize_write(self):
        has_staged_segments = self.staged_segments_count > 0
        self.staged.sort_index(inplace=True)
        if not has_staged_segments:
            self.assert_cannot_finalize_without_staged_data(StagedDataFinalizeMethod.WRITE)
        elif pd.NaT in self.staged.index:
            self.assert_nat_is_not_supported(StagedDataFinalizeMethod.WRITE)
        else:
            write_empty = len(self.staged) == 0
            self.lib.sort_and_finalize_staged_data(SYMBOL)
            arctic_df = self.lib.read(SYMBOL).data
            assert arctic_df.index.equals(self.staged.index)
            assert_frame_equal(arctic_df, self.staged, check_dtype=False, check_like=True)
        self.staged = pd.DataFrame([])
        self.staged_segments_count = 0

    @rule()
    def finalize_append(self):
        self.staged.sort_index(inplace=True)
        pre_append_storage, symbol_exists = self.data_from_storage()
        symbol_is_not_empty = len(pre_append_storage) > 0
        has_staged_segments = self.staged_segments_count > 0
        stage_is_not_empty = len(self.staged) > 0
        if not has_staged_segments:
            self.assert_cannot_finalize_without_staged_data(StagedDataFinalizeMethod.APPEND)
        elif symbol_exists and list(self.staged.columns) != list(pre_append_storage.columns):
            self.assert_append_throws_with_mismatched_columns()
        elif symbol_is_not_empty and stage_is_not_empty and pre_append_storage.index[-1] > self.staged.index[0]:
            self.assert_appended_data_does_not_overlap_with_storage()
        elif pd.NaT in self.staged.index:
            self.assert_nat_is_not_supported(StagedDataFinalizeMethod.APPEND)
        else:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=StagedDataFinalizeMethod.APPEND)
            post_append_storage = self.lib.tail(SYMBOL).data
            if len(post_append_storage) > 0:
                appended_arctic = self.lib.tail(SYMBOL, n=len(self.staged)).data
                assert appended_arctic.index.equals(self.staged.sort_index().index)
                assert_frame_equal(appended_arctic, self.staged, check_dtype=False, check_like=True)
            else:
                assert_frame_equal(post_append_storage, self.staged, check_dtype=False, check_like=True)
        self.staged = pd.DataFrame([])
        self.staged_segments_count = 0

    def assert_appended_data_does_not_overlap_with_storage(self):
        with pytest.raises(Exception) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=StagedDataFinalizeMethod.APPEND)
        assert "append" in str(exception_info.value)

    def assert_cannot_finalize_without_staged_data(self, mode):
        with pytest.raises(UserInputException) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=mode)
        assert "E_NO_STAGED_SEGMENTS" in str(exception_info.value)

    def assert_append_throws_with_mismatched_columns(self):
        with pytest.raises(StreamDescriptorMismatch) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=StagedDataFinalizeMethod.APPEND)
        assert "APPEND" in str(exception_info.value)

    def assert_nat_is_not_supported(self, mode):
        with pytest.raises(UnsortedDataException) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=mode)
        # TODO: Better exception must be raised
        assert "E_UNSORTED_DATA" in str(exception_info.value)

    def data_from_storage(self):
        try:
            return self.lib.read(SYMBOL).data, True
        except NoSuchVersionException:
            return pd.DataFrame(), False

    def teardown(self):
        self.staged = pd.DataFrame([])
        self.staged_segments_count = 0
        self.lib.delete_staged_data(SYMBOL)
        self.lib.delete(SYMBOL)


def test_sort_and_finalize_staged_data(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    StagedWrite.lib = ac.create_library(lib_name)
    run_state_machine_as_test(StagedWrite)
