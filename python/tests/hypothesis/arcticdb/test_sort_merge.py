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
from arcticdb.util._versions import IS_PANDAS_TWO


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
    if not IS_PANDAS_TWO:
        # Due to https://github.com/man-group/ArcticDB/blob/7479c0b0caa8121bc2ca71a73e29769bbc41c66a/python/arcticdb/version_store/_normalization.py#L184
        # we change the dtype of empty float columns. This makes hypothesis tests extremely hard to write as we must
        # keep addional state about is there a mix of empty/non-empty float columns in the staging area, did we write
        # empty float column (if so it's type would be object). These edge cases are covered in the unit tests.
        index = hs_pd.indexes(dtype="datetime64[ns]", min_size=1)
    else:
        index = hs_pd.indexes(dtype="datetime64[ns]")
    return draw(hs_pd.data_frames(columns, index=index))

def fix_dtypes(df):
    all_int_columns = {col.name for col in COLUMN_DESCRIPTIONS if "int" in col.dtype}
    int_columns_in_df = all_int_columns.intersection(set(df.columns))
    for col in int_columns_in_df:
        df[col] = df[col].replace(np.NaN, 0)
    return df

def assert_equal(left, right):
    """
    The sorting Arctic does is not stable. Thus when there are repeated index values the
    result from our sorting and the result from Pandas' sort might differ where the index
    values are repeated.
    """
    if any(left.index.duplicated()):
        assert left.index.equals(right.index), f"Indexes are different {left.index} != {right.index}"
        assert set(left.columns) == set(right.columns), f"Column sets are different {set(left.columns)} != {set(right.columns)}"
        assert left.shape == right.shape, f"Shapes are different {left.shape} != {right.shape}"
        left_groups = left.groupby(left.index, sort=False).apply(lambda x: x.sort_values(list(left.columns)))
        right_groups = right.groupby(right.index, sort=False).apply(lambda x: x.sort_values(list(left.columns)))
        assert_frame_equal(left_groups, right_groups, check_like=True, check_dtype=False)
    else:
        assert_frame_equal(left, right, check_like=True, check_dtype=False)

class StagedWrite(RuleBasedStateMachine):

    lib = None
    staged_segments_count = 0
 
    @initialize()
    def init(self):
        self.reset_state()

    @rule(df=df(COLUMN_DESCRIPTIONS))
    def stage(self, df):
        # pd.concat promotes dtypes. If there are missing values in the int typed column
        # it will become float column and the missing values will be NaN.
        self.staged = fix_dtypes(pd.concat([self.staged, df]))
        self.lib.write(SYMBOL, df, staged=True)
        self.staged_segments_count += 1

    @precondition(lambda self: self.staged_segments_count == 0)
    @rule()
    def finalize_write_no_staged_segments(self):
        self.assert_cannot_finalize_without_staged_data(StagedDataFinalizeMethod.WRITE)
        self.reset_state()

    @precondition(lambda self: self.staged_segments_count > 0 and pd.NaT in self.staged.index)
    @rule()
    def finalize_write_with_nat_in_index(self):
        self.assert_nat_is_not_supported(StagedDataFinalizeMethod.WRITE)
        self.reset_state()

    @precondition(lambda self: self.staged_segments_count > 0 and not pd.NaT in self.staged.index)
    @rule()
    def finalize_write(self):
        self.staged.sort_index(inplace=True)
        self.lib.sort_and_finalize_staged_data(SYMBOL)
        arctic_data = self.lib.read(SYMBOL).data
        assert_equal(arctic_data, self.staged)
        self.reset_state()

    @precondition(lambda self: self.staged_segments_count == 0)
    @rule()
    def finalize_append_no_staged_segments(self):
        self.assert_cannot_finalize_without_staged_data(StagedDataFinalizeMethod.APPEND)
        self.reset_state()

    @precondition(lambda self: self.staged_segments_count > 0)
    @rule()
    def finalize_append(self):
        self.staged.sort_index(inplace=True)
        pre_append_storage, symbol_exists = self.data_from_storage()
        assert pre_append_storage.index.is_monotonic_increasing
        symbol_is_not_empty = len(pre_append_storage) > 0
        stage_is_not_empty = len(self.staged) > 0
        if symbol_exists and list(self.staged.columns) != list(pre_append_storage.columns):
            self.assert_append_throws_with_mismatched_columns()
        elif symbol_is_not_empty and stage_is_not_empty and pre_append_storage.index[-1] > self.staged.index[0]:
            self.assert_appended_data_does_not_overlap_with_storage()
        elif pd.NaT in self.staged.index:
            self.assert_nat_is_not_supported(StagedDataFinalizeMethod.APPEND)
        else:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=StagedDataFinalizeMethod.APPEND)
            arctic_data = self.lib.read(SYMBOL).data
            assert arctic_data.index.is_monotonic_increasing
            assert_equal(arctic_data.iloc[len(pre_append_storage):], self.staged)
        self.reset_state()

    def reset_state(self):
        self.staged = pd.DataFrame([])
        self.staged_segments_count = 0

    def assert_appended_data_does_not_overlap_with_storage(self):
        with pytest.raises(UnsortedDataException) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=StagedDataFinalizeMethod.APPEND)

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
        assert "E_UNSORTED_DATA" in str(exception_info.value)

    def data_from_storage(self):
        try:
            return self.lib.read(SYMBOL).data, True
        except NoSuchVersionException:
            return pd.DataFrame(), False

    def teardown(self):
        self.reset_state()
        self.lib.delete_staged_data(SYMBOL)
        self.lib.delete(SYMBOL)


def test_sort_and_finalize_staged_data(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    StagedWrite.lib = ac.create_library(lib_name)
    run_state_machine_as_test(StagedWrite)
