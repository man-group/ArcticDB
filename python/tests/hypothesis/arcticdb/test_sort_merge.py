import pandas as pd
import pytest
from hypothesis import assume
import hypothesis.extra.pandas as hs_pd
import hypothesis.strategies as st
from hypothesis.stateful import RuleBasedStateMachine, rule, initialize, run_state_machine_as_test, precondition
from collections import namedtuple
from pandas.testing import assert_frame_equal
from arcticdb.version_store.library import StagedDataFinalizeMethod
from arcticdb.exceptions import UserInputException, StreamDescriptorMismatch, UnsortedDataException, NoSuchVersionException, SchemaException
import numpy as np
import string
from arcticdb.util._versions import IS_PANDAS_TWO
from pandas.api.types import is_integer_dtype


ColumnInfo = namedtuple('ColumnInfo', ['name', 'dtype'])

SYMBOL = "sym"
COLUMNS = [f"col_{i}" for i in range(0, 5)]
DTYPES = ["int16", "int64", "float", "object", "datetime64[ns]"]
COLUMN_DESCRIPTIONS = [ColumnInfo(name, dtype) for name in COLUMNS for dtype in DTYPES]

def string_column_strategy(name):
    return hs_pd.column(name=name, elements=st.text(alphabet=string.ascii_letters))

@st.composite
def df(draw, column_list):
    column_infos = draw(st.lists(st.sampled_from(column_list), unique_by=lambda x: x.name, min_size=1))
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

def fix_dtypes(df, int_columns_in_df):
    """
    pd.concat promotes dtypes. If there are missing values in the int typed column
    it will become float column and the missing values will be NaN.
    """
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

class StagedWriteStaticSchema(RuleBasedStateMachine):

    lib = None

    @initialize()
    def init(self):
        self.reset_state()

    @rule(df=df(COLUMN_DESCRIPTIONS))
    def stage(self, df):
        self.staged.append(df)
        self.lib.write(SYMBOL, df, staged=True)

    @precondition(lambda self: not self.has_staged_segments())
    @rule()
    def finalize_write_no_staged_segments(self):
        self.assert_cannot_finalize_without_staged_data(StagedDataFinalizeMethod.WRITE)
        self.reset_state()

    @precondition(lambda self: self.has_staged_segments() and self.has_nat_in_index() and self.staged_segments_have_same_schema())
    @rule()
    def finalize_write_with_nat_in_index(self):
        self.assert_nat_is_not_supported(StagedDataFinalizeMethod.WRITE)
        self.reset_state()

    @precondition(lambda self: not self.staged_segments_have_same_schema() and self.has_staged_segments())
    @rule()
    def finalize_write_with_mismatched_staged_columns(self):
        with pytest.raises(SchemaException):
            self.lib.sort_and_finalize_staged_data(SYMBOL)
        self.reset_state()

    @precondition(lambda self: self.has_staged_segments() and not self.has_nat_in_index() and self.staged_segments_have_same_schema())
    @rule()
    def finalize_write(self):
        staged = self.merge_staged()
        self.lib.sort_and_finalize_staged_data(SYMBOL)
        arctic_data = self.lib.read(SYMBOL).data
        assert_equal(arctic_data, staged)
        self.reset_state()

    @precondition(lambda self: not self.has_staged_segments())
    @rule()
    def finalize_append_no_staged_segments(self):
        self.assert_cannot_finalize_without_staged_data(StagedDataFinalizeMethod.APPEND)
        self.reset_state()
    
    @precondition(lambda self: self.has_staged_segments() and not self.staged_segments_have_same_schema())
    @rule()
    def finalize_append_with_mismatched_staged_columns(self):
        with pytest.raises(SchemaException):
            self.lib.sort_and_finalize_staged_data(SYMBOL)
        self.reset_state()

    @precondition(lambda self: self.has_staged_segments() and self.staged_segments_have_same_schema())
    @rule()
    def finalize_append(self):
        staged = self.merge_staged()
        pre_append_storage, symbol_exists = self.data_from_storage()
        assert pre_append_storage.index.is_monotonic_increasing
        symbol_has_rows = len(pre_append_storage) > 0
        staging_has_rows = len(staged) > 0
        if symbol_exists and not staged.dtypes.equals(pre_append_storage.dtypes):
            self.assert_append_throws_with_mismatched_columns()
        elif symbol_has_rows and staging_has_rows and pre_append_storage.index[-1] > staged.index[0]:
            self.assert_appended_data_does_not_overlap_with_storage()
        elif pd.NaT in staged.index:
            self.assert_nat_is_not_supported(StagedDataFinalizeMethod.APPEND)
        else:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=StagedDataFinalizeMethod.APPEND)
            arctic_data = self.lib.read(SYMBOL).data
            assert arctic_data.index.is_monotonic_increasing
            assert_equal(arctic_data.iloc[len(pre_append_storage):], staged)
        self.reset_state()

    def reset_state(self):
        self.staged = []

    def assert_appended_data_does_not_overlap_with_storage(self):
        with pytest.raises(UnsortedDataException) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=StagedDataFinalizeMethod.APPEND)

    def assert_cannot_finalize_without_staged_data(self, mode):
        with pytest.raises(UserInputException) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=mode)
        assert "E_NO_STAGED_SEGMENTS" in str(exception_info.value)

    def assert_append_throws_with_mismatched_columns(self):
        with pytest.raises(SchemaException) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=StagedDataFinalizeMethod.APPEND)

    def assert_nat_is_not_supported(self, mode):
        with pytest.raises(UnsortedDataException) as exception_info:
            self.lib.sort_and_finalize_staged_data(SYMBOL, mode=mode)
        assert "E_UNSORTED_DATA" in str(exception_info.value)

    def data_from_storage(self):
        try:
            return self.lib.read(SYMBOL).data, True
        except NoSuchVersionException:
            return pd.DataFrame(), False

    def staged_segments_have_same_schema(self):
        if len(self.staged) == 0:
            return True
        schema = self.staged[0].dtypes
        return all(schema.equals(segment.dtypes) for segment in self.staged)

    def has_nat_in_index(self):
        return any(pd.NaT in segment.index for segment in self.staged)

    def has_staged_segments(self):
        return len(self.staged)

    def merge_staged(self):
        int_columns_in_df = set()
        for segment in self.staged:
            int_columns_in_df |= set([col for col in segment.columns if is_integer_dtype(segment.dtypes[col])])
        merged = fix_dtypes(pd.concat(self.staged), int_columns_in_df)
        merged.sort_index(inplace=True)
        return merged

    def teardown(self):
        self.reset_state()
        self.lib.delete_staged_data(SYMBOL)
        self.lib.delete(SYMBOL)


def test_sort_and_finalize_staged_data_static_schema(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    StagedWriteStaticSchema.lib = ac.create_library(lib_name)
    run_state_machine_as_test(StagedWriteStaticSchema)
