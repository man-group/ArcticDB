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
from pandas.api.types import is_numeric_dtype, is_integer_dtype, is_float_dtype


ColumnInfo = namedtuple('ColumnInfo', ['name', 'dtype'])

COLUMNS = [f"col_{i}" for i in range(0, 5)]
DTYPES = ["int16", "int64", "float", "object", "datetime64[ns]"]
COLUMN_DESCRIPTIONS = [ColumnInfo(name, dtype) for name in COLUMNS for dtype in DTYPES]

def are_dtypes_compatible(left, right):
    if left == right:
        return True
    if is_numeric_dtype(left) and is_numeric_dtype(right):
        return True
    return False

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

def assert_equal(left, right, dynamic=False):
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

def assert_cannot_finalize_without_staged_data(lib, symbol, mode):
    with pytest.raises(UserInputException) as exception_info:
        lib.sort_and_finalize_staged_data(symbol, mode=mode)
    assert "E_NO_STAGED_SEGMENTS" in str(exception_info.value)
  
def assert_nat_is_not_supported(lib, symbol, mode):
    with pytest.raises(UnsortedDataException) as exception_info:
        lib.sort_and_finalize_staged_data(symbol, mode=mode)
    assert "E_UNSORTED_DATA" in str(exception_info.value)

def assert_staged_columns_are_compatible(lib, symbol, mode):
    with pytest.raises(SchemaException):
        lib.sort_and_finalize_staged_data(symbol, mode)

def has_nat_in_index(segment_list):
    return any(pd.NaT in segment.index for segment in segment_list)

def merge_and_sort_segment_list(segment_list, int_columns_in_df=None):
    merged = pd.concat(segment_list)
    # pd.concat promotes dtypes. If there are missing values in an int typed column
    # it will become float column and the missing values will be NaN.
    int_columns_in_df = int_columns_in_df if int_columns_in_df else []
    for col in int_columns_in_df:
        merged[col] = merged[col].replace(np.NaN, 0)
    merged.sort_index(inplace=True)
    return merged

def assert_staged_write_is_successful(lib, symbol, expected_segments_list):
    lib.sort_and_finalize_staged_data(symbol)
    arctic_data = lib.read(symbol).data
    int_columns_in_df = [col_name for col_name in arctic_data if is_integer_dtype(arctic_data.dtypes[col_name])]
    expected = merge_and_sort_segment_list(expected_segments_list, int_columns_in_df=int_columns_in_df)
    assert_equal(arctic_data, expected)

def get_symbol(lib, symbol):
    # When https://github.com/man-group/ArcticDB/pull/1798 this should used the symbol list as it
    # will allow to add more preconditions and split the append case even more
    try:
        return lib.read(symbol).data, True
    except NoSuchVersionException:
        return pd.DataFrame(), False

def assert_appended_data_does_not_overlap_with_storage(lib, symbol):
    with pytest.raises(UnsortedDataException) as exception_info:
        lib.sort_and_finalize_staged_data(symbol, mode=StagedDataFinalizeMethod.APPEND)

class StagedWriteStaticSchema(RuleBasedStateMachine):
    
    SYMBOL = "sym_static"
    lib = None

    @initialize()
    def init(self):
        self.reset_state()

    @rule(df=df(COLUMN_DESCRIPTIONS))
    def stage(self, df):
        self.staged.append(df)
        self.lib.write(self.SYMBOL, df, staged=True)

    @precondition(lambda self: not self.has_staged_segments())
    @rule()
    def finalize_write_no_staged_segments(self):
        assert_cannot_finalize_without_staged_data(self.lib, self.SYMBOL, StagedDataFinalizeMethod.WRITE)
        self.reset_staged()

    @precondition(lambda self: self.has_staged_segments() and has_nat_in_index(self.staged) and self.staged_segments_have_same_schema())
    @rule()
    def finalize_write_with_nat_in_index(self):
        assert_nat_is_not_supported(self.lib, self.SYMBOL, StagedDataFinalizeMethod.WRITE)
        self.reset_staged()

    @precondition(lambda self: not self.staged_segments_have_same_schema() and self.has_staged_segments())
    @rule()
    def finalize_write_with_mismatched_staged_columns(self):
        assert_staged_columns_are_compatible(self.lib, self.SYMBOL, StagedDataFinalizeMethod.WRITE)
        self.reset_staged()

    @precondition(lambda self: self.has_staged_segments() and not has_nat_in_index(self.staged) and self.staged_segments_have_same_schema())
    @rule()
    def finalize_write(self):
        assert_staged_write_is_successful(self.lib, self.SYMBOL, self.staged)
        self.stored = self.staged
        self.reset_staged()

    @precondition(lambda self: not self.has_staged_segments())
    @rule()
    def finalize_append_no_staged_segments(self):
        assert_cannot_finalize_without_staged_data(self.lib, self.SYMBOL, StagedDataFinalizeMethod.APPEND)
        self.reset_staged()
    
    @precondition(lambda self: self.has_staged_segments() and not self.staged_segments_have_same_schema())
    @rule()
    def finalize_append_with_mismatched_staged_columns(self):
        with pytest.raises(SchemaException):
            self.lib.sort_and_finalize_staged_data(self.SYMBOL)
        self.reset_staged()

    @precondition(lambda self: self.has_staged_segments() and self.staged_segments_have_same_schema())
    @rule()
    def finalize_append(self):
        staged = merge_and_sort_segment_list(self.staged)
        pre_append_storage, symbol_exists = get_symbol(self.lib, self.SYMBOL)
        assert pre_append_storage.index.is_monotonic_increasing
        symbol_has_rows = len(pre_append_storage) > 0
        staging_has_rows = len(staged) > 0
        if symbol_exists and not staged.dtypes.equals(pre_append_storage.dtypes):
            self.assert_append_throws_with_mismatched_columns()
        elif symbol_has_rows and staging_has_rows and pre_append_storage.index[-1] > staged.index[0]:
            assert_appended_data_does_not_overlap_with_storage(self.lib, self.SYMBOL)
        elif pd.NaT in staged.index:
            assert_nat_is_not_supported(self.lib, self.SYMBOL, StagedDataFinalizeMethod.APPEND)
        else:
            self.lib.sort_and_finalize_staged_data(self.SYMBOL, mode=StagedDataFinalizeMethod.APPEND)
            arctic_data = self.lib.read(self.SYMBOL).data
            assert arctic_data.index.is_monotonic_increasing
            assert_equal(arctic_data.iloc[len(pre_append_storage):], staged)
            self.stored += self.staged
            assert_equal(arctic_data, merge_and_sort_segment_list(self.stored))
        self.reset_staged()

    def reset_staged(self):
        self.staged = []

    def reset_stored(self):
        self.stored = []

    def reset_state(self):
        self.reset_staged()
        self.reset_stored()

    def assert_append_throws_with_mismatched_columns(self):
        with pytest.raises(SchemaException) as exception_info:
            self.lib.sort_and_finalize_staged_data(self.SYMBOL, mode=StagedDataFinalizeMethod.APPEND)

    def staged_segments_have_same_schema(self):
        if len(self.staged) == 0:
            return True
        schema = self.staged[0].dtypes
        return all(schema.equals(segment.dtypes) for segment in self.staged)

    def has_staged_segments(self):
        return len(self.staged)

    def teardown(self):
        self.reset_state()
        self.lib.delete_staged_data(self.SYMBOL)
        self.lib.delete(self.SYMBOL)


class StagedWriteDynamicSchema(RuleBasedStateMachine):

    SYMBOL = "sym_dynamic"
    lib = None

    @initialize()
    def init(self):
        self.reset_state()

    @rule(df=df(COLUMN_DESCRIPTIONS))
    def stage(self, df):
        self.staged.append(df)
        self.lib.write(self.SYMBOL, df, staged=True)

    @precondition(lambda self: not self.has_staged_segments())
    @rule()
    def finalize_write_no_staged_segments(self):
        assert_cannot_finalize_without_staged_data(self.lib, self.SYMBOL, StagedDataFinalizeMethod.WRITE)
        self.reset_staged()

    @precondition(lambda self: self.has_staged_segments() and has_nat_in_index(self.staged) and self.segments_have_compatible_schema(self.staged))
    @rule()
    def finalize_write_with_nat_in_index(self):
        assert_nat_is_not_supported(self.lib, self.SYMBOL, StagedDataFinalizeMethod.WRITE)
        self.reset_staged()

    @precondition(lambda self: not self.segments_have_compatible_schema(self.staged) and self.has_staged_segments())
    @rule()
    def finalize_write_with_mismatched_staged_columns(self):
        assert_staged_columns_are_compatible(self.lib, self.SYMBOL, StagedDataFinalizeMethod.WRITE)
        self.reset_staged()

    @precondition(lambda self: self.has_staged_segments() and not has_nat_in_index(self.staged) and self.segments_have_compatible_schema(self.staged))
    @rule()
    def finalize_write(self):
        assert_staged_write_is_successful(self.lib, self.SYMBOL, self.staged)
        self.stored = self.staged
        self.reset_staged()

    @precondition(lambda self: not self.has_staged_segments())
    @rule()
    def finalize_append_no_staged_segments(self):
        assert_cannot_finalize_without_staged_data(self.lib, self.SYMBOL, StagedDataFinalizeMethod.APPEND)
        self.reset_staged()
    
    @precondition(lambda self: self.has_staged_segments() and not self.segments_have_compatible_schema(self.staged))
    @rule()
    def finalize_append_with_mismatched_staged_columns(self):
        assert_staged_columns_are_compatible(self.lib, self.SYMBOL, StagedDataFinalizeMethod.APPEND)
        self.reset_staged()

    @precondition(lambda self: self.has_staged_segments() and self.segments_have_compatible_schema(self.staged))
    @rule()
    def finalize_append(self):
        staged = merge_and_sort_segment_list(self.staged)
        pre_append_storage, symbol_exists = get_symbol(self.lib, self.SYMBOL)
        assert pre_append_storage.index.is_monotonic_increasing
        symbol_has_rows = len(pre_append_storage) > 0
        staging_has_rows = len(staged) > 0
        if symbol_exists and not self.segments_have_compatible_schema([pre_append_storage, staged]):
            assert_staged_columns_are_compatible(self.lib, self.SYMBOL, StagedDataFinalizeMethod.APPEND)
        elif symbol_has_rows and staging_has_rows and pre_append_storage.index[-1] > staged.index[0]:
            assert_appended_data_does_not_overlap_with_storage(self.lib, self.SYMBOL)
        elif pd.NaT in staged.index:
            assert_nat_is_not_supported(self.lib, self.SYMBOL, StagedDataFinalizeMethod.APPEND)
        else:
            self.lib.sort_and_finalize_staged_data(self.SYMBOL, mode=StagedDataFinalizeMethod.APPEND)
            arctic_data = self.lib.read(self.SYMBOL).data
            assert arctic_data.index.is_monotonic_increasing
            self.stored += self.staged
            int_columns_in_df = [col_name for col_name in arctic_data if is_integer_dtype(arctic_data.dtypes[col_name])]
            assert_equal(arctic_data, merge_and_sort_segment_list(self.stored, int_columns_in_df=int_columns_in_df))
        self.reset_staged()

    def reset_staged(self):
        self.staged = []

    def reset_stored(self):
        self.stored = []

    def reset_state(self):
        self.reset_staged()
        self.reset_stored()

    def has_staged_segments(self):
        return len(self.staged) > 0

    @classmethod
    def segments_have_compatible_schema(cls, segment_list):
        dtypes = {}
        for segment in segment_list:
            for col in segment:
                if col not in dtypes:
                    dtypes[col] = segment.dtypes[col]
                elif not are_dtypes_compatible(dtypes[col], segment.dtypes[col]):
                    return False
        return True

    def teardown(self):
        self.reset_state()
        self.lib.delete_staged_data(self.SYMBOL)
        self.lib.delete(self.SYMBOL)

def test_sort_and_finalize_staged_data_static_schema(lmdb_library):
    StagedWriteStaticSchema.lib = lmdb_library
    run_state_machine_as_test(StagedWriteStaticSchema)

def test_sort_and_finalize_staged_data_dynamic_schema(lmdb_library_dynamic_schema):
    StagedWriteDynamicSchema.lib = lmdb_library_dynamic_schema
    run_state_machine_as_test(StagedWriteDynamicSchema)