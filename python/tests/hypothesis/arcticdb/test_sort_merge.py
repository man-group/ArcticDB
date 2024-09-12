import pandas as pd
import pytest
from hypothesis import assume, given
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
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked


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
def generate_dataframe(draw, column_list):
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

@st.composite
def generate_dataframes(draw, column_list):
    return draw(st.lists(generate_dataframe(COLUMN_DESCRIPTIONS)))

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
    with pytest.raises(SchemaException) as exception_info:
        lib.sort_and_finalize_staged_data(symbol, mode)
    assert "E_DESCRIPTOR_MISMATCH" in str(exception_info.value)

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

def segments_have_compatible_schema(segment_list):
    dtypes = {}
    for segment in segment_list:
        for col in segment:
            if col not in dtypes:
                dtypes[col] = segment.dtypes[col]
            elif not are_dtypes_compatible(dtypes[col], segment.dtypes[col]):
                return False
    return True

@use_of_function_scoped_fixtures_in_hypothesis_checked
@given(df_list=generate_dataframes(COLUMN_DESCRIPTIONS))
def test_sort_merge_static_schema_write(lmdb_library, df_list):
    lib = lmdb_library
    for df in df_list:
        lib.write("sym", df, staged=True, validate_index=False)
    if len(df_list) == 0:
        assert_cannot_finalize_without_staged_data(lib, "sym", StagedDataFinalizeMethod.WRITE)
        return
    if not all(df_list[0].dtypes.equals(segment.dtypes) for segment in df_list):
        assert_staged_columns_are_compatible(lib, "sym", StagedDataFinalizeMethod.WRITE)
        return
    if has_nat_in_index(df_list):
        assert_nat_is_not_supported(lib, "sym", StagedDataFinalizeMethod.WRITE)
        return
    expected = merge_and_sort_segment_list(df_list)
    lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.WRITE)
    data = lib.read("sym").data
    assert_equal(expected, data)

@use_of_function_scoped_fixtures_in_hypothesis_checked
@given(df_list=generate_dataframes(COLUMN_DESCRIPTIONS), initial_df=generate_dataframe(COLUMN_DESCRIPTIONS))
def test_sort_merge_static_schema_append(lmdb_library, df_list, initial_df):
    lib = lmdb_library
    assume(len(initial_df) > 0 and not has_nat_in_index([initial_df]))
    initial_df.sort_index(inplace=True)
    lib.write("sym", initial_df)
    for df in df_list:
        lib.write("sym", df, staged=True, validate_index=False)
    if len(df_list) == 0:
        assert_cannot_finalize_without_staged_data(lib, "sym", StagedDataFinalizeMethod.APPEND)
        return
    if not all(initial_df.dtypes.equals(segment.dtypes) for segment in df_list):
        assert_staged_columns_are_compatible(lib, "sym", StagedDataFinalizeMethod.APPEND)
        return
    if has_nat_in_index(df_list):
        assert_nat_is_not_supported(lib, "sym", StagedDataFinalizeMethod.APPEND)
        return
    merged_staging = merge_and_sort_segment_list(df_list)
    if initial_df.index[-1] > merged_staging.index[-1]:
        assert_appended_data_does_not_overlap_with_storage(lib, "sym")
        return
    expected = pd.concat([initial_df, merged_staging])
    lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
    data = lib.read("sym").data
    assert_equal(expected, data)

@use_of_function_scoped_fixtures_in_hypothesis_checked
@given(df_list=generate_dataframes(COLUMN_DESCRIPTIONS))
def test_sort_merge_dynamic_schema_write(lmdb_library_dynamic_schema, df_list):
    lib = lmdb_library_dynamic_schema
    for df in df_list:
        lib.write("sym", df, staged=True, validate_index=False)
    if len(df_list) == 0:
        assert_cannot_finalize_without_staged_data(lib, "sym", StagedDataFinalizeMethod.WRITE)
        return
    if not segments_have_compatible_schema(df_list):
        assert_staged_columns_are_compatible(lib, "sym", StagedDataFinalizeMethod.WRITE)
        return
    if has_nat_in_index(df_list):
        assert_nat_is_not_supported(lib, "sym", StagedDataFinalizeMethod.WRITE)
        return
    lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.WRITE)
    data = lib.read("sym").data
    int_columns_in_df = [col_name for col_name in data if is_integer_dtype(data.dtypes[col_name])]
    expected = merge_and_sort_segment_list(df_list, int_columns_in_df=int_columns_in_df)
    assert_equal(expected, data)