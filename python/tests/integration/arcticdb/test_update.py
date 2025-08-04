"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


from abc import ABC, abstractmethod
from enum import Enum
import random
import sys
from typing import Dict, Generator, List, Type, Union
from pandas import Timestamp
import pytest
import pandas as pd
import numpy as np

from arcticdb.options import LibraryOptions
from arcticdb.util._versions import IS_PANDAS_ONE
from arcticdb.util.test import dataframe_simulate_arcticdb_update_static
from arcticdb.util.utils import DFGenerator, TimestampNumber, get_logger, set_seed
from arcticdb.version_store._store import VersionedItem
from arcticdb.version_store.library import Library, UpdatePayload, WritePayload
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.version_store import DataError, NoSuchVersionException
from tests.util.mark import LINUX


logger = get_logger()

SEED = 1001


ROWS_PER_SEGMENT = 10
COLS_PER_SEGMENT  = 10
DEFAULT_FREQ = 's'


class UpdatePositionType(Enum):
    '''Enum specifying relative position of an update start or end time relative to
    original dataframe start and end time

    Example:
    If we have a dataframe with index [5, 10]. The following would be true:

        BEFORE -> [1, 3]
        RIGHT_BEFORE -> [2, 4]
        BEFORE_OVERLAP_START -> [2, 5]
        INSIDE_OVERLAP_BEGINNING -> [5, 7]
        TOTAL_OVERLAP -> [5, 10]
        INSIDE -> [6,8]
        etc .... ()
    
    '''
    BEFORE = 0  # End of the update is much before start of original DF
    RIGHT_BEFORE = 1  # End of the update is exactly before start of original DF
    BEFORE_OVERLAP_START = 2 # End of the update overlaps with the start of the original DF
    INSIDE_OVERLAP_BEGINNING = 3 # Start of update overlaps with the start of the original DF, no restrictions on end
    TOTAL_OVERLAP = 4 # Exact same start and end and number of rows as the original DF
    OVERSHADOW_ORIGINAL = 5 # Starts before start of original and ends after the original DF
    INSIDE = 6 # literally inside the original DF, update does not overlap neither start nor end
    INSIDE_OVERLAP_END = 7 # end of both update and original overlap, no restrictions for start
    AFTER_OVERLAP_END = 8 # end of original overlaps with start of of update
    RIGHT_AFTER = 9 # start of update is exactly next to original
    AFTER = 104 # start of the update is after at least 1 duration of end of original


class BasicDataFrameGenerator:
    """Generates the dataframe based on repetition of all arcticdb supported types
    The repetition assures that dataframes generated with certain number of columns will 
    always have exact same columns. A Dataframe which is slightly bigger then will have same 
    starting columns as the dataframe that is shorter in width
    """

    def __init__(self):
        super().__init__()

    def get_dataframe(self, number_columns:int, number_rows: int, 
                      start_time: Union[Timestamp, TimestampNumber] = Timestamp("1993-10-11"), **kwargs) -> pd.DataFrame:
        return DFGenerator.generate_normal_dataframe(num_cols=number_columns, 
                                                num_rows=number_rows, start_time=start_time, 
                                                seed=None, freq=DEFAULT_FREQ)   
    

class UpgradeDataFrameTypesGenerator(BasicDataFrameGenerator):
    """Special generator that can be used to test type promotions during
    update operations. The dataframes generated are always one and the same 
    for a specified number of columns. But then via type mapping dictionary
    the next generation could be forced instead of int32 to generate int64
    for instance. 
    The generator produces only dataframes with columns which types can be upgraded
    with ArcticDB
    """

    def __init__(self, seed=None):
        super().__init__()
        self.no_upgrade_types()
        self.seed = seed

    def define_upgrade_types(self, mappings: Dict[Type, Type]):
        self._type_conversion_dict = mappings

    def no_upgrade_types(self):
        """Resets upgrades and generates original dataframe types"""
        self._type_conversion_dict: Dict[type, type]  = dict()

    def _resolve(self, base_type):
        return self._type_conversion_dict.get(base_type, base_type)

    def get_dataframe(self, number_columns:int, number_rows: int, 
                      start_time: Union[Timestamp, TimestampNumber] = Timestamp("2033-12-11"), **kwargs) -> pd.DataFrame:
        freq = TimestampNumber.DEFAULT_FREQ if isinstance(start_time, Timestamp) else start_time.get_type()

        upgradable_dtypes = [np.int8, np.int16, np.int32, np.uint16, np.uint32, np.float32]
        gen = DFGenerator(size=number_rows, seed=self.seed) 
        for i in range(number_columns):
                dtype = upgradable_dtypes[i % len(upgradable_dtypes)]
                desired_type = self._resolve(dtype)
                if np.issubdtype(desired_type, np.integer):
                    gen.add_int_col(f"col_{i}", desired_type)
                elif np.issubdtype(desired_type, np.floating):
                    gen.add_float_col(f"col_{i}", desired_type)
                else:
                    raise TypeError("Unsupported type {dtype}")
        if start_time is not None:
            if isinstance(start_time, TimestampNumber):
                start_time = start_time.to_timestamp()
            gen.add_timestamp_index("index", freq, start_time)
        return gen.generate_dataframe()    
    

def upgrade_dataframe_types(df: pd.DataFrame, upgrade_types_dict: Dict[type, type]):
    """
    Upgrades all columns of certain type of the specified dataframe 
    to required type, given in the specified dictionary
    """
    for col in df.columns:
        upgrade_to = upgrade_types_dict.get(df[col].dtype.type, None)
        if upgrade_to:
            df[col] = df[col].astype(upgrade_to)


class UpdatesGenerator:
    """
    The class is specialized on generating updates for dataframes, based on desired position of the update 
    according to the timeframe of the original dataframe. It can be before the original dataframe - having no elements
    intersect, just before - will share only first element of the original dataframe etc.
    """

    def __init__(self, gen: BasicDataFrameGenerator):
        """
        Initialize with instance to generator. The generator have to be able to generate dataframes with the 
        same shape. It should be able to generate also fewer columns or more columns than the original dataframe
        but both original and generated dataframe should have same names and types of columns as the smaller dataframe of both
        """
        self.generator: BasicDataFrameGenerator = gen

    def generate_sequence(self, original_dataframe: pd.DataFrame, number_cols:int,  number_rows:int) -> List[pd.DataFrame]:
        """
        Generates sequence of updates covering all updates type.
        """
        sequence: List[pd.DataFrame] = list()

        for position_type in UpdatePositionType:
            df = self.generate_update(original_dataframe, position_type, number_cols, number_rows)
            sequence.append(df)

        return sequence        

    def generate_update(self, original_dataframe: pd.DataFrame, position_type: UpdatePositionType, 
                        number_cols: int, number_rows: int):
        '''
        Generates an update that is based on desired location of the update over original 
        dataframe with specified number of columns and number of rows
        Generator passed should be the same used to generate the `original_dataframe`
        '''
        start = TimestampNumber.from_timestamp(original_dataframe.index[0], DEFAULT_FREQ)
        end = TimestampNumber.from_timestamp(original_dataframe.index[-1], DEFAULT_FREQ)
        rows = original_dataframe.shape[0]
        if position_type == UpdatePositionType.BEFORE:
            update_df = self.generator.get_dataframe(number_cols, number_rows, start.dec(number_rows+1))
        elif position_type == UpdatePositionType.RIGHT_BEFORE:
            update_df = self.generator.get_dataframe(number_cols, number_rows, start.dec(number_rows))
        elif position_type == UpdatePositionType.BEFORE_OVERLAP_START:
            update_df = self.generator.get_dataframe(number_cols, number_rows, start.dec(number_rows-1))
        elif position_type == UpdatePositionType.INSIDE_OVERLAP_BEGINNING:
            update_df = self.generator.get_dataframe(number_cols, number_rows, start)
        elif position_type == UpdatePositionType.INSIDE:
            # inside is completely inside the dataframe no overlaps with start and end.
            # If requested number of rows is more than original dataframe they will be reduced to fit
            to_update = number_rows if (number_rows + 2) <= rows else rows - 2
            to_update = max(to_update, 1) # if the dataframe is tiny we will generate a one line dataframe
            update_df = self.generator.get_dataframe(number_cols, to_update, start.inc(1))
        elif position_type == UpdatePositionType.TOTAL_OVERLAP:
            # In this case we generate total overlap
            update_df = self.generator.get_dataframe(number_cols, rows, start) 
        elif position_type == UpdatePositionType.OVERSHADOW_ORIGINAL:
            # The update df will be bigger than original at least with 2 rows
            # at start and at end
            to_update = number_rows if number_rows > rows + 2 else rows + 2
            update_df = self.generator.get_dataframe(number_cols, to_update, start.dec(1)) 
        elif position_type == UpdatePositionType.INSIDE_OVERLAP_END:
            update_df = self.generator.get_dataframe(number_cols, number_rows, end.dec(number_rows-1))
        elif position_type == UpdatePositionType.AFTER_OVERLAP_END:
            update_df = self.generator.get_dataframe(number_cols, number_rows, end)
        elif position_type == UpdatePositionType.RIGHT_AFTER:
            update_df = self.generator.get_dataframe(number_cols, number_rows, end.inc(1))
        elif position_type == UpdatePositionType.AFTER:
            update_df = self.generator.get_dataframe(number_cols, number_rows, end.inc(2))
        else:
            raise ValueError(f"Invalid update position type: {position_type}")
        return update_df
    

def read_batch_as_dict(lib: Library, symbol_names: List[str]) -> Dict[str, Union[VersionedItem, DataError]]:
    read_results = lib.read_batch(symbol_names)
    return {result.symbol: result for result in read_results}


@pytest.fixture
def custom_library(arctic_client, lib_name, request) -> Generator[Library, None, None]:
    """
    Allows passing library creation parameters as parameters of the test or other fixture.
    Example:


        @pytest.mark.parametrize("custom_library", [
                    {'library_options': LibraryOptions(rows_per_segment=100, columns_per_segment=100)}
                ], indirect=True)
        def test_my_test(custom_library):
        .....
    """
    params = request.param if hasattr(request, "param") else {}
    yield arctic_client.create_library(name=lib_name, **params)
    try:
        arctic_client.delete_library(lib_name)
    except Exception:
        pass


@pytest.mark.storage
@pytest.mark.parametrize("custom_library", [
            {'library_options': LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT, 
                                               columns_per_segment=COLS_PER_SEGMENT )}
        ], indirect=True)
@pytest.mark.only_fixture_params(["lmdb", "real_s3", "real_gcp"])
def test_update_batch_all_supported_datatypes_over_several_segments(custom_library):
    '''
    Test assures that update batch works with all supported datatypes, 
    updates work over several segments of the library and executing several times
    same updates does not alter the result
    '''
    lib: Library = custom_library
    set_seed(SEED)
    start_time = TimestampNumber.from_timestamp(timestamp=Timestamp("10/10/2007"))
    g = BasicDataFrameGenerator()
    ug = UpdatesGenerator(g)
    
    # Update above 'rows_per_segment'
    sym1 = '_s_1'
    df1_num_cols = COLS_PER_SEGMENT *5
    df1_num_rows = ROWS_PER_SEGMENT*5
    df1 = g.get_dataframe(number_columns=df1_num_cols, number_rows=df1_num_rows, start_time=start_time)
    update1 = ug.generate_update(df1, UpdatePositionType.INSIDE, df1_num_cols, ROWS_PER_SEGMENT * 3)
    expected_updated_df1 = dataframe_simulate_arcticdb_update_static(df1, update1) 

    # Update exactly at 'rows_per_segment'
    sym2 = '_s_2'
    df2_num_cols = COLS_PER_SEGMENT -1
    df2_num_rows = ROWS_PER_SEGMENT-1
    df2 = g.get_dataframe(number_columns=df2_num_cols, number_rows=df2_num_rows, start_time=start_time)
    update2 = ug.generate_update(df2, UpdatePositionType.AFTER, df2_num_cols, ROWS_PER_SEGMENT)
    metadata2 = {1, 2, 3, "something", UpdatesGenerator, ug}
    expected_updated_df2 = dataframe_simulate_arcticdb_update_static(df2, update2) 

    # Update below 'rows_per_segment'
    sym3 = '_s_3'
    df3_num_cols = 1
    df3_num_rows = ROWS_PER_SEGMENT*2 - 1
    df3 = g.get_dataframe(number_columns=df3_num_cols, number_rows=df3_num_rows, start_time=start_time)
    update3 = ug.generate_update(df3, UpdatePositionType.INSIDE_OVERLAP_END, df3_num_cols, ROWS_PER_SEGMENT - 2)
    metadata3 = df2.to_json()
    expected_updated_df3 = dataframe_simulate_arcticdb_update_static(df3, update3) 

    # Error update due to mismatch in columns
    sym4 = '_s_4'
    df4_num_cols = COLS_PER_SEGMENT *2
    df4_num_rows = ROWS_PER_SEGMENT*2
    df4 = g.get_dataframe(number_columns=df4_num_cols, number_rows=df4_num_rows, start_time=start_time)
    update4_err = ug.generate_update(df4, UpdatePositionType.INSIDE, df4_num_cols - 2, 5)

    lib.write_batch([WritePayload(sym1, df1), WritePayload(sym2, df2), WritePayload(sym3, df3), WritePayload(sym4, df4)])

    for repetition in range(3):
        update_result = lib.update_batch([
            UpdatePayload(sym1, update1),
            UpdatePayload(sym3, update3, metadata=metadata3),
            UpdatePayload(sym4, update4_err, metadata=metadata3),
            UpdatePayload(sym2, update2, metadata=metadata2)
        ])

        assert update_result[0].version == repetition + 1
        assert update_result[0].metadata == None
        assert update_result[1].version == repetition + 1
        assert update_result[1].metadata == metadata3
        assert update_result[3].version == repetition + 1
        assert update_result[3].metadata == metadata2
        assert_frame_equal(expected_updated_df1, lib.read(sym1).data)
        assert_frame_equal(expected_updated_df3, lib.read(sym3).data)
        assert_frame_equal(expected_updated_df2, lib.read(sym2).data)
        assert isinstance(update_result[2], DataError)


@pytest.mark.storage
@pytest.mark.parametrize("custom_library", [
            {'library_options': LibraryOptions(dynamic_schema=True)}
        ], indirect=True)
@pytest.mark.only_fixture_params(["lmdb", "real_s3", "real_gcp"])
def test_update_batch_types_upgrade(custom_library):
    """
    The goal of this test is to confirm that update_batch can successfully upgrade
    different types to a new type required by the update dataframe.
    Additional checks are done for prune_previous_version, upsert and date_range of
    the updates in step by step approach
    """
    lib: Library = custom_library
    symbol_prefix = "some heck of a symbol!.!"
    number_columns = 20
    number_rows = 100
    set_seed(SEED)
    
    upgrade_path_simple = { 
        np.int16: np.int32,
        np.int32: np.int64,
        np.uint16: np.uint32,
        np.uint32: np.uint64,
        np.float32: np.float64
    }
    
    upgrade_path_mix = { 
        np.int16: np.int64,
        np.int32: np.float64,
        np.uint16: np.int32,
        np.uint32: np.int64,
        np.float32: np.float64
    }

    upgrade_path_float = { 
        np.int16: np.float32,
        np.int32: np.float64,
        np.uint16: np.float32,
        np.uint32: np.float64,
        np.float32: np.float64
    }

    types_to_try = [upgrade_path_mix, upgrade_path_simple, upgrade_path_float]
    original_dataframes = dict()
    symbol_names = []
    update_batch:List[UpdatePayload] = []
    write_batch:List[UpdatePayload] = []
    expected_results = dict()

    logger.info("Prepare updates and calculate expected dataframes")
    for index, upgrade in enumerate(types_to_try): 
        g = UpgradeDataFrameTypesGenerator()
        df1 = g.get_dataframe(number_columns, number_rows)
        g.define_upgrade_types(upgrade)
        df2 = g.get_dataframe(number_columns, number_rows // 3)
        symbol_name = symbol_prefix + str(index)
        symbol_names.append(symbol_name)
        original_dataframes[symbol_name] = df1
        # Calculate expected dataframe
        upgrade_dataframe_types(df1, upgrade)
        expected_results[symbol_name] = dataframe_simulate_arcticdb_update_static(df1, df2)
        update_batch.append(UpdatePayload(symbol_name, df2))
        write_batch.append(WritePayload(symbol_name, df1))

    logger.info("Scenario 1: Try to update symbols that do not exist")
    update_result = lib.update_batch(update_batch)
    for result in update_result:
        assert isinstance(result, DataError)
        assert "E_NO_SUCH_VERSION" in str(result)

    logger.info("Scenario 2: Update with upsert should now create symbols")
    update_result = lib.update_batch(update_batch, upsert=True)
    read_data = read_batch_as_dict(lib, symbol_names)
    for index, result in enumerate(update_result):
        assert result.version == 0
        assert_frame_equal(update_batch[index].data, read_data[symbol_names[index]].data)

    logger.info("Scenario 3: Write original dataframes, then update symbols")
    lib.write_batch(write_batch)
    update_result = lib.update_batch(update_batch, upsert=True)
    read_data = read_batch_as_dict(lib, symbol_names)
    for index, result in enumerate(update_result):
        symbol = symbol_names[index]
        assert result.version == 2
        assert_frame_equal(expected_results[symbol], read_data[symbol].data)

    ''' uncomment once issue 9589648728 is resolved (see next xfail test)

    logger.info("Scenario 4: Write original dataframes, then update symbols, with date range outside of update boundaries")
    logger.info("Result will be original dataframe")
    lib.write_batch(write_batch)
    for update in update_batch:
        start_index = original_dataframes[update.symbol].index[0] - pd.Timedelta(days=1)
        previous_day = start_index - pd.Timedelta(days=1)
        update.date_range = (previous_day, start_index)
    update_result = lib.update_batch(update_batch, prune_previous_versions=True)
    read_data = read_batch_as_dict(lib, symbol_names)
    for index, result in enumerate(update_result):
        symbol = symbol_names[index]
        if not hasattr(result, "version"):
            # To catch rare problem on 3.8
            logger.error(f"Expected {result} with version attribute {repr(result)}\n {str(result)}")
        assert result.version == 4
        assert_frame_equal(original_dataframes[symbol], read_data[symbol].data)
        with pytest.raises(NoSuchVersionException) as ex_info:
            lib.read(symbol, as_of=3).data # Previous version is pruned
    '''            

    logger.info("Scenario 5: Write original dataframes, then update symbols, but with date range matching update dataframe")
    logger.info("Result expected will be calculated dataframe original + update")
    lib.write_batch(write_batch)
    for update in update_batch:
        update.date_range = (update.data.index[0], update.data.index[-1])
    update_result = lib.update_batch(update_batch, prune_previous_versions=True)
    read_data = read_batch_as_dict(lib, symbol_names)
    for index, result in enumerate(update_result):
        symbol = symbol_names[index]
        assert result.version == 4 # This will become 6 when uncommented above once bug is fixed
        assert_frame_equal(expected_results[symbol], read_data[symbol].data)



@pytest.mark.xfail(IS_PANDAS_ONE, reason = "update_batch return unexpected exception (9589648728)")
def test_update_batch_error_scenario1(arctic_library):   
    lib= arctic_library
    symbol = "experimental 342143"
    data = {
        "col_5": [-2.356538e+38, 2.220219e+38]
    }

    index = pd.to_datetime([
        "2033-12-11 00:00:00",
        "2033-12-11 00:00:01",
    ])
    df = pd.DataFrame(data, index=index)
    df_0col = df[0:0]
    lib.write_batch([WritePayload(symbol, df)])
    update = UpdatePayload(symbol, df_0col)
    update_result = lib.update_batch([update], prune_previous_versions=True)
    assert update_result[0].version == 0


@pytest.mark.xfail(IS_PANDAS_ONE, reason = "update_batch return unexpected exception (9589648728)")
def test_update_batch_error_scenario2(arctic_library):   
    lib= arctic_library
    symbol = "experimental 342143"
    data = {
        "col_5": [-2.356538e+38, 2.220219e+38]
    }

    index = pd.to_datetime([
        "2033-12-11 00:00:00",
        "2033-12-11 00:00:01",
    ])
    df = pd.DataFrame(data, index=index)
    lib.write_batch([WritePayload(symbol, df)])
    update = UpdatePayload(symbol, df[0:1], date_range=(pd.Timestamp("2030-12-11 00:00:00"), pd.Timestamp("2030-12-11 00:00:01")))
    update_result = lib.update_batch([update], prune_previous_versions=True)
    assert update_result[0].version == 0


def dataframe_simulate_arcticdb_update_dynamic(expected_df: pd.DataFrame, update_df: pd.DataFrame) -> pd.DataFrame:
    """ Simulates partially dynamic update, when update df has new columns.
    The scenario where there are type promotions of existing columns is not covered yet
    """
    def add_missing_ending_columns(to_df: pd.DataFrame, from_df: pd.DataFrame):
        """
        Modifies `to_df` adding all missing columns at from `from_df` at the end.
        Note that it checks first that first common column of both are same
        """
        assert len(from_df.columns) >= len(to_df.columns)
        for index, col in enumerate(from_df.columns):
            if index < len(to_df.columns):
                message = f"Column [{index}] does not match from_df{from_df.columns[index]} != to_df{from_df.columns[index]}"
                assert from_df.columns[index] == to_df.columns[index], message
            else:
                dtype = from_df[col].dtype
                if np.issubdtype(dtype, np.integer):
                    to_df[col] = np.zeros(len(to_df), dtype=dtype)
                elif np.issubdtype(dtype, np.floating):
                    to_df[col] = np.full(len(to_df), np.nan, dtype=dtype)
                elif np.issubdtype(dtype, np.bool_):
                    to_df[col] = np.full(len(to_df), False, dtype=dtype)
                elif dtype == object or pd.api.types.is_string_dtype(dtype):
                    to_df[col] = [None] * len(to_df)
    add_missing_ending_columns(expected_df, update_df)
    return dataframe_simulate_arcticdb_update_static(expected_df, update_df)


@pytest.mark.storage
@pytest.mark.parametrize("custom_library", [
            {'library_options': LibraryOptions(dynamic_schema=True)}
        ], indirect=True)
@pytest.mark.only_fixture_params(["lmdb", "real_s3", "real_gcp"])
def test_update_batch_different_updates_dynamic_schema(custom_library):
    """ The test examines different types of updates depending on their
    UpdatePositionType over original dataframe. All updates have additional
    columns requiring use of dynamic schema. The updates are having also different 
    sizes compared to original dataframe. Only batch operations are used here
    """
    lib: Library = custom_library
    set_seed(SEED)
    start_time = TimestampNumber.from_timestamp(timestamp=Timestamp("1/2/2017"))
    g = BasicDataFrameGenerator()
    ug = UpdatesGenerator(g)
    original_num_cols = 30
    original_number_rows = 50
    symbol_prefix = "different types of updates"

    symbol_names = []
    update_batch:List[UpdatePayload] = []
    write_batch:List[UpdatePayload] = []
    expected_results = dict()
    dataframes_lenghts = [1, original_number_rows // 2, original_number_rows, original_number_rows * 1.2]

    for iter, number_rows in enumerate(dataframes_lenghts):
        original_dataframe = g.get_dataframe(original_num_cols, original_number_rows, start_time)
        updates_sequence = ug.generate_sequence(original_dataframe, original_num_cols * 4, number_rows)

        logger.info(f"Prepare updates (rows count: {number_rows}) and calculate expected dataframes")
        for index, update in enumerate(updates_sequence): 
            symbol_name = symbol_prefix + f"_({iter}_{index})"
            symbol_names.append(symbol_name)
            # Calculate expected dataframe
            expected_df = original_dataframe.copy(deep=True)
            expected_results[symbol_name] = dataframe_simulate_arcticdb_update_dynamic(expected_df, update)
            update_batch.append(UpdatePayload(symbol_name, update))
            write_batch.append(WritePayload(symbol_name, original_dataframe))
    
    assert len(symbol_names) == len(set(symbol_names)), "There is duplicate symbol"

    logger.info(f"Prepare symbols and do {len(updates_sequence) * len(dataframes_lenghts)} batch updates.")
    lib.write_batch(write_batch)
    update_result = lib.update_batch(update_batch)

    logger.info(f"Read symbols")
    read_data = read_batch_as_dict(lib, symbol_names)

    logger.info(f"Verify expected results for updates with rows count: {number_rows}")
    for index, result in enumerate(update_result):
        assert result.version == 1 
        assert_frame_equal(expected_results[result.symbol], read_data[result.symbol].data)
          
