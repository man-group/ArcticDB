from arcticdb.util.hypothesis import column_strategy, dataframe_strategy, numeric_type_strategies, supported_numeric_dtypes
from arcticdb.version_store._store import NativeVersionStore
from hypothesis import event, given
import numpy as np
import pandas as pd
import datetime as dt

def create_data_frame(num_columns: int, start_index: int, end_index : int) -> pd.DataFrame:
    '''
        Creates a data frame with specified number of columns with integer index starting
        from specified and ending in specified position. The content of the dataframe is 
        integer random numbers ['start_index', 'end_index')
    '''
    rows = end_index - start_index
    cols = ['COL_%d' % i for i in range(num_columns)]
    df = pd.DataFrame(np.random.randint(start_index, 
                                        end_index, 
                                        size=(rows, num_columns)), 
                                        columns=cols)
    df.index = np.arange(start_index, end_index, 1).tolist()
    pd.date_range()
    return df

def create_data_frame_datetime(num_columns: int, start_hour: int, end_hour : int) -> pd.DataFrame:
    """
        Creates data frame with specified number of solumns 
        with datetime index sorted, starting from start_hour till
        end hour (you can use thousands of hours if needed). The data is random integer data
    """
    assert start_hour >= 0 , "start index must be positive"
    assert end_hour >= 0 , "end index must be positive"

    time_start = dt.datetime(2000, 1, 1, 0)
    rows = end_hour - start_hour
    cols = ['COL_%d' % i for i in range(num_columns)]
    df = pd.DataFrame(np.random.randint(start_hour, 
                                        end_hour, 
                                        size=(rows, num_columns)), 
                                        columns=cols)
    start_date = time_start + dt.timedelta(hours=start_hour)
    dr = pd.date_range(start_date, periods=rows, freq='h')
    df.index = dr
    return df

def test_bug(basic_store):
    lib = basic_store
    symbol1 = "sym1"
    df_1_1 = create_data_frame(1, 2, 3)
    df_1_2 = create_data_frame(1, 4, 5)
    df_1_combined = pd.concat([df_1_1, df_1_2])
    lib.write(symbol1, df_1_1)
    lib.append(symbol1, df_1_2)

    symbol2 = "sym2"
    df_2_0 = create_data_frame(1, -10, -9)
    df_2_1 = create_data_frame(1, -8, -7)
    lib.write(symbol2, df_2_0)
    lib.append(symbol2, df_2_1)

    snap1 = "snap1"
    snap1_vers = {symbol1 : 0, symbol2 : 1}
    #snap1_vers = {symbol1 : 2}
    lib.snapshot(snap1, versions=snap1_vers)

    lib.delete_version(symbol1, 0)
    lib.delete_version(symbol2, 1)

    lib.delete_snapshot(snap1)

    # confirm afer deletion of versions all is as expected
    # as well as deleting the snapshot wipes the versions effectivly
    assert sorted(lib.list_snapshots()) == [] 
    assert df_2_0.equals(lib.read(symbol2).data)
    assert df_1_combined.equals(lib.read(symbol1).data)

@given(
    df=dataframe_strategy(
        [column_strategy("a", supported_numeric_dtypes()), column_strategy("b", supported_numeric_dtypes())]
    )
)
def test_filter_numeric_binary_comparison(df):
    event(f"Data frame : \n{df}")
    pass


