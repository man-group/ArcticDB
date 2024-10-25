import pandas as pd
import numpy as np
import datetime as dt

def create_data_frame(num_columns: int, start_index: int, end_index : int) -> pd.DataFrame:
    """
        Creates a data frame with (integer index) specified number of columns with integer index starting
        from specified and ending in specified position. The content of the dataframe is 
        integer random numbers ['start_index', 'end_index')
    """
    rows = end_index - start_index
    cols = ['COL_%d' % i for i in range(num_columns)]
    df = pd.DataFrame(np.random.randint(start_index, 
                                        end_index, 
                                        size=(rows, num_columns)), 
                                        columns=cols)
    df.index = np.arange(start_index, end_index, 1).tolist()
    return df

def create_dataframe_datetime_index(num_columns: int, start_hour: int, end_hour : int) -> pd.DataFrame:
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

def dataframe_update(update_to : pd.DataFrame, update_from: pd.DataFrame) -> pd.DataFrame:
    """
        Updates the rows of the 'update_to' data frame with rows having same index
        in 'update_from' data frame
    """
    df_updated = update_to.copy(deep=True)
    df_updated.update(update_from)
    return df_updated

def dataframe_update_full(update_to : pd.DataFrame, update_from: pd.DataFrame) -> pd.DataFrame:
    '''
        Updates the existing rows of an indexed DataFrame and then adds missing rows
        to it
    '''
    update_to = dataframe_update(update_to, update_from)

    df1 = update_to.copy(deep=True)
    df2 = update_from.copy(deep=True)

    # Reset index to merge on datetime
    df1_reset = df1.reset_index()
    df2_reset = df2.reset_index()
    columns = list(df1_reset.columns)
    # Merge DataFrames with indicator
    merged_df = df1_reset.merge(df2_reset, on=columns, how='right', indicator=True)
    # Filter rows that are only in df2
    missing_rows = merged_df[merged_df['_merge'] == 'right_only'].drop(columns=['_merge'])
    # Set the index back to datetime
    missing_rows = missing_rows.set_index('index')
    # Append missing rows to df1
    df1 = pd.concat([df1, missing_rows], ignore_index=False)

    df1.sort_index(inplace=True) # We need to sort it at the end

    return df1
