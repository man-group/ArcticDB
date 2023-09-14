from IPython.display import display, Markdown
from .library_utils import check_and_adapt_library, check_symbol_exists, get_string_version
from datetime import datetime
import pandas as pd


def display_segments(df):
    """
    Displays data segments from a DataFrame in a table format.

    This function takes a DataFrame that contains data segment information, resets its index,
    selects specific columns for display, and then visually presents this information in a
    styled table.
    """
    # Reset index to convert 'start_index' to a regular column
    df_reset = df.reset_index()

    # Convert 'creation_ts' from epoch timestamp to human-readable UTC time
    df_reset["creation UTC time"] = df_reset["creation_ts"].apply(
        lambda x: datetime.utcfromtimestamp(x / 1e9).strftime("%Y-%m-%d %H:%M:%S")
    )

    # Select the columns to be displayed
    columns = [
        "version_id",
        "start_index",
        "end_index",
        "creation UTC time",
        "start_row",
        "end_row",
        "start_col",
        "end_col",
    ]
    df_display = df_reset[columns]

    # Display the DataFrame in a table format
    display(
        df_display.style.set_table_styles(
            [
                dict(selector="th", props=[("text-align", "center")]),
                dict(selector="td", props=[("text-align", "center")]),
            ]
        )
    )


def display_index_key_structure(lib, symbol, as_of=None):
    """
    Displays the data segments of a specified symbol from a library.

    This function checks if the provided library and symbol exist. If they do,
    it retrieves and displays the data segment information for the specified version
    of the symbol. If 'as_of' is not provided, it will display the data for the latest version.

    Parameters:
    -----------
    lib : object
        The library object in which the symbol is located.
    symbol : str
        The symbol for which the data segment information is to be displayed.
    as_of : int, optional
        The version of the symbol whose data segments are to be displayed.
        If not specified, data for the latest version is displayed.

    Returns:
    --------
    None
    Outputs a markdown report detailing the data segments of the specified symbol.

    Example:
    --------
    display_index_key_structure(lib, 'my_symbol', 3)
    """

    lib, _ = check_and_adapt_library(lib)
    if lib is None:
        return

    if not check_symbol_exists(lib, symbol, as_of):
        return

    string_version = get_string_version(as_of)
    description = f"""
    \n Data segment information for {string_version} from symbol ***{symbol}***:
    """
    display(Markdown(description))
    df = lib.read_index(symbol, as_of=as_of)
    display_segments(df)
