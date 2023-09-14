# import necessary libraries
from IPython.display import display, Markdown
import matplotlib.pyplot as plt
from .library_utils import check_and_adapt_library, check_symbol_exists


def calculate_and_add_segment_occupancy_info(df):
    """
    Calculates the number of data points, rows, and columns for each segment in the DataFrame.
    """
    df["data_points"] = (df["end_row"] - df["start_row"]) * (df["end_col"] - df["start_col"])
    df["rows"] = df["end_row"] - df["start_row"]
    df["columns"] = df["end_col"] - df["start_col"]
    return df


def calculate_sum_segments_occupancy_info(df):
    """
    Calculates the total sum of data points, rows, and columns in the DataFrame.
    """
    total_data_points = df["data_points"].sum()
    total_rows = df["rows"].sum()
    total_columns = df["columns"].sum()
    return total_data_points, total_rows, total_columns


def calculate_segments_occupancy_maximums(df, total_segment_size, segment_row_size, column_group_size):
    """
    Calculates the maximum number of data points, rows, and columns that can be stored in the segments.
    """
    max_data_points = total_segment_size * df.shape[0]
    max_rows = segment_row_size * df.shape[0]
    max_columns = column_group_size * df.shape[0]
    return max_data_points, max_rows, max_columns


def calculate_fragmentation(
    total_data_points, total_rows, total_columns, max_data_points, max_rows, max_columns, dynamic_schema
):
    """
    Calculates the overall fragmentation level as well as row-wise and column-wise fragmentation for the data.
    """
    fragmentation = None if dynamic_schema else float(total_data_points) / max_data_points
    per_row_fragmentation = float(total_rows) / max_rows
    per_column_fragmentation = None if dynamic_schema else float(total_columns) / max_columns
    return fragmentation, per_row_fragmentation, per_column_fragmentation


def display_message(fragmentation, per_row_fragmentation, per_column_fragmentation):
    """
    Displays a markdown message based on the calculated fragmentation levels.
    """
    message = ""
    if fragmentation is not None:
        if fragmentation < 0.5:
            message += (
                f"\n**Warning!** Your overall fragmentation is about {fragmentation * 100:.1f}%. The data appears quite"
                " fragmented. "
            )
        elif fragmentation < 0.75:
            message += (
                f"\n**Attention:** Your overall fragmentation is about {fragmentation * 100:.1f}%. The data appears to"
                " be somewhat fragmented. "
            )
        else:
            message += (
                f"\n**Good news:** Your overall fragmentation is about {fragmentation * 100:.1f}%. No major"
                " fragmentation issues detected. "
            )
            display(Markdown(message))

    if per_row_fragmentation < 0.5:
        message += (
            f"\n- Your row utilization is only about {per_row_fragmentation * 100:.1f}%. This typically occurs when"
            " updates or appends add a small number of rows per data segment, compared to the total row capacity of"
            " the segment. You may consider updating or appending the data in larger chunks or reducing the number "
            "of rows defined by `segment_row_size` in the library options to optimize data storage and retrieval."
            "However, be careful with the latter option as it can have other implications."
        )

    if per_column_fragmentation is not None and per_column_fragmentation < 0.5:
        message += (
            f"\n- Your column utilization is only about {per_column_fragmentation * 100:.1f}%. This could occur when"
            " the number of columns in your data exceeds the column group size. For example, if your column group size"
            " is 50 and your data contains 60 columns, it will result in one segment of 50 columns and another"
            " inefficient segment of 10 columns. Unfortunately, this is less controllable as it depends on the nature"
            " of your data. If this situation is frequently encountered, consider evaluating the column group size"
            " setting in your library, but tread cautiously as this may have other implications and could affect other"
            " symbols in the same library."
        )

    display(Markdown(message))


def plot_data_points(df, total_segment_size):
    """
    Plots a histogram of the number of data points per segment.
    """
    description = (
        "\nThe plot below represents the distribution of the number of data points per data segment. Understanding this"
        " distribution can help optimize data storage and retrieval operations. The dotted red line represents the"
        " maximum number of data points per segment. Thus, the further we are from the dotted line, the more fragmented"
        " the data is."
    )
    display(Markdown(description))

    plt.figure(figsize=(16, 9))
    plt.hist(df["data_points"], bins=20, rwidth=100)
    plt.axvline(total_segment_size, color="r", linestyle="--")
    plt.xlabel("Number of Data Points per Segment")
    plt.ylabel("Frequency")
    plt.title("Histogram of Number of Data Points per Data Segment")
    plt.show()


def plot_per_row_fragmentation(df, segment_row_size):
    """
    Plots a histogram of the number rows per segment.
    """
    description = (
        "\nThe plot below represents the distribution of the number of rows per data segment. Understanding this"
        " distribution can help optimize data storage and retrieval operations. The dotted red line represents the"
        " maximum number of rows per segment. Thus, the further we are from the dotted line, the more fragmented the"
        " data is."
    )
    display(Markdown(description))

    plt.figure(figsize=(16, 9))
    plt.hist(df["rows"], bins=100, rwidth=10)
    plt.axvline(segment_row_size, color="r", linestyle="--")
    plt.xlabel("Number of rows per Segment")
    plt.ylabel("Frequency")
    plt.title("Histogram of Number of rows per Data Segment")
    plt.show()


def fragmentation_diagnosis(lib, symbol, as_of=None):
    """
    Analyzes and displays the fragmentation level for a specified symbol from a library.

    The function checks the provided library's type, reads the index of the symbol, and calculates
    various statistics such as the total segment size, fragmentation, row-wise and column-wise fragmentation.
    Based on these statistics, it generates a markdown report and a histogram plot showing the distribution
    of data points per segment.

    Parameters:
    -----------
    lib : object
        The library object in which the symbol is located.
    symbol : str
        The symbol for which the fragmentation analysis is to be performed.
    as_of : int, optional
        The version of the symbol for which the analysis is to be performed.
        If not specified, the latest version is used.

    Returns:
    --------
    None
    Outputs a markdown report and a histogram plot detailing the fragmentation analysis.

    Example:
    --------
    fragmentation_diagnosis(lib, 'my_symbol', 3)
    """

    lib, _ = check_and_adapt_library(lib)
    if lib is None:
        return

    if not check_symbol_exists(lib, symbol, as_of):
        return

    segment_row_size = (lambda x: x if x > 0 else 100000)(lib._lib_cfg.lib_desc.version.write_options.segment_row_size)
    column_group_size = (lambda x: x if x > 0 else 127)(lib._lib_cfg.lib_desc.version.write_options.column_group_size)
    dynamic_schema = lib._lib_cfg.lib_desc.version.write_options.dynamic_schema

    # Calculate the actual amount of data stored across all the segments
    df = lib.read_index(symbol, as_of=as_of)
    df_with_segment_info = calculate_and_add_segment_occupancy_info(df)
    sum_data_points, sum_rows, sum_columns = calculate_sum_segments_occupancy_info(df_with_segment_info)

    # Calculate the maximum amount of data that can be stored across all segments
    total_segment_size = segment_row_size * column_group_size
    max_data_points, max_rows, max_columns = calculate_segments_occupancy_maximums(
        df_with_segment_info, total_segment_size, segment_row_size, column_group_size
    )

    # Infer fragmentation
    fragmentation, per_row_fragmentation, per_column_fragmentation = calculate_fragmentation(
        sum_data_points, sum_rows, sum_columns, max_data_points, max_rows, max_columns, dynamic_schema
    )

    # Display Results
    display_message(fragmentation, per_row_fragmentation, per_column_fragmentation)
    if not dynamic_schema:
        plot_data_points(df, total_segment_size)
    plot_per_row_fragmentation(df, segment_row_size)
