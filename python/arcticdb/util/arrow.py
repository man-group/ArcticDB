from arcticdb.dependencies import pyarrow as pa


def stringify_dictionary_encoded_columns(table, string_type=None):
    """
    Converts all pyarrow.Table dictionary encoded columns to strings.

    ArcticDB currently returns string columns in dictionary encoded arrow arrays when using
    OutputFormat.EXPERIMENTAL_ARROW.

    Useful for testing when comparing to the source dataframe where we want regular large_string columns instead of
    categorical columns.
    """
    if string_type is None:
        string_type = pa.large_string()
    for i, name in enumerate(table.column_names):
        if pa.types.is_dictionary(table.column(i).type):
            table = table.set_column(i, name, table.column(name).cast(string_type))
    return table
