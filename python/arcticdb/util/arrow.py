from arcticdb.dependencies import pyarrow as pa


def cast_string_columns(table, string_type=None):
    """
    Converts all pyarrow.Table string columns to a given specific string type.

    ArcticDB can returns string columns in different formats depending on the provided ArrowOutputStringFormat

    Useful for testing when comparing to the source dataframe where we want regular large_string columns instead of
    categorical columns.
    """
    if string_type is None:
        string_type = pa.large_string()
    for i, name in enumerate(table.column_names):
        typ = table.column(i).type
        if pa.types.is_dictionary(typ) or pa.types.is_string(typ) or pa.types.is_large_string(typ):
            table = table.set_column(i, name, table.column(i).cast(string_type))
    return table


def stringify_dictionary_encoded_columns(table, string_type=None):
    return cast_string_columns(table, string_type)


def convert_arrow_to_pandas_for_tests(table):
    """
    Converts `pa.Table` outputted via `output_format=OutputFormat.EXPERIMENTAL_ARROW` to a `pd.DataFrame` so it would
    be identical to the one outputted via `output_format=OutputFormat.PANDAS`. This requires the following changes:
    - Replaces dictionary encoded string columns with regular string columns.
    - Fills null values in int columns with zeros.
    - Fills null values in bool columns with False.
    """
    new_table = cast_string_columns(table)
    for i, name in enumerate(new_table.column_names):
        if pa.types.is_integer(new_table.column(i).type):
            new_col = new_table.column(i).fill_null(0)
            new_table = new_table.set_column(i, name, new_col)
        if pa.types.is_boolean(new_table.column(i).type):
            new_col = new_table.column(i).fill_null(False)
            new_table = new_table.set_column(i, name, new_col)
    return new_table.to_pandas()
