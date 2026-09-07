from typing import Union

from arcticdb.dependencies import _PYARROW_AVAILABLE, _POLARS_AVAILABLE, pyarrow as pa, polars as pl
from arcticdb.exceptions import ArcticUnsupportedDataTypeException

NORMALIZABLE_PYARROW_TYPES = (pa.Table, pa.RecordBatch, pa.ChunkedArray, pa.Array) if _PYARROW_AVAILABLE else tuple()
NORMALIZABLE_POLARS_TYPES = (pl.DataFrame, pl.Series) if _POLARS_AVAILABLE else tuple()


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
    Converts `pa.Table` outputted via `output_format=OutputFormat.PYARROW` to a `pd.DataFrame` so it would
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


def to_pyarrow_table(
    arrow_structure: Union[pa.Table, pa.RecordBatch, pa.ChunkedArray, pa.Array, pl.DataFrame, pl.Series],
) -> pa.Table:
    if isinstance(arrow_structure, NORMALIZABLE_POLARS_TYPES):
        if not _PYARROW_AVAILABLE:
            raise ModuleNotFoundError(
                "ArcticDB's pyarrow optional dependency is missing and is required for working with polars DataFrames."
            )
        if isinstance(arrow_structure, pl.Series):
            # For some reason to_arrow() on a pl.DataFrame maintains the chunking and so is zero-copy, but on a
            # pl.Series it copies into a pa.Array if the Series was chunked
            arrow_structure = arrow_structure.to_frame().to_arrow().column(0)
        else:  # pl.DataFrame
            arrow_structure = arrow_structure.to_arrow()
    if isinstance(arrow_structure, (pa.ChunkedArray, pa.Array)):
        arrow_structure = pa.Table.from_arrays([arrow_structure], names=["__array__"])
    elif isinstance(arrow_structure, pa.RecordBatch):
        arrow_structure = pa.Table.from_batches([arrow_structure])
    elif not isinstance(arrow_structure, pa.Table):
        # Should be unreachable due to checks in get_normalizer_for_type
        raise ArcticUnsupportedDataTypeException(f"Unsupported Arrow type: {type(arrow_structure)}")
    return arrow_structure
