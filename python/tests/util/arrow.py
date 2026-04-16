from typing import Union

import polars as pl
import pyarrow as pa

from arcticdb.options import ArrowOutputStringFormat, OutputFormat


def to_format(table: Union[pa.Table, pl.DataFrame], arrow_output_format: OutputFormat):
    if isinstance(table, pa.Table) and arrow_output_format == OutputFormat.POLARS:
        return pl.from_arrow(table)
    if isinstance(table, pl.DataFrame) and arrow_output_format == OutputFormat.PYARROW:
        return table.to_arrow()
    return table


def assert_arrow_equal(expected: Union[pa.Table, pl.DataFrame], received: Union[pa.Table, pl.DataFrame]):
    # Convert expected to be the same type as received
    if isinstance(expected, pa.Table) and isinstance(received, pl.DataFrame):
        expected = pl.from_arrow(expected)
    if isinstance(expected, pl.DataFrame) and isinstance(received, pa.Table):
        expected = expected.to_arrow()
    # Both pyarrow.Table and polars.DataFrame have `equals`
    assert expected.equals(received)


def undictionarify_table(table: Union[pa.Table, pl.DataFrame]) -> Union[pa.Table, pl.DataFrame]:
    if isinstance(table, pl.DataFrame):
        # No-op for pl.DataFrames because pl.DataFrame.equals allows categorical to equal non-categorical
        return table
    for i, name in enumerate(table.column_names):
        typ = table.column(i).type
        if pa.types.is_dictionary(typ):
            table = table.set_column(i, name, table.column(i).cast(typ.value_type))
    return table


def _to_large_string(fmt):
    if fmt == pa.string() or fmt == ArrowOutputStringFormat.SMALL_STRING:
        return pa.large_string()
    return fmt


def string_format_kwargs(arrow_output_format: OutputFormat, default=None, per_column=None):
    """Build arrow string format kwargs, replacing pa.string() with pa.large_string() for polars."""
    is_polars = arrow_output_format == OutputFormat.POLARS
    kwargs = {}
    if default is not None:
        kwargs["arrow_string_format_default"] = _to_large_string(default) if is_polars else default
    if per_column is not None:
        if is_polars:
            per_column = {col: _to_large_string(fmt) for col, fmt in per_column.items()}
        kwargs["arrow_string_format_per_column"] = per_column
    return kwargs
