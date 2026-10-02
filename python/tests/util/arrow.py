import copy
from typing import Union

import polars as pl
import pyarrow as pa

from arcticdb.options import ArrowOutputStringFormat, OutputFormat
from arcticdb.preconditions import check


def to_format(table: Union[pa.Table, pl.DataFrame], arrow_output_format: OutputFormat):
    if isinstance(table, pa.Table) and arrow_output_format == OutputFormat.POLARS:
        return pl.from_arrow(table)
    if isinstance(table, pl.DataFrame) and arrow_output_format == OutputFormat.PYARROW:
        return table.to_arrow()
    return table


def deep_copy(table: Union[pa.Table, pl.DataFrame]) -> Union[pa.Table, pl.DataFrame]:
    """Deep copy of an arrow or polars table"""
    if isinstance(table, pl.DataFrame):
        # copy.deepcopy on a polars DataFrame shares the underlying Arrow buffers, so round-trip through pyarrow.
        return pl.from_arrow(copy.deepcopy(table.to_arrow()))
    return copy.deepcopy(table)


def assert_arrow_equal(expected: Union[pa.Table, pl.DataFrame], received: Union[pa.Table, pl.DataFrame]):
    # If types differ, convert both to pl.DataFrame, as this gracefully handles the int32/uint32 difference in dict keys
    if isinstance(expected, pa.Table) and isinstance(received, pl.DataFrame):
        expected = pl.from_arrow(expected)
    if isinstance(expected, pl.DataFrame) and isinstance(received, pa.Table):
        received = pl.from_arrow(received)
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


def arrow_output_string_format_to_pa_type(arrow_output_string_format):
    check(
        arrow_output_string_format != ArrowOutputStringFormat.UNSPECIFIED,
        "Cannot convert unspecified string format to pyarrow type",
    )
    if arrow_output_string_format == ArrowOutputStringFormat.SMALL_STRING:
        return pa.string()
    elif arrow_output_string_format == ArrowOutputStringFormat.LARGE_STRING:
        return pa.large_string()
    elif arrow_output_string_format in [
        ArrowOutputStringFormat.CATEGORICAL,
        ArrowOutputStringFormat.DICTIONARY_ENCODED,
    ]:
        return pa.dictionary(pa.int32(), pa.large_string())


def create_1d_arrow_structure(input_type: str, data: pa.Array) -> Union[pa.Array, pa.ChunkedArray, pl.Series]:
    if input_type == "Array":
        input = data
    elif input_type == "ChunkedArray":
        input = pa.chunked_array([pa.array(data[: len(data) // 2]), pa.array(data[len(data) // 2 :])])
    elif input_type in ["UnnamedSeries", "NamedSeries"]:
        input = pl.Series(values=data)
        if input_type == "NamedSeries":
            input = input.rename("series_name")
    else:
        assert False, f"Unexpected input_type '{input_type}' in create_1d_arrow_structure"
    return input
