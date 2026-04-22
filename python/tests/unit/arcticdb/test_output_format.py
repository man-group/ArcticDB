import pytest

from arcticdb.options import OutputFormat, RuntimeOptions, output_format_to_internal
from arcticdb_ext.version_store import InternalOutputFormat


@pytest.mark.parametrize("value", [OutputFormat.PANDAS, OutputFormat.PYARROW, OutputFormat.POLARS])
def test_resolve_enum_passthrough(value):
    assert OutputFormat.resolve(value) is value


@pytest.mark.parametrize(
    "value, expected",
    [
        ("PANDAS", OutputFormat.PANDAS),
        ("pandas", OutputFormat.PANDAS),
        ("Pandas", OutputFormat.PANDAS),
        ("PYARROW", OutputFormat.PYARROW),
        ("pyarrow", OutputFormat.PYARROW),
        ("PyArrow", OutputFormat.PYARROW),
        ("POLARS", OutputFormat.POLARS),
        ("polars", OutputFormat.POLARS),
    ],
)
def test_resolve_case_insensitive_strings(value, expected):
    assert OutputFormat.resolve(value) == expected


def test_resolve_none_returns_none():
    assert OutputFormat.resolve(None) is None


def test_resolve_none_with_default():
    assert OutputFormat.resolve(None, default=OutputFormat.PANDAS) == OutputFormat.PANDAS


@pytest.mark.parametrize("value", ["INVALID", "", "arrow", "dataframe", 123])
def test_resolve_invalid_raises(value):
    with pytest.raises(ValueError, match="Unknown OutputFormat"):
        OutputFormat.resolve(value)


def test_output_format_to_internal_pandas():
    assert output_format_to_internal(OutputFormat.PANDAS) == InternalOutputFormat.PANDAS


def test_output_format_to_internal_pyarrow():
    assert output_format_to_internal(OutputFormat.PYARROW) == InternalOutputFormat.ARROW


def test_output_format_to_internal_string_input():
    assert output_format_to_internal("pyarrow") == InternalOutputFormat.ARROW


def test_runtime_options_default_output_format():
    opts = RuntimeOptions()
    assert opts.output_format == OutputFormat.PANDAS


def test_runtime_options_string_resolved_on_init():
    opts = RuntimeOptions(output_format="pyarrow")
    assert opts.output_format == OutputFormat.PYARROW


def test_runtime_options_set_output_format_resolves_string():
    opts = RuntimeOptions()
    opts.set_output_format("polars")
    assert opts.output_format == OutputFormat.POLARS
