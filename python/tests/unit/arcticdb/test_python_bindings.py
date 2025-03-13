"""
pybind11_stubgen is run to pick up any pybind11 gaps and issues
"""
import arcticdb_ext
from pybind11_stubgen import arg_parser, CLIArgs, stub_parser_from_args, QualifiedName


def test_python_bindings():
    args = arg_parser().parse_args(['arcticdb_ext', '--exit-code'], CLIArgs())
    parser = stub_parser_from_args(args)
    parser.handle_module(QualifiedName.from_str(arcticdb_ext.__name__), arcticdb_ext)
    parser.finalize() # raises in finalize if any errors found
