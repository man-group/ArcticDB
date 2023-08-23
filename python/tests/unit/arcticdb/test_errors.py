import pytest

from arcticdb.util.errors import *
import arcticdb.exceptions as ae
from arcticdb.exceptions import *  # keep as wildcard so all_exception_types below includes everything
from arcticdb_ext.exceptions import _ArcticLegacyCompatibilityException


test_raise_params = [(NormalizationError.E_UPDATE_NOT_SUPPORTED, NormalizationException)]

specific_exception_types = [
    InternalException,
    NormalizationException,
    MissingDataException,
    SchemaException,
    StorageException,
    SortingException,
    UnsortedDataException,
    UserInputException,
]

all_exception_types = [globals()[e] for e in dir(ae) if e.endswith("Exception") or e.endswith("Error")]


@pytest.mark.parametrize("code,exception", test_raise_params)
def test_raise(code, exception):
    with pytest.raises(exception, match=f"E{code.value} a message$"):
        arcticdb_raise(code, lambda: "a message")


@pytest.mark.parametrize("exception_type", all_exception_types)
def test_base_exception(exception_type):
    assert issubclass(exception_type, ArcticException)


def test_base_exception_is_runtimeerror():
    with pytest.raises(RuntimeError):
        raise ArcticException("A bad thing happened")


def test_compat_exception():
    for e in specific_exception_types:
        assert issubclass(e, _ArcticLegacyCompatibilityException)


def test_pickling_error(basic_store):
    basic_store.write("sym", [1, 2, 3])
    with pytest.raises(InternalException):
        basic_store.append("sym", [4, 5, 6])


def test_internal_error_is_compat_exception(basic_store):
    basic_store.write("sym", [1, 2, 3])
    try:
        basic_store.append("sym", [4, 5, 6])
    except _ArcticLegacyCompatibilityException:
        pass
    except Exception:
        pytest.fail("Should have raised a compat exception")


def test_snapshot_error(basic_store):
    basic_store.write("sym", [1, 2, 3])
    basic_store.snapshot("snap")
    with pytest.raises(InternalException) as e:
        basic_store.snapshot("snap")


def test_symbol_not_found_exception(basic_store):
    """Check we match the legacy behaviour where this is not an _ArcticLegacyCompatibilityException"""
    with pytest.raises(NoDataFoundException) as e:
        basic_store.read("non_existent_symbol")

    assert not issubclass(e.type, _ArcticLegacyCompatibilityException)
    assert issubclass(e.type, ArcticException)


def test_no_such_version_exception(basic_store):
    """Check we match the legacy behaviour where this is not an _ArcticLegacyCompatibilityException"""
    basic_store.write("sym", [1, 2, 3])
    with pytest.raises(NoSuchVersionException) as e:
        basic_store.restore_version("sym", as_of=999)
    assert not issubclass(e.type, _ArcticLegacyCompatibilityException)
    assert issubclass(e.type, NoDataFoundException)
    assert issubclass(e.type, ArcticException)
