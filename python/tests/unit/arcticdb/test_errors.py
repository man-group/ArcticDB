import pytest

from arcticdb.util.errors import *
import arcticdb.exceptions as ae
from arcticdb.exceptions import *  # keep as wildcard so all_exception_types below includes everything
from arcticdb_ext.exceptions import _ArcticLegacyCompatibilityException
from tests.util.mark import SLOW_TESTS_MARK

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


def test_pickling_error(lmdb_version_store):
    lmdb_version_store.write("sym", [1, 2, 3])
    with pytest.raises(InternalException):
        lmdb_version_store.append("sym", [4, 5, 6])


def test_internal_error_is_compat_exception(lmdb_version_store):
    lmdb_version_store.write("sym", [1, 2, 3])
    try:
        lmdb_version_store.append("sym", [4, 5, 6])
    except _ArcticLegacyCompatibilityException:
        pass
    except Exception:
        pytest.fail("Should have raised a compat exception")


def test_snapshot_error(lmdb_version_store):
    lmdb_version_store.write("sym", [1, 2, 3])
    lmdb_version_store.snapshot("snap")
    with pytest.raises(InternalException) as e:
        lmdb_version_store.snapshot("snap")


def test_symbol_not_found_exception(lmdb_version_store):
    """Check we match the legacy behaviour where this is not an _ArcticLegacyCompatibilityException"""
    with pytest.raises(NoDataFoundException) as e:
        lmdb_version_store.read("non_existent_symbol")

    assert not issubclass(e.type, _ArcticLegacyCompatibilityException)
    assert issubclass(e.type, ArcticException)


def test_no_such_version_exception(lmdb_version_store):
    """Check we match the legacy behaviour where this is not an _ArcticLegacyCompatibilityException"""
    lmdb_version_store.write("sym", [1, 2, 3])
    with pytest.raises(NoSuchVersionException) as e:
        lmdb_version_store.restore_version("sym", as_of=999)
    assert not issubclass(e.type, _ArcticLegacyCompatibilityException)
    assert issubclass(e.type, NoDataFoundException)
    assert issubclass(e.type, ArcticException)


@SLOW_TESTS_MARK
def test_symbol_list_exception_and_printout(
    mock_s3_store_with_mock_storage_exception,
):  # moto is choosen just because it's easy to give storage error
    with pytest.raises(InternalException, match="E_S3_RETRYABLE Retry-able error"):
        mock_s3_store_with_mock_storage_exception.list_symbols()
