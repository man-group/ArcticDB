import string

from hypothesis import example, given, strategies as st
from hypothesis.extra.pandas import column, data_frames, range_indexes
from arcticdb_ext.version_store import NoSuchVersionException
from arcticdb.util.hypothesis import (
    numeric_type_strategies,
    string_strategy,
    use_of_function_scoped_fixtures_in_hypothesis_checked
)

try:
    from arcticdb.version_store import VersionedItem as PythonVersionedItem
except ImportError:
    # arcticdb squashes the packages
    from arcticdb._store import VersionedItem as PythonVersionedItem
from arcticdb.version_store.vector_db import VectorDB

import pytest
import pandas as pd
import numpy as np
from arcticdb_ext.tools import AZURE_SUPPORT
from arcticdb.util.test import assert_frame_equal
import random

if AZURE_SUPPORT:
    pass

try:
    from arcticdb.version_store.library import (
        WritePayload,
        ArcticUnsupportedDataTypeException,
        ReadRequest,
        ReadInfoRequest,
        ArcticInvalidApiUsageException,
        StagedDataFinalizeMethod,
    )
except ImportError:
    # arcticdb squashes the packages
    from arcticdb.library import (
        WritePayload,
        ArcticUnsupportedDataTypeException,
        ReadRequest,
        ReadInfoRequest,
        ArcticInvalidApiUsageException,
        StagedDataFinalizeMethod,
    )

def test_top_k(arctic_client):
    np.random.seed(0)
    random.seed(0)
    ac = arctic_client

    dimensions, number_of_vectors, k = 15, 20, 5
    string_column_names = [''.join(random.choices(
        string.ascii_uppercase + string.digits,
        k=10))
        for _ in range(number_of_vectors)]

    ac.create_library("pytest_test_top_k")
    vector_db = VectorDB(ac["pytest_test_top_k"])

    df = pd.DataFrame(
        np.random.rand(dimensions,number_of_vectors),
        columns=string_column_names
    )
    qv = np.array([0]*dimensions)
    distances = df.apply(lambda x: np.linalg.norm(x-qv))
    top_k_distances = distances[distances.argsort()[:k]]
    python_result = df[top_k_distances.index].append(
        pd.DataFrame(top_k_distances).T
    )
    python_result.index = list(range(dimensions)) + ["similarity"]

    vector_db.upsert(f"df{dimensions}", df)
    cpp_result = vector_db.top_k(f"df{dimensions}", k, qv)

    assert_frame_equal(python_result, cpp_result)

def test_validation(arctic_client):
    ac = arctic_client
    ac.create_library("pytest_test_validation")
    vector_db = VectorDB(ac["pytest_test_validation"])

    # Three-dimensional arrays can't be ingested..
    with pytest.raises(ValueError):
        vector_db["vdb"].upsert(np.array([[[0]]]), identifiers=["test"])

    # The number of vectors should be the same as the number of identifiers.
    with pytest.raises(ValueError):
        vector_db["vdb"].upsert(np.array([[0]]), identifiers=[])

    # The identifiers must be strings.
    with pytest.raises(ValueError):
        vector_db["vdb"].upsert(np.array([[0]]), identifiers=[0])

    # ndarrays must be supplied with identifiers.
    with pytest.raises(ValueError):
        vector_db["vdb"].upsert(np.array([[1]]))

    # Subsequent insertion of differently-dimensioned vectors
    # makes top-k meaningless and therefore is prohibited.
    with pytest.raises(ValueError):
        vector_db["differently_dimensioned_array_case"].upsert(np.array([[1]]), identifiers=["a"])
        vector_db["differently_dimensioned_array_case"].upsert(np.array([[1,2]]), identifiers=["b"])
    with pytest.raises(ValueError):
        vector_db["differently_dimensioned_df_case"].upsert(pd.DataFrame(np.random.rand(100,100)))
        vector_db["differently_dimensioned_df_case"].upsert(pd.DataFrame(np.random.rand(200,200)))
    with pytest.raises(ValueError):
        vector_db["vdb"].upsert({"1": {"vector": [0]}, "2": {"vector": [1, 2]}})

    # Vectors in mappings must be one-dimensional.
    with pytest.raises(ValueError):
        vector_db["vdb"].upsert({"1": {"vector": [[0]]}})

    # Keys to vectors in mappings must be strings.
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert({0: {"vector": [0]}})

    # top-k with k !> 0 doesn't make sense.
    with pytest.raises(ValueError):
        vector_db["empty"].top_k(0, [])

    # Non-numeric "vectors" (soi-disants) are unsupported.
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert(pd.DataFrame([""]))
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert({"vector": {"vector": [""]}})
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert(
            np.array([[""]]),
            identifiers=["test"]
        )

    # Upsertion takes mappings, DFs, and ndarrays.
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert(True)
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert([])


def test_inextant_namespace(arctic_client):
    ac = arctic_client
    ac.create_library("pytest_test_inextant_namespace")
    vector_db = VectorDB(ac["pytest_test_inextant_namespace"])
    with pytest.raises(NoSuchVersionException):
        vector_db.read("DOES_NOT_EXIST")
    with pytest.raises(NoSuchVersionException):
        vector_db["DOES_NOT_EXIST"].read()

def test_vector_db_requires_library(arctic_client):
    with pytest.raises(ArcticUnsupportedDataTypeException):
        VectorDB(3)

def test_vector_db_empty_upsertions(arctic_client):
    np.random.seed(0)
    random.seed(0)

    ac = arctic_client
    ac.create_library("pytest_test_vector_db_empty_upsertions")

    vector_db = VectorDB(ac["pytest_test_vector_db_empty_upsertions"])

    with pytest.raises(ValueError):
        vector_db.upsert("empty1", np.array([[]]))

    vector_db.upsert("empty2", {})
    assert_frame_equal(
        vector_db.read("empty2"),
        pd.DataFrame(),
        check_index_type=False
    )

    vector_db.upsert("empty3", pd.DataFrame())
    assert_frame_equal(
        vector_db.read("empty3"),
        pd.DataFrame(),
        check_index_type=False
    )

def test_vector_db_upsertion(arctic_client):
    np.random.seed(0)
    random.seed(0)

    ac = arctic_client
    ac.create_library("pytest_test_vector_db_upsertion")

    vector_db = VectorDB(ac["pytest_test_vector_db_upsertion"])

    dimensions, number_of_vectors, k = 15, 20, 5
    first_string_column_names = [''.join(random.choices(
        string.ascii_uppercase + string.digits,
        k=10))
        for _ in range(number_of_vectors)]
    second_string_column_names = [''.join(random.choices(
        string.ascii_uppercase + string.digits,
        k=15))
        for _ in range(number_of_vectors)]
    first_df = pd.DataFrame(
        np.random.rand(dimensions,number_of_vectors),
        columns=first_string_column_names
    )
    second_df = pd.DataFrame(
        np.random.rand(dimensions,number_of_vectors),
        columns=second_string_column_names
    )

    vector_db.upsert("first_is_first", first_df)
    vector_db.upsert("first_is_first", second_df)
    vector_db.upsert("second_is_first", second_df)
    vector_db.upsert("second_is_first", first_df)

    assert_frame_equal(
        vector_db.read("first_is_first"),
        vector_db.read("second_is_first"),
        check_like=True
    )

    vector_db.upsert("first_is_first", first_df)
    vector_db.upsert("first_is_first", first_df)
    vector_db.upsert("first_is_first", first_df)

    assert_frame_equal(
        vector_db.read("first_is_first"),
        vector_db.read("second_is_first"),
        check_like=True
    )
@use_of_function_scoped_fixtures_in_hypothesis_checked
@given(df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()))
def test_upsertion_idempotent(arctic_client, df):
    assert True