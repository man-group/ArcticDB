import string

from hypothesis import HealthCheck, settings, given, strategies as st, reproduce_failure
from hypothesis.extra.pandas import column, data_frames, range_indexes
from hypothesis.extra.numpy import arrays
from arcticdb.options import LibraryOptions
from arcticdb_ext.version_store import NoSuchVersionException
from arcticdb.util.hypothesis import (
    numeric_type_strategies,
    string_strategy,
    use_of_function_scoped_fixtures_in_hypothesis_checked,
)

try:
    from arcticdb.version_store import VersionedItem as PythonVersionedItem
except ImportError:
    # arcticdb squashes the packages
    from arcticdb._store import VersionedItem as PythonVersionedItem
from arcticdb.vector_db.vector_db import VectorDB, new_vector_db, _process_raw_top_k_result

import pytest
import pandas as pd
import numpy as np
from arcticdb_ext.tools import AZURE_SUPPORT
from arcticdb.util.test import assert_frame_equal, assert_series_equal
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


def test_top_k_simple(arctic_client):
    np.random.seed(0)
    random.seed(0)
    ac = arctic_client

    dimensions, number_of_vectors, k = 15000, 200, 5
    string_column_names = [
        "".join(random.choices(string.ascii_uppercase + string.digits, k=10)) for _ in range(number_of_vectors)
    ]

    ac.create_library("pytest_test_top_k_simple")
    vector_db = VectorDB(ac["pytest_test_top_k_simple"])

    df = pd.DataFrame(np.random.rand(dimensions, number_of_vectors), columns=string_column_names)
    qv = np.random.rand(dimensions)
    distances = df.apply(lambda x: np.linalg.norm(x - qv))

    combined = np.array([distances, np.array(distances.index)])
    indices = np.core.records.fromrecords(combined.T, names=["distance", "id"]).argsort(order=["distance", "id"])[:k]
    top_k_distances = distances[indices]

    python_result = df.iloc[:, indices].append(pd.DataFrame(top_k_distances).T)
    python_result.index = list(range(dimensions)) + ["similarity"]

    vector_db.upsert(f"df{dimensions}", df)
    cpp_result = vector_db._top_k(f"df{dimensions}", qv, k)

    assert_frame_equal(python_result, cpp_result)


def test_maximum_vector_size(arctic_client):
    ac = arctic_client
    ac.create_library("pytest_test_maximum_vector_size", LibraryOptions(rows_per_segment=2))
    vdb = VectorDB(ac["pytest_test_maximum_vector_size"])
    vdb.upsert("pytest_test_maximum_vector_size_namespace", pd.DataFrame(np.zeros((1, 1))))
    with pytest.raises(ValueError):
        vdb.upsert("pytest_test_maximum_vector_size_namespace", pd.DataFrame(np.zeros((2, 2))))


def test_new_vector_db(arctic_client):
    new_vector_db(arctic_client, "pytest_test_new_vector_db", 100)
    new_vector_db(arctic_client, "pytest_test_new_vector_db_two", 100, 100)
    with pytest.raises(ValueError):
        new_vector_db(arctic_client, "pytest_test_new_vector_db", 100)
    with pytest.raises(ValueError):
        new_vector_db(arctic_client, "pytest_test_new_vector_db", 0)


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
        vector_db["differently_dimensioned_array_case"].upsert(np.array([[1, 2]]), identifiers=["b"])
    with pytest.raises(ValueError):
        vector_db["differently_dimensioned_df_case"].upsert(pd.DataFrame(np.random.rand(100, 100)))
        vector_db["differently_dimensioned_df_case"].upsert(pd.DataFrame(np.random.rand(200, 200)))
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
        vector_db["empty"].top_k([], 0)

    # Non-numeric "vectors" (soi-disants) are unsupported.
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert(pd.DataFrame([""]))
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert({"vector": {"vector": [""]}})
    with pytest.raises(ArcticUnsupportedDataTypeException):
        vector_db["vdb"].upsert(np.array([[""]]), identifiers=["test"])

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
    assert_frame_equal(vector_db.read("empty2"), pd.DataFrame(), check_index_type=False)

    vector_db.upsert("empty3", pd.DataFrame())
    assert_frame_equal(vector_db.read("empty3"), pd.DataFrame(), check_index_type=False)


def test_vector_db_upsertion(arctic_client):
    np.random.seed(0)
    random.seed(0)

    ac = arctic_client
    ac.create_library("pytest_test_vector_db_upsertion")

    vector_db = VectorDB(ac["pytest_test_vector_db_upsertion"])

    dimensions, number_of_vectors, k = 15, 20, 5
    first_string_column_names = [
        "".join(random.choices(string.ascii_uppercase + string.digits, k=10)) for _ in range(number_of_vectors)
    ]
    second_string_column_names = [
        "".join(random.choices(string.ascii_uppercase + string.digits, k=15)) for _ in range(number_of_vectors)
    ]
    first_df = pd.DataFrame(np.random.rand(dimensions, number_of_vectors), columns=first_string_column_names)
    second_df = pd.DataFrame(np.random.rand(dimensions, number_of_vectors), columns=second_string_column_names)

    vector_db.upsert("first_is_first", first_df)
    vector_db.upsert("first_is_first", second_df)
    vector_db.upsert("second_is_first", second_df)
    vector_db.upsert("second_is_first", first_df)

    assert_frame_equal(vector_db.read("first_is_first"), vector_db.read("second_is_first"), check_like=True)

    vector_db.upsert("first_is_first", first_df)
    vector_db.upsert("first_is_first", first_df)
    vector_db.upsert("first_is_first", first_df)

    assert_frame_equal(vector_db.read("first_is_first"), vector_db.read("second_is_first"), check_like=True)


def test_output_formatting():
    raw_frame = pd.DataFrame({"A": [5, 6, 7], "B": [8, 9, 10]})
    raw_frame.index = [0, 1, "similarity"]
    correct_result_sans_vectors = [{"id": "A", "distance": 7}, {"id": "B", "distance": 10}]
    assert correct_result_sans_vectors == _process_raw_top_k_result(raw_frame, False)
    correct_result_with_vectors = [
        {"id": "A", "distance": 7, "vector": pd.Series([5, 6], name="A").sort_index()},
        {"id": "B", "distance": 10, "vector": pd.Series([8, 9], name="B").sort_index()},
    ]
    result_with_vectors = _process_raw_top_k_result(raw_frame, True)
    for correct, actual in zip(correct_result_with_vectors, result_with_vectors):
        assert correct["id"] == actual["id"]
        assert correct["distance"] == actual["distance"]
        assert_series_equal(correct["vector"], actual["vector"])


@st.composite
def dataframe_and_query_vector_and_k_and_repeats(draw):
    vectors = draw(st.integers(min_value=1, max_value=10))
    df = draw(
        data_frames(
            [column(i, dtype=np.float64) for i in range(vectors)],
            index=range_indexes(min_size=1, max_size=10),
        )
    )
    length = df.shape[0]
    query_vector = draw(arrays(np.float64, shape=(length)))
    k = draw(st.integers(min_value=1, max_value=vectors))
    repeats = draw(st.integers(min_value=0, max_value=10))
    return (df, query_vector, k, repeats)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(suppress_health_check=[HealthCheck.too_slow], deadline=None, print_blob=True, max_examples=100)
@given(dataframe_and_query_vector_and_k_and_repeats=dataframe_and_query_vector_and_k_and_repeats())
def test_upsertion_idempotent(arctic_client, dataframe_and_query_vector_and_k_and_repeats):
    ac = arctic_client
    df, query_vector, k, repeats = dataframe_and_query_vector_and_k_and_repeats
    if "pytest_test_upsertion_idempotent" in ac.list_libraries():
        ac.delete_library("pytest_test_upsertion_idempotent")

    vdb = new_vector_db(ac, "pytest_test_upsertion_idempotent", df.shape[0])

    if df.isnull().values.any():
        with pytest.raises(ValueError):
            vdb.upsert("df", df)
    else:
        maximum_component = ((0.5 * np.finfo(np.float64).max) ** 0.5) / df.shape[0]
        if (abs(df).values > maximum_component).any():
            with pytest.raises(ValueError):
                vdb.upsert("df", df)
        else:
            vdb.upsert("df", df)
            first = vdb.read("df")
            for _ in range(repeats):
                vdb.upsert("df", df)
            assert_frame_equal(first.sort_index(axis=1), vdb.read("df").sort_index(axis=1))


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(suppress_health_check=[HealthCheck.too_slow], deadline=None, print_blob=True, max_examples=100)
@given(dataframe_and_query_vector_and_k_and_repeats=dataframe_and_query_vector_and_k_and_repeats())
def test_top_k(arctic_client, dataframe_and_query_vector_and_k_and_repeats):
    ac = arctic_client
    df, qv, k, repeats = dataframe_and_query_vector_and_k_and_repeats
    dimensions = df.shape[0]

    if "pytest_test_top_k" in ac.list_libraries():
        ac.delete_library("pytest_test_top_k")

    ac.create_library("pytest_test_top_k")
    vector_db = VectorDB(ac["pytest_test_top_k"])

    if df.isnull().values.any():
        with pytest.raises(ValueError):
            vector_db.upsert(f"df{dimensions}", df)
    else:
        maximum_component = ((0.5 * np.finfo(np.float64).max) ** 0.5) / df.shape[0]
        if (abs(df).values > maximum_component).any():
            with pytest.raises(ValueError):
                vector_db.upsert(f"df{dimensions}", df)
        else:
            vector_db.upsert(f"df{dimensions}", df)
            if np.isnan(np.dot(qv, qv)):  # apparently fast way of looking for nans
                with pytest.raises(ValueError):
                    vector_db.top_k(f"df{dimensions}", qv, k)
            elif (abs(qv) > maximum_component).any():
                with pytest.raises(ValueError):
                    vector_db.top_k(f"df{dimensions}", qv, k)
            else:
                distances = df.apply(lambda x: np.linalg.norm(x - qv))
                indices = np.lexsort((np.array(distances.index).astype(str), distances))[:k]
                top_k_distances = distances[indices]
                python_result = df.iloc[:, indices].append(pd.DataFrame(top_k_distances).T)
                python_result.index = list(range(dimensions)) + ["similarity"]

                cpp_result = vector_db._top_k(f"df{dimensions}", qv, k)
                assert_frame_equal(python_result, cpp_result)
