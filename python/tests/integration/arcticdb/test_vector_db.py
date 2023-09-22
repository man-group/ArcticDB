import string
import time

from hypothesis import HealthCheck, settings, given, strategies as st, reproduce_failure
from hypothesis.extra.pandas import column, data_frames, range_indexes
from hypothesis.extra.numpy import arrays
from arcticdb import Arctic
from arcticdb.options import LibraryOptions
from arcticdb_ext.version_store import NoSuchVersionException
from arcticdb.util.hypothesis import (
    numeric_type_strategies,
    string_strategy,
    use_of_function_scoped_fixtures_in_hypothesis_checked,
)
from arcticdb.encoding_version import EncodingVersion

try:
    from arcticdb.version_store import VersionedItem as PythonVersionedItem
except ImportError:
    # arcticdb squashes the packages
    from arcticdb._store import VersionedItem as PythonVersionedItem
from arcticdb.vector_db.vector_db import VectorDB, new_vector_db

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

def test_search():
    ac = Arctic("lmdb:///var/jpl")
    vector_db = VectorDB(ac["reference_library_1695381000.4680254"])
    num_vectors = 100000
    dimensions = 2000
    namespace = f"{num_vectors}-vectors-{dimensions}-d"
    centroids = 500
    training_points = centroids * 10
    begin = time.time()
    vector_db._bucketise_namespace(namespace, "L2", centroids, dimensions, pd.DataFrame(np.random.rand(dimensions, training_points)))
    bucketise = time.time()
    print(f"Bucketised {dimensions*num_vectors} floats in {bucketise - begin} seconds i.e. {dimensions*num_vectors/(bucketise - begin)} floats per second.")

    vector_db._index_segments(namespace, "test", "L2", dimensions)
    segment = time.time()
    print(f"Indexed in {segment - bucketise} seconds i.e. {dimensions*num_vectors/(bucketise-begin)}")

    result = vector_db.search_vectors_with_bucketiser_and_index(namespace, np.zeros(dimensions), 5, 1, dimensions)
    search = time.time()
    print(f"Searched in {search - segment} seconds i.e. {dimensions*num_vectors/(search - segment)}")
    pass

def test_test():
    ac = Arctic("lmdb:///scratch/data/jpl/vectors")
    vector_db = VectorDB(ac["reference_library"])
    namespace = "100000-vectors-5000-d"
    # these go from 100k to 10m vectors and 100D to 5000D (possibly more, can't remember)
    # above a million this breaks in a variety of exotic ways.
    dimensions = 100
    centroids = 250
    training_points = centroids * 50
    begin = time.time()
    vector_db._bucketise_namespace(namespace, "IP", centroids, dimensions, pd.DataFrame(np.random.rand(dimensions, training_points)))
    # bucketising alone isn't enough for indexing; we also need to call _index_segments
    # the index param is ignored but the metric isn't
    # VectorDB.index is what's exposed
    elapsed = time.time() - begin
    pass

def test_upsert():
    np.random.seed(0)
    ac = Arctic("lmdb:///var/jpl/")
    tempus = time.time()
    lib_name = f"reference_library_{tempus}"
    #
    max_dimensions = 2000

    ac.create_library(lib_name,
                      LibraryOptions(
                          encoding_version = EncodingVersion.V2,
                          rows_per_segment = max_dimensions,
                          columns_per_segment = 10
                      ))
    vector_db = VectorDB(ac[lib_name])

    for number_of_vectors in [1000]:
        for dimensions in [2000]:
            print(f"{number_of_vectors} vectors, {dimensions} dimensions.")
            string_column_names = list(range(number_of_vectors))
            df = pd.DataFrame(np.random.rand(dimensions, number_of_vectors), columns=string_column_names)
            last = time.time()
            vector_db._upsert_for_testing(f"{number_of_vectors}-vectors-{dimensions}-d", df)
            vector_db._bucketise_namespace(f"{number_of_vectors}-vectors-{dimensions}-d", "IP", 100, dimensions, pd.DataFrame(np.random.rand(dimensions, 10)))
            print(f"Upserted {dimensions*number_of_vectors} floats in {time.time() - last} seconds i.e. {dimensions*number_of_vectors/(time.time() - last)} fps.")

    print("Done")
def bucketise():
    lib_name = "reference_library"
    ac = Arctic("lmdb://test_with_indexing")
    vector_db = VectorDB(ac[lib_name])

    vector_db._bucketise_namespace()

def test_with_indexing():
    np.random.seed(0)
    random.seed(0)
    ac = Arctic("lmdb://test_with_indexing")

    dimensions, number_of_vectors, k = 1500, 2000, 5
    string_column_names = list(range(number_of_vectors)) # [
    #     "".join(random.choices(string.ascii_uppercase + string.digits, k=10)) for _ in range(number_of_vectors)
    # ]

    tempus = time.time()
    lib_name = f"pytest_test_top_k_simple_{tempus}_dimensions_{dimensions}_vectors_{number_of_vectors}"

    ac.create_library(lib_name,
                      LibraryOptions(
                          encoding_version = EncodingVersion.V2,
                          rows_per_segment = dimensions,
                          columns_per_segment = int(100000000/dimensions)
                      ))
    vector_db = VectorDB(ac[lib_name])

    df = pd.DataFrame(np.random.rand(dimensions, number_of_vectors))

    start = time.time()
    vector_db._upsert_for_testing(f"df{dimensions}", df)
    finished_upsert = time.time()
    time_upsert = finished_upsert - start
    print(f"Wrote {df.size/time_upsert} floats per second.")

    vector_db._bucketise_namespace(f"df{dimensions}", "IP", 100, dimensions, df)
    finished_bucketise = time.time()
    time_bucketise = finished_bucketise - finished_upsert
    print(f"Bucketised {df.size/time_bucketise} floats per second.")

    vector_db._index_segments(f"df{dimensions}", "test", "IP", dimensions)
    finished_index_segments = time.time()
    time_index_segments = finished_index_segments - finished_bucketise
    print(f"Indexed [within segments] {df.size/time_index_segments} floats per second.")

    result = vector_db.search_vectors_with_bucketiser_and_index(f"df{dimensions}", np.zeros(dimensions), 5, 10, dimensions)
    finished_search = time.time()
    time_search = finished_search - finished_index_segments
    print(f"Searched {df.size/time_search} floats per second.")
    print(result)

    pass

# Nearly everything below is for exact top-k.

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
    cpp_result = vector_db.top_k(f"df{dimensions}", qv, k)

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

                cpp_result = vector_db.top_k(f"df{dimensions}", qv, k)
                assert_frame_equal(python_result, cpp_result)
