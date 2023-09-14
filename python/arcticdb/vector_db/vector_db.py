import warnings
import pandas as pd
import numpy as np

from collections import namedtuple
from typing import Union, Optional, Collection, List, Mapping

from arcticdb import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.version_store.library import Library, ArcticUnsupportedDataTypeException
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.version_store import NoSuchVersionException
from arcticdb_ext.storage import NoDataFoundException

VECTOR_DB_DISTINGUISHED_SUFFIX = "_vector_db"

VECTOR_VALUE_ERROR = """
Vectors uploaded as mappings must have the following form.
- The key should be an identifier that is a string.
- The value should be another mapping.
    - The keys of these mappings should be strings.
    - They should include a value under "vector".
    - The value under "vector" should be something that is turned into a one-dimensional ndarray of floats by the method
    np.array, e.g., a one-dimensional list of floats.
    - That array should have the right number of values (i.e. correspond to the number of dimensions of the other
    vectors in the namespace.)
Attempts to upsert mappings not in this form should cause errors.
"""

PythonTopKClause = namedtuple("TopKClause", ["vector", "k"])

SENSIBLE_SEGMENT_SIZE = 100000000


def _generate_dataframe_from_ndarray(
    vectors: np.ndarray,
    identifiers: Collection[str],
    expected_dimensions: Optional[int] = None,
    metadata: Optional[Mapping[str, Mapping[str, any]]] = None,
) -> pd.DataFrame:
    """
    Generates a normalised `pd.DataFrame` for writing to an Arctic symbol from an `np.ndarray`.

    Parameters
    ----------
    vectors
        The array from which the dataframe is generated.
    identifiers
        A collection of strings that serve as identifiers of the vectors. These will be used as column headers.
    expected_dimensions
        An optional int parameter; if specified, the array will be checked for the correct dimensionality.
    metadata
        An optional dictionary of metadata to associate with the dataframe; presently ignored.

    Returns
    -------
    A `pd.DataFrame` suitable for writing to an Arctic symbol internally.

    Raises
    ------
    ArcticUnsupportedDataTypeException
        if the array given contains non-numeric types.
    ValueError
        if no identifiers are given, the wrong number of identifiers is given, a non-string identifier is given, or a
        vector of the wrong dimensionality is given.
    """
    if vectors.ndim != 2:
        raise ValueError(
            "Upsertion of vectors in an np.ndarray takes two-dimensional arrays; "
            f"the vector given had {vectors.ndim} instead."
        )
    if not np.issubdtype(vectors.dtype, float):
        if np.issubdtype(vectors.dtype, int):
            vectors = vectors.astype(np.float64)
        else:
            raise ArcticUnsupportedDataTypeException(
                "Vectors inserted must all exclusively contain floats. "
                f"You attempted to insert {vectors.dtype}, which doesn't count."
            )
    if identifiers is None:
        raise ValueError("Upsertion of vectors in an np.ndarray requires a list of identifiers.")
    if len(identifiers) != len(vectors):
        raise ValueError(f"You gave {len(identifiers)} identifiers but {len(vectors)} vectors.")
    if any([type(identifier) is not str for identifier in identifiers]):
        raise ValueError(f"All identifiers must be strings.")
    if expected_dimensions and expected_dimensions != vectors.shape[1]:
        raise ValueError(
            f"Expected vectors of {expected_dimensions} dimensions; got vectors of {vectors.shape[1]} dimensions."
        )
    return pd.DataFrame(vectors.T, columns=identifiers)


def _generate_dataframe_from_mapping(
    vectors: Mapping[str, Mapping[str, any]], expected_dimensions: Optional[int] = None
) -> pd.DataFrame:
    """
    Generates a normalised `pd.DataFrame` suitable for writing to an Arctic library from a mapping.

    Parameters
    ----------
    vectors
        The mapping should have the following form:

        ```
        {...
            identifier :
                { ...
                    "vector" : vector
                }
        }
        ```

        where the identifier is a string, and the result of calling `np.array(vector)` is a one-dimensional array of
        floats of the right length.

        The mapping may optionally include metadata in addition to the value of the vector. Those metadata are presently
        ignored.
    expected_dimensions
        An optional int parameter; if specified, the array will be checked for the correct dimensionality.

    Returns
    -------
    A `pd.DataFrame` suitable for writing to an Arctic symbol internally.

    Raises
    ------
    ArcticUnsupportedDataTypeException
        if any of the values under the key `vector` have non-numeric type.
    ValueError
        if a vector is included that when converted to an `np.ndarray` has a shape of more than one dimension, or if
        it is a one-dimensional `np.ndarray` of the wrong length given the expected length of each vector.
    """
    data_frame_to_upsert = pd.DataFrame()
    for k, v in vectors.items():
        if type(k) is not str:
            raise ArcticUnsupportedDataTypeException(VECTOR_VALUE_ERROR)
        if "vector" not in v.keys():
            raise ValueError()
        column = np.array(v["vector"])
        if column.ndim != 1:
            raise ValueError(VECTOR_VALUE_ERROR)
        if not np.issubdtype(column.dtype, float):
            if np.issubdtype(column.dtype, int):
                column = column.astype(np.float64)
            else:
                raise ArcticUnsupportedDataTypeException(
                    "Vectors inserted must exclusively contain floats. You attempted to insert a vector that as an "
                    f"ndarray has dtype {column.dtype} which doesn't count."
                )
        if not expected_dimensions:
            expected_dimensions = column.shape[0]
        elif column.shape[0] != expected_dimensions:
            raise ValueError("The vectors must all have the same number of dimensions.")
        data_frame_to_upsert[k] = column
    return data_frame_to_upsert


def _generate_dataframe_from_dataframe(
    vectors: pd.DataFrame, expected_dimensions: Optional[int] = None
) -> pd.DataFrame:
    """
    Generates a normalised `pd.DataFrame` suitable for writing to an Arctic library from a `pd.DataFrame`.

    vectors
        the dataframe.
    expected_dimensions
        An optional int parameter; if specified, the array will be checked for the correct dimensionality.

    Returns
    -------
    A `pd.DataFrame` suitable for writing to an Arctic symbol internally.

    Raises
    ------
    ArcticUnsupportedDataTypeException
        if any of the values in the frame has a non-numeric type.
    ValueError
        if the length of the vectors supplied is unexpected.
    """
    for col in vectors:
        if np.issubdtype(vectors[col], int):
            vectors[col] = vectors[col].astype(np.float64)
        elif not np.issubdtype(vectors[col].dtype, float):
            raise ArcticUnsupportedDataTypeException(
                "Vectors inserted must all exclusively contain floats. You attempted to "
                f"insert {vectors[col].dtype}, which does not count."
            )

    if expected_dimensions and vectors.shape[0] != expected_dimensions:
        raise ValueError(
            f"Expected vectors of {expected_dimensions} dimensions; got vectors of {vectors.shape[0]} dimensions."
        )
    else:
        return vectors


def _generate_dataframe(
    vectors: Union[Mapping[str, Mapping[str, any]], np.ndarray, pd.DataFrame],
    expected_dimensions: Optional[int] = None,
    identifiers: Optional[Collection[str]] = None,
    metadata: Optional[Mapping[str, Mapping]] = None,
) -> pd.DataFrame:
    """
    Generates a `pd.DataFrame` from input.

    Parameters
    ----------
    vectors
        A `Mapping`, `np.ndarray`, or `pd.DataFrame`. See `_generate_dataframe_from_ndarray` and
        `_generate_dataframe_from_mapping` for further details.
    expected_dimensions
        The number of dimensions expected from each vector; if specified, and if vectors do not match, a `ValueError`
        will be raised.
    identifiers
        In the case of an `np.ndarray`, we require a separate collection of identifiers of the vectors.
    metadata
        Presently ignored.

    Returns
    -------
    A `pd.DataFrame` suitable for writing to an Arctic sybmol.

    Raises
    ------
    ArcticUnsupportedDataTypeException
        if the input is not a `Mapping`, `np.ndarray`, or `pd.DataFrame`, or any vector given has non-numeric type.
    ValueError
        if a `Mapping` is provided and `metadata` are too; or if a `Mapping` or `pd.DataFrame` is provided and
        identifiers are too.
    """
    if isinstance(vectors, Mapping):
        if metadata:
            raise ValueError(
                "Vectors uploaded as mappings should not be accompanied by a separate metadata parameter. "
                "Include metadata in the mapping itself."
            )
        if identifiers:
            raise ValueError(
                "Vectors uploaded as mappings should not be accompanied by a separate parameter for "
                "identifiers. The keys of the mapping are used as identifiers instead."
            )
        df_to_upsert = _generate_dataframe_from_mapping(vectors, expected_dimensions)
    elif isinstance(vectors, np.ndarray):
        df_to_upsert = _generate_dataframe_from_ndarray(vectors, identifiers, expected_dimensions, metadata)
    elif isinstance(vectors, pd.DataFrame):
        if identifiers:
            raise ValueError(
                "Vectors uploaded as dataframes should not be accompanied by a separate parameter for "
                "identifiers. The column headings of the dataframe are used as identifiers instead."
            )
        df_to_upsert = _generate_dataframe_from_dataframe(vectors, expected_dimensions)
    else:
        raise ArcticUnsupportedDataTypeException(f"Upsertion of vectors of type {type(vectors)} is unsupported.")
    df_to_upsert.columns = df_to_upsert.columns.astype(str)
    return df_to_upsert


class VectorDB:
    """
    The main interface exposing vector database functionality in a given Arctic instance.

    VectorDBs contain namespaces which are the atomic unit of vector storage. Namespaces
    support upsertion and top-k queries.
    """

    def __init__(self, library: Library):
        """
        Parameters
        ----------
        library
            The library in which vectors will be stored. See `Arctic.create_library`. In initialising the library, we
            recommend using the `new_vector_db` method.
        """
        if type(library) is not Library:
            raise ArcticUnsupportedDataTypeException(
                f"Vector databases must be initialised on libraries. You tried to initialise from a {type(library)}."
            )
        self._max_vector_size = library._nvs._library.config.write_options.segment_row_size
        self._lib = library

    def __repr__(self):
        return f"VectorDB({str(self._lib)})"

    def __contains__(self, namespace: str):
        return f"{namespace}{VECTOR_DB_DISTINGUISHED_SUFFIX}" in self._lib

    def __getitem__(self, item: str):
        return _VectorSymbol(item, self)

    def _upsert_for_testing(
        self,
        namespace: str,
        vectors: pd.DataFrame
    ) -> None:
        """
        Utility method for testing that ignores all checks.
        """
        symbol_name = f"{namespace}{VECTOR_DB_DISTINGUISHED_SUFFIX}"
        self._lib.write(symbol_name, vectors, metadata={"dimensions": vectors.shape[0]})


    def upsert(
        self,
        namespace: str,
        vectors: Union[Mapping[str, Mapping[str, any]], pd.DataFrame, np.ndarray],
        identifiers: Optional[Collection[str]] = None,
        metadata: Optional[Mapping[str, Mapping]] = None,
    ) -> None:
        """
        Parameters
        ----------
        namespace
            The namespace to which `vectors` should be upserted.
        vectors
            In the case of a mapping, we expect a mapping whose keys are strings
            (taken as identifiers of vectors), and whose values are in turn mapping
            minimally containing a key-value pair 'value' pointing to something that
            yields a one-dimensional `np.ndarray` of numeric types corresponding to a
            vector. That could, for example, be a one-dimensional list of floats.
            Each vector must have the same dimensionality.

            In the case of a pandas DataFrame, we expect string columns and the entries to
            all be of a numeric type.

            In the case of an ndarray, we expect a list of `identifiers` corresponding to
            the number of vectors. We also expect an `np.ndarray` of `np.ndarray`s
            (i.e. two dimensions) populated by numeric types. Note that upserting an
            `np.ndarray` corresponds to upserting the transpose of the equivalent
            `pd.DataFrame`. For example:

            >>> vdb.upsert("vdb", np.array([[0,1],[2,3]]), identifiers=["a", "b"])

            inserts vectors <0,1> and <2,3>, but

            >>> vdb.upsert("vdb", pd.DataFrame(np.array([0,1],[2,3])))

            inserts vectors <0,2> and <1,3>.
            identifiers
        identifiers
            In the case of an `np.ndarray`, we require a separate collection of identifiers of the vectors.
        metadata
            Presently ignored.
        """
        symbol_name = f"{namespace}{VECTOR_DB_DISTINGUISHED_SUFFIX}"
        if metadata:
            warnings.warn("Metadata are presently ignored.")

        try:  # where symbol_name in self._lib
            expected_dimensions = self._lib.read_metadata(symbol_name).metadata["dimensions"]
        except NoDataFoundException:  # symbol_name not in self._lib
            expected_dimensions = None

        data_frame_to_upsert = _generate_dataframe(vectors, expected_dimensions, identifiers, metadata)
        actual_dimensions = data_frame_to_upsert.shape[0]
        if actual_dimensions > self._max_vector_size:
            raise ValueError(
                f"VectorDB {self} has maximum vector size {self._max_vector_size}, but you attempted to "
                f"upsert vectors of size {data_frame_to_upsert.shape[0]}. To store vectors with more than "
                f"{self._max_vector_size} vectors, you will have to initialise a new VectorDB on a "
                "fresh library like so:\n\n"
                ">>> arctic_client.create_library('nomen', LibraryOptions(rows_per_segment = k)\n\n"
                "where k is the maximum vector size."
            )
        if data_frame_to_upsert.isnull().values.any():
            raise ValueError("Vectors upserted must not contain NaNs.")
        maximum_component = ((0.5 * np.finfo(np.float64).max) ** 0.5) / actual_dimensions
        if (abs(data_frame_to_upsert).values > maximum_component).any():
            raise ValueError(
                "Vector component too large. The distance between vectors must not exceed the largest value storeable "
                f"in an np.float64, {np.finfo(np.float64).max}.\n\n"
                "To ensure this, the absolute value of component must not exceed\n\n"
                "(1/d) * ((0.5 * np.finfo(np.float64).max) ** 0.5)\n\n"
                "where d is the number of dimensions of the vector namespace. This applies to both the query vector "
                "and the vectors stored. This bounds the vectors to a region in which the n-dimensional diagonal is "
                "(approximately) np.finfo(np.float64).max."
            )
        try:  # in case where symbol_name in self._lib
            old_df = self.read(namespace)
            warnings.warn("This is extremely memory-inefficient!")
            updated_df = old_df.combine_first(data_frame_to_upsert)
            self._lib.write(symbol_name, updated_df, metadata={"dimensions": data_frame_to_upsert.shape[0]})
        except NoSuchVersionException:  # symbol_name not in self._lib
            self._lib.write(symbol_name, data_frame_to_upsert, metadata={"dimensions": data_frame_to_upsert.shape[0]})

    def top_k(
        self,
        namespace: str,
        query_vector: Collection[float],
        k: int,
        norm: Optional[str] = None,
        index: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Returns the top `k` most similar vectors to `query_vector` in `namespace`. We use the vector ID as a tiebreaker.

        Parameters
        ----------
        namespace
            The namespace in which to perform the top-k search.
        k
            The number of vectors to return from the search.
        query_vector
            The vector from which distances are taken.
        norm
            The distance norm based on which the top k will be computed. This is presently ignored; the default
            is Euclidean.
        index
            The index based on which to compute the top k. This is also ignored.
        """
        symbol_name = f"{namespace}{VECTOR_DB_DISTINGUISHED_SUFFIX}"
        if k < 1:
            raise ValueError("top-k makes sense only for integer k>0.")
        if norm:
            warnings.warn("Norms are presently ignored; we just use the Euclidean.")
        if index:
            warnings.warn("Indexing is presently unsupported.")
        qv = np.array(query_vector)
        if qv.ndim != 1:
            raise ValueError("Query vectors must be one-dimensional.")
        if np.isnan(np.dot(qv, qv)):  # apparently fast way of looking for nans.
            raise ValueError("Query vectors may not contain NaNs.")

        expected_dimensions = self._lib.read_metadata(symbol_name).metadata["dimensions"]
        maximum_component = ((0.5 * np.finfo(np.float64).max) ** 0.5) / expected_dimensions
        if (np.abs(qv) > maximum_component).any():
            raise ValueError(
                "Vector component too large. The distance between vectors must not exceed the largest value storeable "
                f"in an np.float64, {np.finfo(np.float64).max}.\n\n"
                "To ensure this, the absolute value of component must not exceed\n\n"
                "(1/d) * ((0.5 * np.finfo(np.float64).max) ** 0.5)\n\n"
                "where d is the number of dimensions of the vector namespace. This applies to both the query vector "
                "and the vectors stored. This bounds the vectors to a region in which the n-dimensional diagonal is "
                "(approximately) np.finfo(np.float64).max."
            )

        if qv.shape[0] != expected_dimensions:
            raise ValueError(
                "Query vectors must have the same number of components "
                f"({expected_dimensions}) "
                "as the vectors in the VectorDB. The vector given had "
                f"{qv.shape[0]} components."
            )
        q = QueryBuilder()
        q._top_k(qv, k)
        result = self._lib.read(symbol_name, query_builder=q).data
        result.index = list(result.index[:-1]) + ["similarity"]
        return result

    def read(self, namespace: str, identifiers: Optional[Collection[str]] = None) -> pd.DataFrame:
        """
        Reads a `namespace`.

        Parameters
        ----------
        namespace
            The namespace read.
        identifiers
            Presently ignored.
        """
        return self._lib.read(f"{namespace}{VECTOR_DB_DISTINGUISHED_SUFFIX}", identifiers).data

    def index(
        self,
        namespace: str,
        index: str, # presently ignored but can contain faiss 'index factory' string
        metric: str,
        centroids: int,
        training_set: pd.DataFrame,
        dimensions: int
    ) -> None:
        self._bucketise_namespace(namespace, metric, centroids, dimensions, training_set)
        if metric not in ["IP", "L2"]:
            raise ValueError("Only inner product ('IP') and L2 ('L2') supported metrics.")
        self._index_segments(namespace, index, metric, dimensions)

    def _bucketise_namespace(
            self,
            namespace: str,
            metric: str,
            centroids: int,
            dimensions: int,
            training_set: pd.DataFrame,
    ) -> pd.DataFrame:
        self._lib._nvs.version_store.train_vector_namespace_bucketiser(namespace, metric, centroids, list(training_set.to_numpy().flatten()), dimensions)
        self._lib._nvs.version_store.bucketise_vector_namespace(namespace, dimensions)

    def _index_segments(
            self,
            namespace: str,
            index: str,
            metric: str,
            dimensions: int
    ) -> pd.DataFrame:
        self._lib._nvs.version_store.index_segment_vectors(namespace, index, metric, dimensions)

    def search_vectors_with_bucketiser_and_index(
            self,
            namespace: str,
            query_vector: List[float],
            k: int,
            nprobes: int,
            dimensions: int
    ) -> None:
        return self._lib._nvs.version_store.search_vectors_with_bucketiser_and_index(namespace, query_vector, k, nprobes, dimensions)


class _VectorSymbol:
    def __init__(self, namespace: str, vector_db: VectorDB):
        """
        A fairly simple wrapper to allow use of subscripts to access namespaces.

        Parameters
        ----------
        namespace
            The VectorDB to be subscripted.
        vector_db
            The namespace (the subscript).
        """
        self.namespace = namespace
        self.vector_db = vector_db

    def upsert(
        self,
        vectors: Union[Mapping[str, Mapping[str, any]], pd.DataFrame, np.ndarray],
        identifiers: Optional[Collection[str]] = None,
        metadata: Optional[Mapping[str, Mapping]] = None,
    ) -> None:
        self.vector_db.upsert(self.namespace, vectors, identifiers, metadata)

    def top_k(
        self, k: int, query_vector: Collection[float], norm: Optional[str] = None, index: Optional[str] = None
    ) -> pd.DataFrame:
        return self.vector_db.top_k(self.namespace, k, query_vector, norm, index)

    def read(self, identifiers: Optional[List[str]] = None) -> pd.DataFrame:
        return self.vector_db.read(self.namespace, identifiers)

def new_vector_db(ac: Arctic, name: str, max_vector_size: int, segment_width: Optional[int] = None) -> VectorDB:
    """
    Returns a new VectorDB, after automatically creating an Arctic library in which to store the vectors. This is the
    recommended method to initialise a VectorDB with the correct library parameters.

    Explanation

    Data in namespaces are processed in 'segments', which correspond to a row and column slice. Each vector is stored in
    a column. For reasons of efficiency, we require that each vector should fit in one segment. The height of the
    segment is determined in advance by `LibraryOptions`. Therefore, the maximum vector size is given by the
    `rows_per_segment` parameter of `LibraryOptions` minus one. But we also store distances in the same column, so
    the `rows_per_segment` parameter must be one more than the maximum vector size.

    However, we also process vectors in batches given by segments, and it is more efficient to process
    reasonably large numbers of vectors in one go. Therefore, we want `columns_per_segment` to be fairly large.

    This would suggest that we should make segments as large (width- and height-wise) as possible. But for
    reasons of efficiency, the segment size should be limited to roughly 100 million entries. Within all these
    constraints, you should therefore work out the largest size of vector you envisage adding to the VectorDB.
    This determines the segment row size, and therefore the recommended segment column size.

    Parameters
    ----------
    ac
        An `ArcticClient` in which the `VectorDB` will be stored.
    name
        The name of the library which the `VectorDB` will use.
    max_vector_size
        The maximum size of vectors that can be inserted.
    segment_width
        An optional parameter to specify the segment width; otherwise computed automatically.

    Returns
    -------
    A VectorDB.
    """
    if max_vector_size < 1:
        raise ValueError(f"max_vector_size must be at least 1; you attempted to set it to {max_vector_size}.")
    column_width = segment_width if segment_width else int(SENSIBLE_SEGMENT_SIZE / max_vector_size)
    ac.create_library(name, LibraryOptions(rows_per_segment=max_vector_size, columns_per_segment=column_width))
    return VectorDB(ac[name])
