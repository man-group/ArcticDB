import warnings
import pandas as pd
import numpy as np

from collections import namedtuple
from pandas import RangeIndex
from typing import Union, Optional, Collection, List, Mapping

from arcticdb.supported_types import Timestamp, numeric_types
from arcticdb.version_store.library import Library, ArcticUnsupportedDataTypeException
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.version_store import TopKClause as _TopKClause

VECTOR_DB_DISTINGUISHED_PREFIX = "vector_db_"

VECTOR_VALUE_ERROR = """
Vectors uploaded as mappings must have the following form.
- The key should be an identifier that is a string.
- The value should be another mapping.
    - The keys of these mappings should be strings.
    - They should include a value under "vector".
    - The value under "vector" should be something that can be turned into a one-dimensional ndarray of floats.
    - That array should have the right number of values (i.e. correspond to the number of dimensions of the other
    vectors in the namespace.)
Attempts to upsert mappings not in this form should cause errors.
"""

PythonTopKClause = namedtuple("TopKClause", ["vector", "k"])


def _generate_dataframe_from_ndarray(
        vectors: np.ndarray,
        identifiers: Collection[str],
        expected_dimensions: Optional[int] = None,
        metadata: Optional[Mapping[str, Mapping[str, any]]] = None
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
        raise ValueError("Upsertion of vectors in an `np.ndarray` takes two-dimensional arrays; "
                         f"the vector given had {vectors.ndim} instead.")
    if vectors.dtype not in numeric_types:
        raise ArcticUnsupportedDataTypeException(
            "Vectors inserted must all have a numeric type. "
            f"You attempted to insert {vectors.dtype}, which doesn't count."
        )
    if identifiers is None:
        raise ValueError("Upsertion of vectors in an `np.ndarray` requires a list of identifiers.")
    if len(identifiers) != len(vectors):
        raise ValueError(f"You gave {len(identifiers)} identifiers but {len(vectors)} vectors.")
    if any([type(identifier) is not str for identifier in identifiers]):
        raise ValueError(f"All identifiers must be strings.")
    if expected_dimensions and expected_dimensions != vectors.shape[1]:
        raise ValueError(f"Expected vectors of {expected_dimensions} dimensions; got vectors of {vectors.shape[1]} "
                         "dimensions.")
    return pd.DataFrame(
        vectors.astype(np.float32).T,
        columns=identifiers
    )


def _generate_dataframe_from_mapping(
        vectors: Mapping[str, Mapping[str, any]],
        expected_dimensions: Optional[int] = None,
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
        if column.dtype not in numeric_types:
            raise ArcticUnsupportedDataTypeException(
                "Vectors inserted must all have a numeric type. You attempted to insert a vector that as an np.array "
                f"has dtype {column.dtype} which doesn't count."
            )
        if not expected_dimensions:
            expected_dimensions = column.shape[0]
        elif column.shape[0] != expected_dimensions:
            raise ValueError("The vectors must all have the same number of dimensions.")
        data_frame_to_upsert[k] = column.astype(np.float32)
    return data_frame_to_upsert

def _generate_dataframe_from_dataframe(
        vectors: pd.DataFrame,
        expected_dimensions: Optional[int] = None
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
    if any([t not in numeric_types for t in vectors.dtypes.unique()]):
        raise ArcticUnsupportedDataTypeException(
            "Vectors inserted must all have a numeric type. You attempted to "
            f"insert {vectors.dtypes.unique()}, at least one of which does "
            "count."
        )
    elif expected_dimensions and vectors.shape[0] != expected_dimensions:
        raise ValueError(f"Expected vectors of {expected_dimensions} "
                         f"dimensions; got vectors of {vectors.shape[0]} "
                         "dimensions.")
    else:
        return vectors.astype(np.float32)


def _generate_dataframe(
        vectors: Union[
            Mapping[str, Mapping[str, any]],
            np.ndarray, pd.DataFrame
        ],
        expected_dimensions: Optional[int] = None,
        identifiers: Optional[Collection[str]] = None,
        metadata: Optional[Mapping[str, Mapping]] = None
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
            raise ValueError("Vectors uploaded as mappings should not be accompanied by a separate metadata parameter. "
                             "Include metadata in the mapping itself.")
        if identifiers:
            raise ValueError("Vectors uploaded as mappings should not be accompanied by a separate parameter for "
                             "identifiers. The keys of the mapping are used as identifiers instead.")
        df_to_upsert = _generate_dataframe_from_mapping(
            vectors,
            expected_dimensions,
        )
    elif isinstance(vectors, np.ndarray):
        df_to_upsert = _generate_dataframe_from_ndarray(
            vectors,
            identifiers,
            expected_dimensions,
            metadata
        )
    elif isinstance(vectors, pd.DataFrame):
        if identifiers:
            raise ValueError("Vectors uploaded as dataframes should not be accompanied by a separate parameter for "
                             "identifiers. The column headings of the dataframe are used as identifiers instead.")
        df_to_upsert = _generate_dataframe_from_dataframe(
            vectors,
            expected_dimensions
        )
    else:
        raise ArcticUnsupportedDataTypeException(
            f"Upsertion of vectors of type {type(vectors)} is unsupported."
        )
    if isinstance(df_to_upsert.columns, RangeIndex):
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
            The library in which vectors will be stored. See `Arctic.create_library`.
        dimensions
            The number of dimensions those vectors should have.
        """
        if type(library) is not Library:
            raise ArcticUnsupportedDataTypeException(
                "Vector databases must be initialised on libraries. "
                f"You tried to initialise from a {type(library)}."
            )
        self._lib = library
        self._dimensions = dict()  # dimensionality of vectors in each symbol

    def __repr__(self):
        return f"VectorDB({str(self._lib)})"

    def __contains__(self, namespace: str):
        return f"{VECTOR_DB_DISTINGUISHED_PREFIX}{namespace}" in self._lib

    def __getitem__(self, item: str):
        return _VectorSymbol(item, self)

    def upsert(
            self,
            namespace: str,
            vectors: Union[Mapping[str, Mapping[str, any]], pd.DataFrame, np.ndarray],
            identifiers: Optional[Collection[str]] = None,
            metadata: Optional[Mapping[str, Mapping]] = None
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
            vector. Each vector must have the same dimensionality.

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
        """
        if metadata:
            warnings.warn("Metadata are presently ignored.")
        symbol_name = f"{VECTOR_DB_DISTINGUISHED_PREFIX}{namespace}"
        data_frame_to_upsert = _generate_dataframe(
            vectors,
            self._dimensions.get(namespace),
            identifiers,
            metadata
        )
        self._dimensions[namespace] = data_frame_to_upsert.shape[0]
        if symbol_name in self._lib:
            warnings.warn("This is extremely memory-inefficient!")
            old_df = self.read(namespace)
            updated_df = old_df.combine_first(data_frame_to_upsert)
            self._lib.write(
                symbol_name,
                updated_df
            )
        else: # symbol_name not in self._lib
            self._lib.write(
                symbol_name,
                data_frame_to_upsert
            )

    def top_k(
            self,
            namespace: str,
            k: int,
            query_vector: Collection[float],
            norm: Optional[str] = None,
            index: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Returns the top `k` most similar vectors to `query_vector` in `namespace`.

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
        if k < 1:
            raise ValueError("top-k makes sense only for integer k>0.")
        if norm:
            warnings.warn("Norms are presently ignored; we just use the Euclidean.")
        if index:
            warnings.warn("Indexing is presently unsupported.")
        qv = np.array(query_vector)
        if qv.ndim != 1:
            raise ValueError("Query vectors must be one-dimensional.")
        if qv.shape[0] != self._dimensions[namespace]:
            raise ValueError("Query vectors must have the same number of components "
                             f"({self._dimensions[namespace]}) "
                             "as the vectors in the VectorDB. The vector given had "
                             f"{qv.shape[0]} components.")
        q = QueryBuilder()
        q.clauses.append(_TopKClause(qv, k))
        q._python_clauses.append(PythonTopKClause(qv, k))
        result = self._lib.read(f"{VECTOR_DB_DISTINGUISHED_PREFIX}{namespace}", query_builder=q).data
        result.index = list(result.index[:-1]) + ["similarity"]
        return result

    def read(
            self,
            namespace: str,
            identifiers: Optional[Collection[str]] = None
    ) -> pd.DataFrame:
        """
        Reads a `namespace`.

        Parameters
        ----------
        namespace
            The namespace read.
        identifiers
            Presently ignored.
        """
        return self._lib.read(f"{VECTOR_DB_DISTINGUISHED_PREFIX}{namespace}", identifiers).data


class _VectorSymbol:
    def __init__(
            self,
            namespace: str,
            vector_db: VectorDB
    ):
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
            metadata: Optional[Mapping[str, Mapping]] = None
    ) -> None:
        self.vector_db.upsert(
            self.namespace,
            vectors,
            identifiers,
            metadata
        )

    def top_k(
            self,
            k: int,
            query_vector: Collection[float],
            norm: Optional[str] = None,
            index: Optional[str] = None
    ) -> pd.DataFrame:
        return self.vector_db.top_k(
            self.namespace,
            k,
            query_vector,
            norm,
            index
        )

    def read(
            self,
            identifiers: Optional[List[str]] = None
    ) -> pd.DataFrame:
        return self.vector_db.read(self.namespace, identifiers)
