import warnings
from typing import Union, Dict, Optional, Iterable


import pandas as pd
import numpy as np

from collections.abc import Mapping
from collections import namedtuple

from arcticdb.supported_types import Timestamp, numeric_types
from arcticdb.version_store.library import Library, ArcticUnsupportedDataTypeException

from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.version_store import TopKClause as _TopKClause

VECTOR_DB_DISTINGUISHED_PREFIX = "vector_db_"

VECTOR_VALUE_ERROR = "Vectors uploaded as dictionaries must be " \
                     "in the form of dictionaries with keys that are " \
                     "string identifiers and values that are " \
                     "dictionaries containing in turn " \
                     "at least an entry under 'values' containing " \
                     "something that can be coerced to an `np.ndarray` " \
                     "of floats of one dimension."

PythonTopKClause = namedtuple("TopKClause", ["vector", "k"])


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
        self._lib = library
        self._dimensions = dict()  # dimensionality of vectors in each sym

    def __repr__(self):
        return f"VectorDB({str(self._lib)})"

    def __contains__(self, namespace: str):
        return f"{VECTOR_DB_DISTINGUISHED_PREFIX}{namespace}" in self._lib

    def upsert(
            self,
            namespace: str,
            vectors: Union[Dict[str, Dict[str, any]], pd.DataFrame, np.ndarray],
            identifiers: Optional[Iterable[str]] = None,
            metadata: Optional[Dict[str, Dict]] = None
    ) -> None:
        """
        Parameters
        ----------
        namespace
            The namespace to which `vectors` should be upserted.
        vectors
            In the case of a dictionary, we expect a dictionary whose keys are strings
            (taken as identifiers of vectors), and whose values are in turn dictionaries
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
        data_frame_to_upsert = None
        symbol_name = f"{VECTOR_DB_DISTINGUISHED_PREFIX}{namespace}"
        dimensions = self._dimensions.get(namespace)
        if symbol_name in self._lib:
            raise NotImplementedError
        else:  # namespace not in self.lib
            if isinstance(vectors, Mapping):
                data_frame_to_upsert = pd.DataFrame()
                for k, v in vectors.items():
                    if type(k) is not str:
                        raise ArcticUnsupportedDataTypeException(VECTOR_VALUE_ERROR)
                    if "vectors" not in v.keys():
                        raise ValueError()
                    column = np.array(v)
                    if column.ndim != 1:
                        raise ValueError(VECTOR_VALUE_ERROR)
                    if column.dtype not in numeric_types:
                        raise ArcticUnsupportedDataTypeException(
                            "Vectors inserted must all have a numeric type. You attempted to "
                            f"insert a vector that as an np.array has dtype {column.dtype}, "
                            "which does count."
                        )
                    if not dimensions:
                        self._dimensions[namespace] = column.shape[0]
                        dimensions = column.shape[0]
                    elif column.shape[0] != dimensions:
                        raise ValueError("The vectors must all have the same number "
                                         "of dimensions.")
                    data_frame_to_upsert[k] = column.astype(np.float32)
            elif isinstance(vectors, pd.DataFrame):
                if any([t not in numeric_types for t in vectors.dtypes.unique()]):
                    raise ArcticUnsupportedDataTypeException(
                        "Vectors inserted must all have a numeric type. You attempted to "
                        f"insert {vectors.dtypes.unique()}, at least one of which does "
                        "count."
                    )
                self._dimensions[namespace] = vectors.shape[0]
                data_frame_to_upsert = vectors.astype(np.float32)
            elif isinstance(vectors, np.ndarray):
                if vectors.ndim != 2:
                    raise ValueError("Upsertion of vectors in an `np.ndarray` takes "
                                     "two-dimensional arrays; the parameter `vector` had "
                                     f"{vectors.ndim} instead.")
                if any([type not in numeric_types for type in vectors.dtypes.unique()]):
                    raise ArcticUnsupportedDataTypeException(
                        "Vectors inserted must all have a numeric type. You attempted to "
                        f"insert {vectors.dtypes.unique()}, at least one of which does "
                        "count."
                    )
                if not identifiers:
                    raise ValueError("Upsertion of vectors in an `np.ndarray` requires "
                                     "a list of identifiers.")
                if len(identifiers) != len(vectors):
                    raise ValueError(f"You gave {len(identifiers)} identifiers but "
                                     f"{len(vectors)} vectors.")
                if any([type(identifier) is not str for identifier in identifiers]):
                    raise ValueError(f"All identifiers must be strings.")
                self._dimensions[namespace] = vectors.shape[1]
                data_frame_to_upsert = pd.DataFrame(vectors.T, columns=identifiers)
            else:
                raise ArcticUnsupportedDataTypeException(
                    f"Upsertion of vectors of type {type(vectors)} is unsupported."
                )
        self._lib.write(
            symbol_name,
            data_frame_to_upsert)

    def top_k(
            self,
            namespace: str,
            k: int,
            query_vector: Iterable[float],
            norm: Optional[str] = None,
            index: Optional[str] = None
    ) -> pd.DataFrame:
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