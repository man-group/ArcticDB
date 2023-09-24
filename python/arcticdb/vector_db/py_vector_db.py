from typing import Optional, Union, Dict, Any

from arcticdb import Arctic
from arcticdb.encoding_version import EncodingVersion
from arcticdb.version_store.library import Library
from arcticdb.options import DEFAULT_ENCODING_VERSION, LibraryOptions
from arcticdb.exceptions import ArcticDbNotYetImplemented

import numpy as np
import pandas as pd
import faiss


VECTOR_NORMALISABLE_TYPES = (np.ndarray)
NormalisableVectors = Union[VECTOR_NORMALISABLE_TYPES]


def namespace_metadata_symbol(namespace: str) -> str:
    return f"{namespace}_metadata"


def namespace_bucket_vectors_symbol(namespace: str, segment: int) -> str:
    return f"{namespace}_segment_{segment}_vectors"


def namespace_segment_index_symbol(namespace: str, segment: int) -> str:
    return f"{namespace}_segment_{segment}_index"


def namespace_vectors_to_bucket_symbol(namespace: str) -> str:
    return f"{namespace}_vectors_to_segments"


def namespace_bucketiser_symbol(namespace: str) -> str:
    return f"{namespace}_bucketiser"


class VectorLibrary(object):
    """
    The main interface exposing read/write functionality in a given PyVectorDB.

    PyVectorDBs contain named namespaces which are the atomic unit of data storage within a PyVectorDB. Vector
    namespaces store vectors in a transparent but somewhat unintuitive format. For reasons of speed, vectors are stored
    serially in long columns. This is because storing arrays in each cell is not yet possible. (It will be soon.
    Internally, storing vectors serially in long columns is fairly similar in terms of memory layout to storing them
    as arrays.) These namespaces are nearly exactly analogous to symbols: see the documentation for the class Library.

    VectorLibraries should not directly be initialised. Instead, they should be initialised through a PyVectorDB's
    create_vector_library method.

    VectorLibraries' I/O capabilities and limitations follow exactly from those of the Arctic libraries underlying them.
    See the documentation of the Library class for details.
    """
    def __init__(
            self,
            library: Library
    ):
        self._lib = library
        self._metadata = {}
        # possibly inefficient for large numbers of namespaces.
        self._bucketisers = {}

    def __repr__(self):
        return f"VectorLibrary({str(self._lib)})"

    def __get__item(self, symbol: str) -> None:
        raise ArcticDbNotYetImplemented

    def _get_metadata(self, namespace: str) -> Dict[str, Any]:
        if namespace in self._metadata:
            return self._metadata[namespace]
        elif namespace_metadata_symbol(namespace) in self._lib.list_symbols():
            return {k: v[0] for k,v in
                    self._lib.read(namespace_metadata_symbol(namespace)).data.to_dict()}
            # See comment in create_namespace: to create a pandas frame to upsert we had to put the values in a
            # singleton list.
        else:
            raise Exception("Metadata not found.")

    def _get_bucketiser(self, namespace: str) -> faiss.Index:
        if namespace in self._bucketisers:
            return self._bucketisers[namespace]
        elif namespace_bucketiser_symbol(namespace) in self._bucketisers:
            return faiss.deserialize_index(self._lib.read(namespace_bucketiser_symbol(namespace)).data)
        else:
            raise Exception("Bucketiser not found.")


    def create_namespace(
            self,
            name: str,
            dimension: int,
            metric: str,
            bucketiser: Optional[str] = None,
            buckets: Optional[int] = None,
            index: Optional[str] = None,
            training_vectors: Optional[NormalisableVectors] = None
    ) -> None:
        """
        Creates a vector namespace.
        
        Parameters
        ----------
        name: str
            The name of the vector namespace to be created.
        dimension: int
            The number of dimensions of the vectors to be upserted.
        metric: str
            The metric to be used in searching the namespace. Must be "L2" or "IP".
        bucketisation: Optional[str], default = None
            It is very efficient in minimising I/O time to index vectors at a high level using an 'inverted index'. This
            involves bucketising the vectors by proximity, and searching only buckets close to the query vector. It is
            possible to do this in a number of ways, but presently, bucketisation must be "exact" if it is specified.
        buckets: Optional[int], default = None
            The number of buckets. If ``bucketisation`` is true, this must be specified.
        training_vectors: Optional[NormalisableVectors], default = None
            Training vectors to determine the bucketisation strategy. If ``bucketisation`` is true, this must be
            specified.
        """
        if metric not in ["L2", "IP"]:
            raise ArcticDbNotYetImplemented("Metrics other than L2 and IP are not presently supported.")
        if bucketiser:
            if bucketiser not in ["exact"]:
                raise ArcticDbNotYetImplemented("Non-exact bucketisation is not presently supported.")
            if (buckets is None) or (training_vectors is None):
                raise Exception("If a bucketiser is specified, a number of buckets and some training vectors must "
                                "be specified too.")

        if (training_vectors is not None):
            if training_vectors.shape[1] != dimension:
                raise Exception(f"Training vectors must be be of {dimension} dimensions, i.e., the second component of "
                            f"their shape must be {dimension}; yours had shape {training_vectors.shape}.")
            if len(training_vectors.shape) != 2:
                raise Exception(f"Training vectors should be given in a two-dimensional np.ndarray. Yours were of "
                                f"shape {training_vectors.shape}, with, therefore, {len(training_vectors.shape)} "
                                f"dimensions.")

        # We write metadata to a symbol for persistence.
        metadata = {
            "dimension": dimension,
            "metric": metric,
            "bucketiser": bucketiser,
            "buckets": buckets,
            "index": index
        }
        self._metadata[name] = metadata
        self._lib.write(namespace_metadata_symbol(name), pd.DataFrame({k: [v] for k,v in metadata.items()}))
        # Rather annoyingly the constructor for pd.DataFrame expects dictionaries like {"dimension": [30]}, not
        # {"dimension": 30}.

        # We work out the centroids and buckets.
        kmeans = faiss.Kmeans(dimension, buckets, niter=50, verbose=True)
        kmeans.train(training_vectors)
        # Now we initialise the bucketiser.
        if metric == "L2":
            bucketiser = faiss.IndexFlatL2(dimension)
        elif metric == "IP":
            bucketiser = faiss.IndexFlatIP(dimension)
        else:
            raise Exception("Metrics other than L2 and IP are not presently supported. This should've been caught.")
        bucketiser.add(kmeans.centroids)
        # It's more efficient not to re-read the bucketiser each time, and it should be fairly small.
        self._bucketisers[name] = bucketiser
        # But we still want to be able to read and write it.
        self._lib.write(
            namespace_bucketiser_symbol(name),
            faiss.serialize_index(bucketiser)
        )

        pass

    def upsert(
            self,
            namespace: str,
            vectors: pd.DataFrame
    ) -> None:
        # Some initial checks; we also get some metadata and the bucketiser.
        if len(vectors.shape) != 2:
            raise Exception("Vectors must be presented in a two-dimensional np.ndarray.")

        metadata = self._get_metadata(namespace)
        dimension = metadata["dimension"]
        index = metadata["index"]

        if vectors.shape[1] != dimension:
            raise Exception("Vectors of wrong dimensionality.")

        bucketiser = self._get_bucketiser(namespace)

        # We add a column in vectors corresponding to the bucket in which each vector should be placed.
        vectors["bucket"] = bucketiser.search(vectors.to_numpy(), 1)[1]

        for bucket, vectors_to_upsert in vectors.groupby("bucket"):
            if not self._lib.has_symbol(namespace_bucket_vectors_symbol(namespace, bucket)):
                self._initialise_bucket_index(namespace, bucket, index)
                self._lib.write(
                    namespace_bucket_vectors_symbol(namespace, bucket),
                    vectors_to_upsert.drop("bucket", axis=1).to_numpy().flatten()
                )
            else:
                self._lib.append(
                    namespace_bucket_vectors_symbol(namespace, bucket),
                    vectors_to_upsert.drop("bucket", axis=1).to_numpy().flatten()
                )
                self._update_bucket_index(namespace, bucket)
            # todo: add to the index in cpp

        vectors_to_buckets = vectors["bucket"].reset_index()
        vectors_to_buckets.rename(columns={"index": "label"}, inplace=True)
        self._lib.append(namespace_vectors_to_bucket_symbol(namespace), vectors_to_buckets)
        # reset_index allows us to use QueryBuilder to find the entry corresponding to a particular vector ID for
        # reading specific vectors by ID. This is of course dreadfully slow. An interesting question is whether Arctic
        # is at all suitable as a suitable store of data with many keys (i.e., big dictionaries.) That is because we
        # surely wish also to retrieve the original documents associated with an embedding, perhaps by ID, and Arctic
        # presumably should be able to do that. We probably can by hijacking the timestamp system or replicating it.

        pass

    def _initialise_bucket_index(
            self,
            namespace: str,
            bucket: int,
            index: str
    ):
        pass

    def _update_bucket_index(
            self,
            namespace: str,
            bucket: str
    ):
        pass


class PyVectorDB(object):
    def __init__(
            self,
            uri: str,
            encoding_version: EncodingVersion = DEFAULT_ENCODING_VERSION
    ):
        """
        Initialises a PyVectorDB.

        Parameters
        ----------
        See top-level documentation for the Arctic client.
        """
        self._arctic_client = Arctic(uri, encoding_version)
        #  todo: catch exceptions from self._arctic_client and explain them more legibly in terms of PyVectorDB.

    def __get__item(self, name: str) -> Library:
        return VectorLibrary(self._arctic_client[name])

    def __repr__(self):
        return f"PyVectorDB({str(self._arctic_client)})"

    def create_vector_library(self, name: str, library_options: Optional[LibraryOptions] = None):
        """
        Creates a vector library named ``name``. See documentation for VectorLibrary.

        Parameters
        ----------
        name: str
            The name of the vector library to be created.
        library_options: Optional[LibraryOptions], default = None
            Library options for the Arctic library storing the vector library's contents.
        """
        return VectorLibrary(self._arctic_client.create_library(name, library_options))

    def get_vector_library(
            self,
            name: str,
            create_if_missing: Optional[bool] = False,
            library_options: Optional[LibraryOptions] = None
    ) -> VectorLibrary:
        """
        Returns the vector library named ``name``. This can also be invoked by subscripting.

        Parameters
        ----------
        name: str
            The name of the vector library to be retrieved.
        create_if_missing: Optional[bool], default = False
            If true, and the vector library is missing, it is created.
        library_options: Optional[LibraryOptions], default = None
            Library options for the Arctic library storing the vector library's contents.

        Returns
        -------
        Library
        """
        return VectorLibrary(self._arctic_client.get_library(name, create_if_missing, library_options))
