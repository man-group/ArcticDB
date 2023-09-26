from typing import Optional, Union, Dict, Any

from arcticdb import Arctic
from arcticdb.encoding_version import EncodingVersion
from arcticdb.version_store.library import Library, WritePayload
from arcticdb.options import DEFAULT_ENCODING_VERSION, LibraryOptions
from arcticdb.exceptions import ArcticDbNotYetImplemented

from arcticdb_ext.version_store import PythonVersionStoreVersionQuery as _PythonVersionStoreVersionQuery

import numpy as np
import pandas as pd
import faiss


VECTOR_NORMALISABLE_TYPES = (np.ndarray)
NormalisableVectors = Union[VECTOR_NORMALISABLE_TYPES]


def namespace_metadata_symbol(namespace: str) -> str:
    return f"{namespace}_metadata"


def namespace_bucket_vectors_symbol(namespace: str, bucket: int) -> str:
    return f"{namespace}_bucket_{bucket}_vectors"


def namespace_bucket_index_symbol(namespace: str, bucket: int) -> str:
    return f"{namespace}_bucket_{bucket}_index"


def namespace_bucket_label_map_symbol(namespace: str, bucket: int) -> str:
    """
    When searching without an index, we get labels relative to the ordering of the vectors in the segment. This symbol
    maps to the original.
    """
    return f"{namespace}_bucket_{bucket}_label_map"


def namespace_vectors_to_bucket_symbol(namespace: str) -> str:
    return f"{namespace}_vectors_to_buckets"


def namespace_insertion_bucketiser_symbol(namespace: str) -> str:
    return f"{namespace}_insertion_bucketiser"


def namespace_query_bucketiser_symbol(namespace: str) -> str:
    return f"{namespace}_query_bucketiser"


def namespace_centroids_symbol(namespace: str) -> str:
    return f"{namespace}_centroids"


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
        self._insertion_bucketisers = {}
        self._query_bucketisers = {}
        self._centroids = {}

    def __repr__(self):
        return f"VectorLibrary({str(self._lib)})"

    def __get__item(self, symbol: str) -> None:
        raise ArcticDbNotYetImplemented

    def _write_metadata(self, namespace: str, metadata: Dict[str, Any]):
        self._metadata[namespace] = metadata
        self._lib.write_pickle(namespace_metadata_symbol(namespace), pd.DataFrame({k: [v] for k,v in metadata.items()}))
        # Rather annoyingly the constructor for pd.DataFrame expects dictionaries like {"dimension": [30]}, not
        # {"dimension": 30}.

    def _write_query_bucketiser(self, namespace: str, query_bucketiser: faiss.Index):
        self._query_bucketisers[namespace] = query_bucketiser
        self._lib.write(namespace_query_bucketiser_symbol(namespace), faiss.serialize_index(query_bucketiser))

    def _get_metadata(self, namespace: str) -> Dict[str, Any]:
        if namespace in self._metadata:
            return self._metadata[namespace]
        elif namespace_metadata_symbol(namespace) in self._lib.list_symbols():
            return {k: v[0] for k,v in
                    self._lib.read_pickle(namespace_metadata_symbol(namespace)).data.to_dict()}
            # See comment in create_namespace: to create a pandas frame to upsert we had to put the values in a
            # singleton list.
        else:
            raise Exception(f"PyVectorDB: metadata not found for {namespace}.")

    def _get_insertion_bucketiser(self, namespace: str) -> faiss.Index:
        if namespace in self._insertion_bucketisers:
            return self._insertion_bucketisers[namespace]
        elif namespace_insertion_bucketiser_symbol(namespace) in self._insertion_bucketisers:
            return faiss.deserialize_index(self._lib.read(namespace_insertion_bucketiser_symbol(namespace)).data)
        else:
            raise Exception(f"PyVectorDB: insertion bucketiser not found for {namespace}.")

    def _get_query_bucketiser(self, namespace: str) -> faiss.Index:
        if namespace in self._query_bucketisers:
            return self._query_bucketisers[namespace]
        elif namespace_query_bucketiser_symbol(namespace) in self._query_bucketisers:
            return faiss.deserialize_index(self._lib.read(namespace_query_bucketiser_symbol(namespace)).data)
        else:
            raise Exception(f"PyVectorDB: query bucketiser not found for {namespace}.")

    def _get_centroids(self, namespace: str) -> np.ndarray:
        if namespace in self._centroids:
            return self._centroids[namespace]
        elif namespace_centroids_symbol(namespace) in self._query_bucketisers:
            return self._lib.read(namespace_centroids_symbol(namespace)).data
        else:
            raise Exception(f"PyVectorDB: centroids not found for {namespace}.")


    def create_namespace(
            self,
            name: str,
            dimension: int,
            metric: str,
            insertion_bucketiser: Optional[str] = None,
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
        if insertion_bucketiser:
            if insertion_bucketiser not in ["exact"]:
                raise ArcticDbNotYetImplemented("Non-exact bucketisation is not presently supported.")
            if (buckets is None) or (training_vectors is None):
                raise Exception("If a insertion_bucketiser is specified, a number of buckets and some training vectors must "
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
            "insertion_bucketiser": insertion_bucketiser,
            "buckets": buckets,
            "live_buckets": [],
            "index": index
        }
        self._write_metadata(name, metadata)


        # We work out the centroids and buckets.
        kmeans = faiss.Kmeans(dimension, buckets, niter=50, verbose=True)
        kmeans.train(training_vectors)
        # Now we initialise the insertion_bucketiser.
        if metric == "L2":
            insertion_bucketiser = faiss.IndexFlatL2(dimension)
            query_bucketiser = faiss.index_factory(dimension, "IDMap, Flat", faiss.METRIC_L2)
        elif metric == "IP":
            insertion_bucketiser = faiss.IndexFlatIP(dimension)
            query_bucketiser = faiss.index_factory(dimension, "IDMap,Flat", faiss.METRIC_INNER_PRODUCT)
        else:
            raise Exception("Metrics other than L2 and IP are not presently supported. This should've been caught.")
        insertion_bucketiser.add(kmeans.centroids)
        # It's more efficient not to re-read the insertion_bucketiser each time, and it should be fairly small.
        self._insertion_bucketisers[name] = insertion_bucketiser
        # But we still want to be able to read and write it.
        self._lib.write(
            namespace_insertion_bucketiser_symbol(name),
            faiss.serialize_index(insertion_bucketiser)
        )
        # Not all bucketisers will be available at any given time.
        self._query_bucketisers[name] = query_bucketiser
        self._lib.write(
            namespace_query_bucketiser_symbol(name),
            faiss.serialize_index(query_bucketiser)
        )
        # We also store the centroids.
        self._centroids[name] = kmeans.centroids
        self._lib.write(
            namespace_centroids_symbol(name),
            kmeans.centroids
        )

        pass
    

    def upsert(
            self,
            namespace: str,
            vectors: pd.DataFrame
    ) -> None:
        # Some initial checks; we also get some metadata and the insertion_bucketiser.
        if len(vectors.shape) != 2:
            raise Exception("Vectors must be presented in a two-dimensional np.ndarray.")

        metadata = self._get_metadata(namespace)
        dimension = metadata["dimension"]
        index = metadata["index"]

        if vectors.shape[1] != dimension:
            raise Exception("Vectors of wrong dimensionality.")

        insertion_bucketiser = self._get_insertion_bucketiser(namespace)
        query_bucketiser = self._get_query_bucketiser(namespace)
        centroids = self._get_centroids(namespace)

        # We add a column in vectors corresponding to the bucket in which each vector should be placed.
        vectors["bucket"] = insertion_bucketiser.search(vectors.to_numpy(), 1)[1]
        append_payloads = []

        for bucket, vectors_to_upsert in vectors.groupby("bucket"):
            query_bucketiser.remove_ids(np.array([bucket]))
            query_bucketiser.add_with_ids(np.array([centroids[bucket]]), np.array([bucket]))
            # todo: dedup more sensibly.
            flattened = vectors_to_upsert.drop("bucket", axis=1).to_numpy().flatten()
            labels = vectors_to_upsert.index.to_numpy().astype(np.uint64)
            append_payloads.append(WritePayload(
                namespace_bucket_vectors_symbol(namespace, bucket),
                flattened
            ))
            if index:
                if not self._lib.has_symbol(namespace_bucket_vectors_symbol(namespace, bucket)):
                    self._lib._nvs.version_store.initialise_bucket_index(
                        namespace_bucket_index_symbol(namespace, bucket),
                        index,
                        "L2",
                        dimension,
                        flattened,
                        labels
                    )
                    # todo: work out whatever cursed check is stopping us from reading the symbol since something is probably
                    # wrong but I can't work out what.
                else:
                    self._lib._nvs.version_store.update_bucket_index(
                        namespace_bucket_index_symbol(namespace, bucket),
                        flattened,
                        labels
                    )
            else: # not index so need to store a label map
                append_payloads.append(WritePayload(
                    namespace_bucket_label_map_symbol(namespace, bucket),
                    labels
                ))
        self._lib.append_batch(append_payloads)
        self._write_query_bucketiser(namespace, query_bucketiser)

        vectors_to_buckets = vectors["bucket"].reset_index()
        vectors_to_buckets.rename(columns={"index": "label"}, inplace=True)
        self._lib.append(namespace_vectors_to_bucket_symbol(namespace), vectors_to_buckets)
        # reset_index allows us to use QueryBuilder to find the entry corresponding to a particular vector ID for
        # reading specific vectors by ID. This is of course dreadfully slow. An interesting question is whether Arctic
        # is at all suitable as a suitable store of data with many keys (i.e., big dictionaries.) That is because we
        # surely wish also to retrieve the original documents associated with an embedding, perhaps by ID, and Arctic
        # presumably should be able to do that. We probably can by hijacking the timestamp system or replicating it.

        pass

    def query(
            self,
            namespace: str,
            query_vectors: np.ndarray,
            k: int,
            nprobes: int
    ) -> Any:
        if len(query_vectors.shape) != 2:
            raise Exception("PyVectorDB: Vectors must be presented in a two-dimensional np.ndarray.")
        metadata = self._get_metadata(namespace)
        dimension = metadata["dimension"]
        index = metadata["index"]
        if query_vectors.shape[1] != dimension:
            raise Exception(f"PyVectorDB: query vectors to namespace {namespace} must have length {dimension}")

        query_bucketiser = self._get_query_bucketiser(namespace)
        distances, buckets = query_bucketiser.search(query_vectors, nprobes)
        buckets_to_vectors = {bucket: [] for bucket in set(buckets.flatten())} # vectors to search in each bucket
        for i in range(query_vectors.shape[0]):
            for bucket in buckets[i]:
                buckets_to_vectors[bucket].append(i)

        if index:
            results_by_bucket = {bucket: self._lib._nvs.version_store.search_bucket_with_index(
                    namespace_bucket_index_symbol(namespace, bucket),
                    _PythonVersionStoreVersionQuery(),
                    np.concatenate([query_vectors[i] for i in vectors]),
                    k
                ) for bucket, vectors in buckets_to_vectors.items()}
        else:
            vector_buckets, label_maps, vb_queries, lm_queries, query_vectors_flattened = [], [], [], [], []
            for bucket, vectors in buckets_to_vectors.items():
                vector_buckets.append(namespace_bucket_vectors_symbol(namespace, bucket))
                label_maps.append(namespace_bucket_label_map_symbol(namespace, bucket))
                vb_queries.append(_PythonVersionStoreVersionQuery())
                lm_queries.append(_PythonVersionStoreVersionQuery())
                query_vectors_flattened.append(np.concatenate([query_vectors[i] for i in vectors]))
            search_result = self._lib._nvs.version_store.batch_search_bucket_without_index(
                vector_buckets, label_maps, vb_queries, lm_queries, query_vectors_flattened,
                k, dimension
            )
            results_by_bucket = {bucket[0]: search_result[i] for i, bucket in enumerate(buckets_to_vectors.items())}

        results_by_vector = {i: {"labels": [], "distances": []} for i in range(len(query_vectors))}
        for bucket, (d, l) in results_by_bucket.items():
            vectors = buckets_to_vectors[bucket]
            for i in range(len(d) // k): # iterate over the number of vectors queried from each bucket.
                results_by_vector[vectors[i]]["labels"] += d[i*k:(i+1)*k]
                results_by_vector[vectors[i]]["distances"] += l[i*k:(i+1)*k]
        top_k_by_vector = {i: [(label, distance) for distance, label in sorted(zip(res["distances"], res["labels"]))] for res in results_by_vector.values()}
        return top_k_by_vector


        print(0)




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
