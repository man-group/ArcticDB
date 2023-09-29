from typing import Optional, List, Tuple

from arcticdb import Arctic
from arcticdb.encoding_version import EncodingVersion
from arcticdb.version_store.library import Library, WritePayload
from arcticdb.options import DEFAULT_ENCODING_VERSION, LibraryOptions
from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb.vector_db.utils import *
# from utils import *
from arcticdb_ext.version_store import PythonVersionStoreVersionQuery as _PythonVersionStoreVersionQuery

import numpy as np
import pandas as pd
import faiss
from math import floor


class VectorLibrary(object):
    """
    The main interface exposing read/write functionality in a given PyVectorDB.

    PyVectorDBs are a proof of concept-in-a-proof of concept. Various tasks that should be undertaken in C++ are instead
    implemented here. In particular, what should be stored in contiguous segments in a symbol is instead stored in
    separate symbols.

    PyVectorDBs contain named namespaces which are the atomic unit of data storage within a PyVectorDB. Vector
    namespaces store vectors in a transparent but somewhat unintuitive format. For reasons of speed, vectors are stored
    serially in long columns. This is because storing arrays in each cell is not yet possible. (It will be soon.
    Internally, storing vectors serially in long columns is fairly similar in terms of memory layout to storing them
    as arrays.) These namespaces are nearly exactly analogous to symbols: see the documentation for the class Library.

    VectorLibraries should not directly be initialised. Instead, they should be initialised through a PyVectorDB's
    create_vector_library method.

    VectorLibraries' I/O capabilities and limitations follow exactly from those of the Arctic libraries underlying them.
    See the documentation of the Library class for details.

    The _write and _get methods below are used to cache certain useful data in memory but to write them to Arctic for
    future retrieval.

    The principal notable feature of a VectorLibrary is that it contains buckets and two bucketisers:

    * a query bucketiser, and
    * an insertion bucketiser.

    The insertion bucketiser contains a certain number of centroids and assigns inserted vectors to the corresponding
    bucket. Not all buckets are populated. We have a separate query bucketiser for two reasons.

    1. There is no point in searching empty buckets.
    2. More importantly, to guarantee that top-k queries return at least k, we can simply search at least k (probes).
    When k < nprobes we raise an exception.

    The buckets, as initialised, are given increasing integer numbers. The query bucketiser will only output a certain
    subset of those numbers, but the numbering is the same. (Faiss has IDMap functionality.)

    The buckets are initialised as Voronoi cells about centroids which are in turn k-means from a training set.

    In querying, we use the query bucketiser to work out which buckets to look inside. In inserting, we use the
    insertion bucketiser to insert; any buckets so initialised are then added to the query bucketiser.

    We keep track of the size of the buckets. It is possible to initialise new buckets by splitting up existing buckets
    that are too large. This can be enabled with ``dynamic_rebucketise``. This does not preserve the size of buckets.

    Bucketisation with respect to symbols poses a problem in identifying vectors with respect to their internal position
    and with respect to their position in the namespace. Two features follow.

    1. Faiss allows one to ``add_with_ids``, in which case we can add vectors to bucket-specific indices with their
    namespace-relative labels.
    2. We store two maps, one from namespace labels to bucket labels, and one vice versa. Ideally there would be some
    way of storing two-way dictionaries efficiently in Arctic. These are the so-called ``label_map``s.
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

    def __get__item(self, namespace: str) -> None:
        raise ArcticDbNotYetImplemented
        # Possibly convenient syntax:
        # >>> vector_library[namespace].query(query_vectors, k)
        # or even
        # >>> namespace = vector_library["namespace"]
        # >>> namespacee.query(query_vectors, k)

    def _write_metadata(self, namespace: str, metadata: NamespaceMetadata):
        self._metadata[namespace] = metadata
        self._lib.write_pickle(namespace_metadata_symbol(namespace), metadata)
        # Rather annoyingly the constructor for pd.DataFrame expects dictionaries like {"dimension": [30]}, not
        # {"dimension": 30}.

    def _write_query_bucketiser(self, namespace: str, query_bucketiser: faiss.Index):
        self._query_bucketisers[namespace] = query_bucketiser
        self._lib.write(namespace_query_bucketiser_symbol(namespace), faiss.serialize_index(query_bucketiser))

    def _write_insertion_bucketiser(self, namespace: str, query_bucketiser: faiss.Index):
        self._insertion_bucketisers[namespace] = query_bucketiser
        self._lib.write(namespace_insertion_bucketiser_symbol(namespace), faiss.serialize_index(query_bucketiser))

    def _write_centroids(self, namespace: str, centroids: np.ndarray):
        self._centroids[namespace] = centroids
        self._lib.write(namespace_centroids_symbol(namespace), centroids)

    def _get_metadata(self, namespace: str) -> NamespaceMetadata:
        if namespace in self._metadata:
            return self._metadata[namespace]
        elif namespace_metadata_symbol(namespace) in self._lib.list_symbols():
            return self._lib.read_pickle(namespace_metadata_symbol(namespace))
        else:
            raise PyVectorDBException(f"Metadata not found for {namespace}.")

    def _get_insertion_bucketiser(self, namespace: str) -> faiss.Index:
        if namespace in self._insertion_bucketisers:
            return self._insertion_bucketisers[namespace]
        elif namespace_insertion_bucketiser_symbol(namespace) in self._insertion_bucketisers:
            return faiss.deserialize_index(self._lib.read(namespace_insertion_bucketiser_symbol(namespace)).data)
        else:
            raise PyVectorDBException(f"PyVectorDB: insertion bucketiser not found for {namespace}.")

    def _get_query_bucketiser(self, namespace: str) -> faiss.IndexIDMap:
        if namespace in self._query_bucketisers:
            return self._query_bucketisers[namespace]
        elif namespace_query_bucketiser_symbol(namespace) in self._query_bucketisers:
            return faiss.deserialize_index(self._lib.read(namespace_query_bucketiser_symbol(namespace)).data)
        else:
            raise PyVectorDBException(f"PyVectorDB: query bucketiser not found for {namespace}.")

    def _get_centroids(self, namespace: str) -> np.ndarray:
        if namespace in self._centroids:
            return self._centroids[namespace]
        elif namespace_centroids_symbol(namespace) in self._query_bucketisers:
            return self._lib.read(namespace_centroids_symbol(namespace)).data
        else:
            raise PyVectorDBException(f"PyVectorDB: centroids not found for {namespace}.")

    def create_namespace(
            self,
            name: str,
            dimension: int,
            metric: str,
            insertion_bucketiser: str = "IDMap, Flat",
            buckets: Optional[int] = None,
            index: Optional[str] = None,
            training_vectors: Optional[NormalisableVectors] = None,
            dynamic_rebucketise = True
    ) -> None:
        """
        Creates a vector namespace.
        
        Parameters
        ----------
        name: str
            The name of the vector namespace to be created.
        dimension: int
            The number of dimensions of the vectors to be inserted.
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
        dynamic_rebucketise
            Whether to attempt to rebucketise the vectors for performance.
        """
        if insertion_bucketiser:
            if (buckets is None) or (training_vectors is None):
                raise PyVectorDBNotYetImplemented("A number of buckets and some training vectors must be specified.")
        if (training_vectors is not None):
            if training_vectors.shape[1] != dimension:
                raise Exception(f"Training vectors must be be of {dimension} dimensions, i.e., the second component of "
                            f"their shape must be {dimension}; yours had shape {training_vectors.shape}.")
            if len(training_vectors.shape) != 2:
                raise Exception(f"Training vectors should be given in a two-dimensional np.ndarray. Yours were of "
                                f"shape {training_vectors.shape}, with, therefore, {len(training_vectors.shape)} "
                                f"dimensions.")
        # We could do some typechecking here. But this is a poc-within-a-poc so there's probably no need to.

        # We write metadata to a symbol for persistence.
        metadata = NamespaceMetadata(
            dimension,
            metric,
            insertion_bucketiser,
            buckets,
            index,
            {},
            dynamic_rebucketise,
            0
        )
        self._write_metadata(name, metadata)


        # We work out the centroids and buckets.
        kmeans = faiss.Kmeans(dimension, buckets, niter=20, verbose=True)
        kmeans.train(training_vectors)
        # Now we initialise the insertion_bucketiser.
        query_bucketiser = faiss.index_factory(dimension, "IDMap, Flat", get_metric(metric))
        insertion_bucketiser = faiss.index_factory(dimension, "IDMap, Flat", get_metric(metric))
        insertion_bucketiser.add_with_ids(kmeans.centroids, np.arange(buckets))
        # It's more efficient not to re-read the bucketisers each time: they should be fairly small.
        # But we still want to be able to read and write it for persistence.
        self._insertion_bucketisers[name] = insertion_bucketiser
        self._lib.write(
            namespace_insertion_bucketiser_symbol(name),
            faiss.serialize_index(insertion_bucketiser)
        )
        self._query_bucketisers[name] = query_bucketiser
        self._lib.write(
            namespace_query_bucketiser_symbol(name),
            faiss.serialize_index(query_bucketiser)
        )
        # We also store the centroids.
        self._write_centroids(name, kmeans.centroids)


    def insert(
            self,
            namespace: str,
            vectors: pd.DataFrame,
            uniqueness_check: bool = True,
            disable_rebucketise: bool = False
    ) -> None:
        """
        Insert vectors to a named namespace. This should be relatively efficient, unlike upsert. It relies on a
        so-called 'largest label' to ensure uniqueness of labels: the labels of all freshly inserted labels must be
        greater than the existing largest label.

        Parameters
        ----------
        namespace: str
            The namespace to insert to.
        vectors: pd.DataFrame
            The vectors to insert. The column headings of the dataframe inserted must be integers.
        uniqueness_check: bool, default: True
            Checks for uniqueness of labels inserted.
        disable_rebucketise: bool, default: False
            Disable rebucketisation for this read.
        """
        # Some initial checks; we also get some metadata and the bucketisers.
        if len(vectors.shape) != 2:
            raise Exception("Vectors must be presented in a two-dimensional np.ndarray.")

        metadata = self._get_metadata(namespace)
        dimension = metadata.dimension
        index = metadata.index

        if vectors.shape[1] != dimension:
            raise Exception("Vectors of wrong dimensionality.")

        insertion_bucketiser = self._get_insertion_bucketiser(namespace)
        query_bucketiser = self._get_query_bucketiser(namespace)
        centroids = self._get_centroids(namespace)

        # We add a column in vectors corresponding to the bucket in which each vector should be placed.
        vectors["bucket"] = insertion_bucketiser.search(vectors.to_numpy(), 1)[1]
        append_payloads = []
        
        if min(vectors.index) < metadata.largest_label and uniqueness_check:
            raise PyVectorDBInvalidArgument("To maintain uniqueness, vectors should be inserted with increasing "
                                            f"labels. The smallest label was {min(vectors.index)}; the minimum "
                                            f"label size is presently {metadata.largest_label}.")

        # Now we insert into each bucket.
        for bucket, vectors_to_insert in vectors.groupby("bucket"):
            # We want to keep track of which buckets have vectors in them. So we only add centroids to the
            # query bucketiser when vectors are upserted to them. There is the problem here that we might then add
            # duplicate centroids. So we preemptively remove from query_buckeiser just in case.
            query_bucketiser.remove_ids(np.array([bucket]))
            query_bucketiser.add_with_ids(np.array([centroids[bucket]]), np.array([bucket]))
            # todo: dedup more sensibly
            # We don't want to record the bucket of the vectors within the bucket.
            # We write batch. We flatten here because writing long columns is efficient but we want vectors to be
            # contiguous in memory. Until numpy array supported is added, this is the next best option.
            flattened = vectors_to_insert.drop("bucket", axis=1).to_numpy().flatten()
            append_payloads.append(WritePayload(
                namespace_bucket_vectors_symbol(namespace, bucket),
                flattened
            ))
            # We need a map from namespace labels to bucket labels for reading. nans represent vectors not in the
            # bucket and are hopefully stored efficiently.
            namespace_label_to_bucket_label = pd.DataFrame(
                np.nan,
                index=np.arange(max(vectors.index)+1),
                columns=[NAMESPACE_LABEL_TO_BUCKET_LABEL_COLUMN]
            )
            namespace_label_to_bucket_label.loc[
                vectors_to_insert.index,
                NAMESPACE_LABEL_TO_BUCKET_LABEL_COLUMN
            ] = np.arange(len(vectors_to_insert))
            append_payloads.append(WritePayload(
                namespace_bucket_inverse_label_map_symbol(namespace, bucket),
                namespace_label_to_bucket_label
            ))
            # We need the labels explicitly to populate a map from the bucket labels to the namespace labels.
            labels = vectors_to_insert.index.to_numpy().astype(np.uint64)
            # If there is an index, we add to the faiss index with the labels. If there isn't, we store a map ourselves.
            if index:  # i.e. if there is meant to be an index!
                # If no index has been initialised, start one.
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
        # We finally write to the individual buckets.
        self._lib.append_batch(append_payloads)
        # Then we record any centroids we've added to the query bucketiser.
        self._write_query_bucketiser(namespace, query_bucketiser)

        # We need to update the metadata:
        # first, we keep track of the sizes of buckets for dynamic rebucketisation; and
        for bucket, vectors_to_insert in vectors.groupby("bucket"):
            if bucket in metadata.bucket_sizes:
                metadata.bucket_sizes[bucket] += len(vectors_to_insert)
            else:
                metadata.bucket_sizes[bucket] = len(vectors_to_insert)
        # second, the largest label must be updated. Then we write the new metadata.
        metadata.largest_label = max(vectors.index)
        self._write_metadata(namespace, metadata)

        # Optionally, we can do this, to make read more efficient; but I haven't.
        # vectors_to_buckets = vectors["bucket"].reset_index()
        # vectors_to_buckets.rename(columns={"index": "label"}, inplace=True)
        # self._lib.append(namespace_vectors_to_bucket_symbol(namespace), vectors_to_buckets)

        # Finally, if we do want to rebucketise, we do so.
        if metadata.dynamic_rebucketise and (not disable_rebucketise):
            self.rebucketise(namespace)

    def rebucketise(
            self,
            namespace: str
    ) -> None:
        """
        Rebucketises a namespace: large buckets are deduplicated.

        A sensible strategy is to split buckets that are more than twice the mean size.

        Parameters
        ----------
        namespace: str
        """
        bucket_sizes = self._get_metadata(namespace).bucket_sizes
        # We should maintain the invariant that buckets containing zero vectors are not in the query bucketiser.
        buckets = len([size for size in bucket_sizes.values() if size != 0])
        total_vectors = sum(bucket_sizes.values())
        mean = total_vectors / buckets
        buckets_to_deduplicate = [key for key, size in bucket_sizes.items() if size > 2*mean]
        next_bucket = len(self._centroids[namespace])
        # We try to remove up to buckets_added empty buckets at the end.
        buckets_added = 0

        for bucket in buckets_to_deduplicate:
            # Note that some buckets might be more than twice the mean after an upsert.
            splits = floor(bucket_sizes[bucket] / mean)
            buckets_added += (splits-1)
            kmeans = faiss.Kmeans(
                self._get_metadata(namespace).dimension,
                splits,
                niter=20,
                verbose=True
            )
            kmeans.train(
                self.read_whole_bucket(namespace, bucket).to_numpy()[np.random.choice(
                    bucket_sizes[bucket],
                    size=floor((bucket_sizes[bucket]) ** 0.5),
                    replace=False
                )]
            )
            # Now we have new centroids in kmeans.centroids.
            # First, amend the centroids recorded.
            self._write_centroids(namespace, np.append(self._get_centroids(namespace), kmeans.centroids, axis=0))
            # Second, amend the query bucketiser.
            query_bucketiser = self._get_query_bucketiser(namespace)
            query_bucketiser.remove_ids(np.array([bucket]))
            query_bucketiser.add_with_ids(
                kmeans.centroids,
                np.array(np.arange(next_bucket, next_bucket+splits))
            )
            self._write_query_bucketiser(namespace, query_bucketiser)
            # Third, amend the insertion bucketiser.
            insertion_bucketiser = self._get_insertion_bucketiser(namespace)
            insertion_bucketiser.remove_ids(np.array([bucket]))
            insertion_bucketiser.add_with_ids(
                kmeans.centroids,
                np.array(np.arange(next_bucket, next_bucket+splits))
            )
            self._write_insertion_bucketiser(namespace, insertion_bucketiser)
            # Fourth, rewrite the new vectors, and delete the old ones.
            self.insert(
                namespace,
                self.read_whole_bucket(namespace, bucket),
                uniqueness_check=False,  # Since we are inserting under the largest label.
                disable_rebucketise=True  # Since we don't recursively rebucketise.
            )
            self._lib.delete(namespace_bucket_vectors_symbol(namespace, bucket))
            self._lib.delete(namespace_bucket_index_symbol(namespace, bucket))
            self._lib.delete(namespace_bucket_inverse_label_map_symbol(namespace, bucket))
            self._lib.delete(namespace_bucket_label_map_symbol(namespace, bucket))
            # Fifth, rewrite the bucket size of the old bucket.
            metadata = self._get_metadata(namespace)
            metadata.bucket_sizes[bucket] = DELETED_BUCKET
            self._write_metadata(namespace, metadata)
            # Sixth, bump up next_bucket.
            next_bucket += splits

    def read(
            self,
            namespace: str,
            labels: np.ndarray
    ) -> pd.DataFrame:
        """
        Reads vectors with the labels given.

        Parameters
        ----------
        namespace: str
        labels: np.ndarray

        Returns
        -------
        pd.DataFrame
        """
        return pd.concat([self._read_ids_from_bucket(namespace, bucket, labels)
                          for bucket, members in self._get_metadata(namespace).bucket_sizes.items()
                          if members > 0])

    def _read_ids_from_bucket(
            self,
            namespace: str,
            bucket: int,
            namespace_labels: np.ndarray  # w.r.t. the namespace
    ) -> pd.DataFrame:
        """
        Reads any vectors within the bucket with the given namespace labels.

        Parameters
        ----------
        namespace: str
        bucket: int
        namespace_labels: np.ndarray
            These labels are with respect to the namespace and are mapped to bucket labels.
        """
        label_map = self._lib.read(namespace_bucket_inverse_label_map_symbol(namespace, bucket)).data
        label_map.append(pd.DataFrame(np.nan, np.arange(len(label_map)), columns=[NAMESPACE_LABEL_TO_BUCKET_LABEL_COLUMN]))
        # If the label map was initialised before a later upsert, it won't necessarily map all the namespace labels
        # given. So we just append some nans (which represent absence from the column).
        internal_labels = np.array(label_map.iloc[namespace_labels, :].dropna()[NAMESPACE_LABEL_TO_BUCKET_LABEL_COLUMN])
        dimension = self._get_metadata(namespace).dimension
        if internal_labels.size > 0:
            # Since vectors are stored serially in long columns (no numpy array support yet), we have to work out
            # which rows we want by hand.
            internal_rows = np.hstack((np.arange(label*dimension, label*dimension+dimension)
                                       for label in internal_labels.astype(int)))
            vectors_serially = self._lib.read(
                namespace_bucket_vectors_symbol(namespace, bucket)
            ).data[internal_rows]
            vectors_pd = pd.DataFrame(np.reshape(vectors_serially, (vectors_serially.size // dimension, dimension)))
            vectors_pd.index = internal_labels
            return vectors_pd
        else:
            # If there's nothing in the bucket we just append a zero-row pd.DataFrame in read(); the concat will ignore
            # it.
            return pd.DataFrame(np.zeros((0, dimension)))

    def read_whole_bucket(
            self,
            namespace: str,
            bucket: int
    ) -> pd.DataFrame:
        """
        Reads a whole bucket. To rapidly retrieve data, it is most efficient to read whole buckets at once.

        Parameters
        ----------
        namespace: str
        bucket: int

        Returns
        -------
        pd.DataFrame
        """
        vectors_serially = self._lib.read(
            namespace_bucket_vectors_symbol(namespace, bucket)
        ).data
        dimension = self._get_metadata(namespace).dimension
        vectors_pd = pd.DataFrame(np.reshape(vectors_serially, (vectors_serially.size // dimension, dimension)))
        vectors_pd.index = self._lib.read(namespace_bucket_inverse_label_map_symbol(namespace, bucket)).data.dropna().index
        return vectors_pd

    def query(
            self,
            namespace: str,
            query_vectors: np.ndarray,
            k: int,
            nprobes: int
    ) -> List[List[Tuple[int, float]]]:
        """
        Searches the vector namespace for the top-k vectors closest to query_vectors. The indexing strategy will already
        have been set by create_vector_namespace.

        Parameters
        ----------
        namespace: str
            The namespace to search.
        query_vectors: np.ndarray
            The query vectors to search. Each element of the np.ndarray should be an individual query vector.
        k: int
            k in top-k.
        nprobes: int
            The number of buckets to search. If nprobes exceeds the number of buckets, we automatically reduce it.

        Returns
        -------
        Suppose we submit two query vectors with k=3. Our output will look like this:
        >>> [
        >>>         [(2, 0.2354...), (121, 34.2342...), (144, 239.9234...)],
        >>>         [(324, 0.1934), (341, 494.2341), (905.2349)]
        >>> ]
        where the ith entry corresponds to the top-k for the ith vector, and each tuple corresponds to a vector label
        and its distance.
        """
        if len(query_vectors.shape) != 2:
            raise Exception("PyVectorDB: Vectors must be presented in a two-dimensional np.ndarray.")
        query_bucketiser = self._get_query_bucketiser(namespace)
        if nprobes > query_bucketiser.index.ntotal:
            nprobes = query_bucketiser.index.ntotal
            # Alternatively, we could
            # raise PyVectorDBException(f"We can't query with more probes ({nprobes}) than live buckets "
            #                          f"({query_bucketiser.index.ntotal}). Reduce the number of probes.")

        metadata = self._get_metadata(namespace)
        dimension = metadata.dimension
        index = metadata.index
        if query_vectors.shape[1] != dimension:
            raise Exception(f"PyVectorDB: query vectors to namespace {namespace} must have length {dimension}")

        # We ignore distances, but we get an array whose ith member is an array of the buckets to search.
        _, buckets_to_search = query_bucketiser.search(query_vectors, nprobes)
        # We then work out, for each bucket, which vectors need to be searched.
        buckets_to_vectors = {bucket: [] for bucket in set(buckets_to_search.flatten())} # vectors to search in each bucket
        for i in range(query_vectors.shape[0]):
            for bucket in buckets_to_search[i]:
                buckets_to_vectors[bucket].append(i)

        # There are two cases. Either we have initialised a second-level within-bucket index, or we haven't.
        # If we have, then we serially search each index with the bucket. Experimentally this doesn't need to be batched
        # since faiss parallelises already.
        if index:
            results_by_bucket = {bucket: self._lib._nvs.version_store.search_bucket_with_index(
                namespace_bucket_index_symbol(namespace, bucket),
                _PythonVersionStoreVersionQuery(),
                np.concatenate([query_vectors[i] for i in vectors]),
                k
            ) for bucket, vectors in buckets_to_vectors.items()}
        else:
            # Otherwise, we need to batch, according to my preliminary results.
            vector_buckets, label_maps, vb_queries, lm_queries, query_vectors_flattened = [], [], [], [], []
            # At an earlier stage I thought it would be nice to have some versioning here. This does go through the
            # version map in cpp.
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
        # Results by bucket maps a bucket to two arrays. The first contains the distances of the top-k for each of the
        # vectors we searched within it - but unordered, since that's how Faiss returns them. The second contains the
        # labels.

        # Now we get the results by vector, from all the buckets.
        results_by_vector = {i: {"labels": [], "distances": []} for i in range(len(query_vectors))}
        for bucket, (distances, labels) in results_by_bucket.items():
            vectors = buckets_to_vectors[bucket]
            for i in range(len(distances) // k): # iterate over the number of vectors queried from each bucket.
                results_by_vector[vectors[i]]["labels"] += distances[i*k:(i+1)*k]
                results_by_vector[vectors[i]]["distances"] += labels[i*k:(i+1)*k]
        # So now we have the top-k per bucket. We then sort by distance and take the top-k from all the buckets.
        return [sorted(zip(res["labels"], res["distances"]), key=lambda x: x[1])[:k]
               for res in results_by_vector.values()]

    def get_largest_label(self, namespace: str):
        """
        Returns largest label from a bucket.

        Parameters
        ----------
        namespace: str

        Returns
        -------
        int
        """
        return self._get_metadata(namespace).largest_label

    def get_buckets(self, namespace: str) -> List[int]:
        """
        Returns a list of buckets.

        Parameters
        ----------
        namespace: str

        Returns
        -------
        List[int]
        """
        return self._get_metadata(namespace).bucket_sizes.keys()


class PyVectorDB(object):
    def __init__(
            self,
            uri: str,
            encoding_version: EncodingVersion = DEFAULT_ENCODING_VERSION
    ):
        """
        Initialises a PyVectorDB, analogous to an Arctic client.

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
        VectorLibrary.
        """
        return VectorLibrary(self._arctic_client.get_library(name, create_if_missing, library_options))
