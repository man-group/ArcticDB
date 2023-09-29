from typing import Union, Any, Optional, Dict
import faiss
import numpy as np


class PyVectorDBException(Exception):
    pass


class PyVectorDBNotYetImplemented(PyVectorDBException):
    pass


class PyVectorDBInvalidArgument(PyVectorDBException):
    pass


def type_error(param: Any, param_name: str, required_type: type):
    return PyVectorDBInvalidArgument(f"Attempted to pass {type(param).__name__} as {param_name}, which should have "
                                     f"been a {required_type.__name__}.")


VECTOR_NORMALISABLE_TYPES = (np.ndarray)
NormalisableVectors = Union[VECTOR_NORMALISABLE_TYPES]
NAMESPACE_LABEL_TO_BUCKET_LABEL_COLUMN = "in_bucket"


def get_metric(metric: str) -> int:  # faiss metrics are ints
    if metric == "L2":
        return faiss.METRIC_L2
    elif metric == "IP":
        return faiss.METRIC_INNER_PRODUCT
    else:
        raise PyVectorDBNotYetImplemented("Metrics other than L2 and IP (inner product) are not supported.")


def namespace_metadata_symbol(namespace: str) -> str:
    return f"{namespace}_metadata"


def namespace_bucket_vectors_symbol(namespace: str, bucket: int) -> str:
    return f"{namespace}_bucket_{bucket}_vectors"


def namespace_bucket_index_symbol(namespace: str, bucket: int) -> str:
    return f"{namespace}_bucket_{bucket}_index"


def namespace_bucket_label_map_symbol(namespace: str, bucket: int) -> str:
    """
    When searching without an index, we get labels relative to the ordering of the vectors in the bucket. This symbol
    maps to the original.
    """
    return f"{namespace}_bucket_{bucket}_label_map"


def namespace_bucket_inverse_label_map_symbol(namespace: str, bucket: int) -> str:
    """
    Reading individual vectors is pretty inefficient. (Reading them as a whole is perfectly fine: just serially read
    each bucket or as many buckets as will fit in memory.) We need an inverse map from the equivalent above, so that
    if we want the kth vector overall, we know to get the k'th vector in a bucket.
    """
    return f"{namespace}_bucket_{bucket}_inverse_label_map"


def namespace_vectors_to_bucket_symbol(namespace: str) -> str:
    return f"{namespace}_vectors_to_buckets"


def namespace_insertion_bucketiser_symbol(namespace: str) -> str:
    return f"{namespace}_insertion_bucketiser"


def namespace_query_bucketiser_symbol(namespace: str) -> str:
    return f"{namespace}_query_bucketiser"


def namespace_centroids_symbol(namespace: str) -> str:
    return f"{namespace}_centroids"


class NamespaceMetadata(object):
    """
    Utility class for namespace metadata.
    """

    def __init__(
            self,
            dimension: int,
            metric: str,
            insertion_bucketiser: str,
            buckets: int,
            index: Optional[str],
            bucket_sizes: Dict[int, int],
            dynamic_rebucketise: bool,
            largest_label: int
    ):
        if isinstance(dimension, int):
            self.dimension = dimension
        else:
            raise type_error(dimension, "dimension", int)
        self.metric = get_metric(metric)  # get_metric validates
        if insertion_bucketiser.startswith("IDMap"):
            self.insertion_bucketiser = insertion_bucketiser
        else:
            raise PyVectorDBInvalidArgument(f"{insertion_bucketiser} is not a valid bucketiser. Valid bucketisers must "
                                            f"start with 'IDMap'.")
        if isinstance(buckets, int):
            self.buckets = buckets
        else:
            raise type_error(buckets, "buckets", int)
        if isinstance(index, str) or index is None:
            self.index = index
        else:
            raise type_error(index, "index", str)
        if isinstance(bucket_sizes, dict):
            self.bucket_sizes = bucket_sizes
        else:
            raise type_error(bucket_sizes, "bucket_sizes", dict)
        if isinstance(dynamic_rebucketise, bool):
            self.dynamic_rebucketise = dynamic_rebucketise
        else:
            raise type_error(dynamic_rebucketise, "dynamic_rebucketise", bool)
        if isinstance(largest_label, int):
            self.largest_label = largest_label
        else:
            raise type_error(largest_label, "largest_label", int)

DELETED_BUCKET = -1