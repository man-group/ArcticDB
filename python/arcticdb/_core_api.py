"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.

This module implements a backwards compatible version of msgpack functions.
"""

# Treat everything here as public APIs!!!! Any change made here may break downstream repos!!!!

from arcticdb_ext.storage import NativeVariantStorage
from arcticdb_ext.metrics.prometheus import MetricsConfig
from arcticdb.version_store._store import _env_config_from_lib_config as env_config_from_lib_config


def convert_native_variant_storage_to_py_tuple(native_cfg: NativeVariantStorage):
    return native_cfg.__getstate__()


def convert_metrics_config_to_py_tuple(metrics_cfg: MetricsConfig):
    return metrics_cfg.__getstate__()
