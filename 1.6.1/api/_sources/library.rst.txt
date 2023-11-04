Library API
===========

This page documents the ``arcticdb.version_store.library`` module. This module is the main interface
exposing read/write functionality within a given Arctic instance.

The key functionality is exposed through ``arcticdb.version_store.library.Library`` instances. See the
Arctic API section for notes on how to create these. The other types exposed in this module are less
important and are used as part of the signature of ``arcticdb.version_store.library.Library`` instance
methods.

.. autosummary::
    :toctree: library

    arcticdb.version_store.library.Library
    arcticdb.version_store.library.NormalizableType
    arcticdb.version_store.library.ArcticInvalidApiUsageException
    arcticdb.version_store.library.ArcticDuplicateSymbolsInBatchException
    arcticdb.version_store.library.ArcticUnsupportedDataTypeException
    arcticdb.version_store.library.SymbolVersion
    arcticdb.version_store.library.VersionInfo
    arcticdb.version_store.library.SymbolDescription
    arcticdb.version_store.library.WritePayload
    arcticdb.version_store.library.ReadRequest
    arcticdb.version_store.library.ReadInfoRequest

.. autoclass:: arcticdb.version_store.library.Library
    :special-members: __init__
    :members:

.. autoclass:: arcticdb.VersionedItem

.. autoclass:: arcticdb.DataError

.. autoclass:: arcticdb.VersionRequestType

.. automodule:: arcticdb.version_store.library
    :members: arcticdb.version_store.library.NormalizableType,arcticdb.version_store.library.ArcticInvalidApiUsageException,arcticdb.version_store.library.ArcticDuplicateSymbolsInBatchException,arcticdb.version_store.library.ArcticUnsupportedDataTypeException,arcticdb.version_store.library.SymbolVersion,arcticdb.version_store.library.VersionInfo,arcticdb.version_store.library.SymbolDescription,arcticdb.version_store.library.WritePayload,arcticdb.version_store.library.ReadRequest
