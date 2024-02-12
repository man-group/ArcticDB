Library API
===========

This page documents the ``arcticdb.version_store.library`` module. This module is the main interface
exposing read/write functionality within a given Arctic instance.

The key functionality is exposed through ``arcticdb.version_store.library.Library`` instances. See the
Arctic API section for notes on how to create these. The other types exposed in this module are less
important and are used as part of the signature of ``arcticdb.version_store.library.Library`` instance
methods.


::: arcticdb.version_store.library.Library
    options:
      # init is not intended to be used directly
      filters: ["!^__init__$"]
