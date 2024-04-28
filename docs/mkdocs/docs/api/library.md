Library API
===========

This page documents the ``arcticdb.version_store.library`` module. This module is the main interface
exposing read/write functionality within a given Arctic instance.

The key functionality is exposed through ``arcticdb.version_store.library.Library`` instances. See the
[Arctic API](arctic.md) section for notes on how to create these. The other types exposed in this module
are used as part of the signature of ``arcticdb.version_store.library.Library`` instance methods, see the
[Library related objects](library_types.md) section for more details.

::: arcticdb.version_store.library.Library
    options:
      # init is not intended to be used directly
      filters: ["!^__init__$"]
