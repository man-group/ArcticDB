ArcticDB Python Documentation
=============================

Introduction
------------

This part of the site documents the Python API of ArcticDB.

The API is structured into the following components:

* [**Arctic**](arctic.md): Arctic is the primary API used for accessing and manipulating ArcticDB libraries.
* [**Library**](library.md): The Library API enables reading and manipulating symbols inside ArcticDB libraries.
* [**Query Builder**](query_builder.md): The QueryBuilder API enables the specification of complex queries, utilised in the Library API.

Most of the code snippets in the API docs require importing `arcticdb` as `adb`:

```python
import arcticdb as adb
```