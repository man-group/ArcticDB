"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pickle
import pytest
import sys
from typing import Union

from arcticdb_ext.version_store import AtomKey
from arcticdb_ext.storage import KeyType


def test_basic_pickle_roundtrip():
    original_key = AtomKey("test_symbol", 42, 1, 0, 1, 2, KeyType.TABLE_DATA)

    pickled_data = pickle.dumps(original_key)
    unpickled_key = pickle.loads(pickled_data)

    assert original_key.id == unpickled_key.id
    assert original_key.version_id == unpickled_key.version_id
    assert original_key.creation_ts == unpickled_key.creation_ts
    assert original_key.content_hash == unpickled_key.content_hash
    assert original_key.start_index == unpickled_key.start_index
    assert original_key.end_index == unpickled_key.end_index
    assert original_key.type == unpickled_key.type

    assert original_key == unpickled_key


@pytest.mark.parametrize(
    "stream_id",
    [
        0,
        1,
        2**31 - 1,
        -(2**31),
        2**63 - 1,
        -(2**63),
        "",
        "test_symbol",
        "long_string" * 100,
        2**32,
        2**63,
        2**64 - 1,
    ],
)
def test_stream_id_variants(stream_id: Union[int, str]):
    key = AtomKey(stream_id, 1, 1, 0, 0, 100, KeyType.TABLE_DATA)

    pickled_data = pickle.dumps(key)
    unpickled_key = pickle.loads(pickled_data)

    assert key == unpickled_key
    assert key.id == unpickled_key.id


@pytest.mark.parametrize(
    "version_id",
    [
        0,
        1,
        2**32 - 1,
        2**64 - 1,
    ],
)
def test_version_id_variants(version_id: int):
    key = AtomKey("test_symbol", version_id, -1, 4, 0, "hi", KeyType.VERSION)

    pickled_data = pickle.dumps(key)
    unpickled_key = pickle.loads(pickled_data)

    assert key == unpickled_key
    assert key.version_id == unpickled_key.version_id


@pytest.mark.parametrize(
    "timestamp",
    [
        0,
        1,
        -1,
        2**63 - 1,
        -(2**63),
    ],
)
def test_creation_timestamp_variants(timestamp: int):
    key = AtomKey("test_symbol", 1, timestamp, 1, 0, 100, KeyType.SNAPSHOT_REF)

    pickled_data = pickle.dumps(key)
    unpickled_key = pickle.loads(pickled_data)

    assert key == unpickled_key
    assert key.creation_ts == unpickled_key.creation_ts


@pytest.mark.parametrize("content_hash", [0, 1, 2**32 - 1, 2**64 - 1])
def test_content_hash_variants(content_hash: int):
    key = AtomKey("test_symbol", 1, 0, content_hash, 0, 100, KeyType.SNAPSHOT_REF)

    pickled_data = pickle.dumps(key)
    unpickled_key = pickle.loads(pickled_data)

    assert key == unpickled_key
    assert key.content_hash == unpickled_key.content_hash


@pytest.mark.parametrize(
    "start_index,end_index",
    [(0, 0), (-(2**63 - 1), 2**64 - 1), (-(2**63 - 1), "test"), (2**63 - 1, 2**64 - 1), ("test", 0), ("test", "test")],
)
def test_index_value_variants(start_index: Union[int, str], end_index: Union[int, str]):
    key = AtomKey("test_symbol", 1, 1, 0, start_index, end_index, KeyType.MULTI_KEY)

    pickled_data = pickle.dumps(key)
    unpickled_key = pickle.loads(pickled_data)

    assert key == unpickled_key
    assert key.start_index == unpickled_key.start_index
    assert key.end_index == unpickled_key.end_index


@pytest.mark.parametrize("key_type_name", list(KeyType.__entries.keys()))
def test_key_type_variants(key_type_name):
    key_type = getattr(KeyType, key_type_name)
    key = AtomKey("test_symbol", 1, 1, 0, 1, 2, key_type)
    pickled_data = pickle.dumps(key)
    unpickled_key = pickle.loads(pickled_data)

    assert key == unpickled_key
    assert key.type == unpickled_key.type


def test_pickle_protocol_versions():
    key = AtomKey("test_symbol", 42, 1, 1, 1, 2, KeyType.VERSION)

    for protocol in [2, 3, 4, 5]:
        pickled_data = pickle.dumps(key, protocol=protocol)
        unpickled_key = pickle.loads(pickled_data)
        assert key == unpickled_key
