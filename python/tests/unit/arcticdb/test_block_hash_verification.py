"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb import Arctic, LibraryOptions
from arcticdb.encoding_version import EncodingVersion
from arcticdb.exceptions import DataIntegrityException, ErrorCode, StorageException
from arcticdb.version_store.library import DataError
from arcticdb_ext.storage import KeyType
from arcticdb.util.test import assert_frame_equal, config_context
from tests.util.mark import WINDOWS

VERIFY_OPTION = "Codec.VerifyBlockHash"
NUM_ROWS = 1000
CORRUPTED_ROW = 500


def _random_frame():
    # Random float64s, so LZ4 stores them as literals and each value's bytes appear verbatim in data.mdb
    rng = np.random.default_rng(42)
    index = pd.date_range("2025-01-01", periods=NUM_ROWS, freq="s")
    return pd.DataFrame({"untouched": np.arange(NUM_ROWS), "payload": rng.normal(size=NUM_ROWS)}, index=index)


def _flip_stored_byte(lmdb_dir, pattern, not_preceded_by=b""):
    """Flips one bit of the only stored copy of `pattern`, as damage at rest would. Copies right after
    `not_preceded_by` are skipped (an LMDB key holding the same bytes)."""
    matches = []
    for data_file in lmdb_dir.rglob("data.mdb"):
        contents = bytearray(data_file.read_bytes())
        offset = contents.find(pattern)
        while offset >= 0:
            if not (not_preceded_by and contents[offset - len(not_preceded_by) : offset] == not_preceded_by):
                matches.append((data_file, contents, offset))
            offset = contents.find(pattern, offset + 1)
    assert len(matches) == 1, f"expected one stored copy of the pattern, found {len(matches)}"
    ((data_file, contents, offset),) = matches
    contents[offset] ^= 0x01
    data_file.write_bytes(contents)


@pytest.fixture(params=[EncodingVersion.V1, EncodingVersion.V2], ids=["V1", "V2"])
def corrupted_lmdb_library(request, tmp_path, lib_name):
    ac = Arctic(f"lmdb://{tmp_path}")
    lib = ac.create_library(lib_name, LibraryOptions(encoding_version=request.param))
    df = _random_frame()
    lib.write("sym", df)
    _flip_stored_byte(tmp_path, np.float64(df["payload"].iloc[CORRUPTED_ROW]).tobytes())
    yield lib, df
    del lib
    ac.delete_library(lib_name)


@pytest.mark.skipif(WINDOWS, reason="Rewrites data.mdb while LMDB has it mapped")
def test_corrupted_block_raises_with_verification_on(corrupted_lmdb_library):
    lib, _ = corrupted_lmdb_library
    with config_context(VERIFY_OPTION, 1):
        with pytest.raises(DataIntegrityException) as exc_info:
            lib.read("sym")
    assert isinstance(exc_info.value, StorageException)
    message = str(exc_info.value)
    assert message.startswith("E_DATA_INTEGRITY Block hash mismatch: stored 0x")
    assert "in block 0 of 1" in message
    assert "in column 'payload' (field 2)" in message  # field 0 is the index, field 1 "untouched"
    assert "symbol 'sym', version 0" in message
    assert "storage object name '*sTt*sym*0*" in message
    assert "If re-reading succeeds, the damage happened in transit" in message


@pytest.mark.skipif(WINDOWS, reason="Rewrites data.mdb while LMDB has it mapped")
def test_corrupted_block_is_a_data_integrity_error_in_read_batch(corrupted_lmdb_library):
    lib, _ = corrupted_lmdb_library
    with config_context(VERIFY_OPTION, 1):
        (result,) = lib.read_batch(["sym"])
    assert isinstance(result, DataError)
    assert result.error_code == ErrorCode.E_DATA_INTEGRITY


@pytest.mark.skipif(WINDOWS, reason="Rewrites data.mdb while LMDB has it mapped")
def test_corrupted_index_key_raises_data_integrity_not_missing_data(tmp_path, lib_name):
    ac = Arctic(f"lmdb://{tmp_path}")
    lib = ac.create_library(lib_name)
    try:
        lib.write("sym", _random_frame())
        (data_key,) = lib._nvs.library_tool().find_keys_for_id(KeyType.TABLE_DATA, "sym")
        # The index key stores each data key's content hash in a column. The data key's own LMDB key also holds it,
        # straight after its creation timestamp, so that copy is skipped.
        _flip_stored_byte(
            tmp_path,
            np.uint64(data_key.content_hash).tobytes(),
            not_preceded_by=np.uint64(data_key.creation_ts).tobytes(),
        )
        with config_context(VERIFY_OPTION, 1):
            with pytest.raises(DataIntegrityException, match="symbol 'sym', version 0"):
                lib.read("sym")
    finally:
        del lib
        ac.delete_library(lib_name)


@pytest.mark.skipif(WINDOWS, reason="Rewrites data.mdb while LMDB has it mapped")
def test_corrupted_block_returns_wrong_data_with_verification_off(corrupted_lmdb_library):
    lib, df = corrupted_lmdb_library
    result = lib.read("sym").data
    assert result["payload"].iloc[CORRUPTED_ROW] != df["payload"].iloc[CORRUPTED_ROW]
    assert_frame_equal(result.drop(index=result.index[CORRUPTED_ROW]), df.drop(index=df.index[CORRUPTED_ROW]))


def test_verification_on_reads_intact_data(lmdb_library):
    df = _random_frame().assign(text=[f"s{i % 7}" for i in range(NUM_ROWS)])
    lmdb_library.write("sym", df.iloc[:600])
    lmdb_library.append("sym", df.iloc[600:])
    with config_context(VERIFY_OPTION, 1):
        assert_frame_equal(lmdb_library.read("sym").data, df)
        assert_frame_equal(lmdb_library.read("sym", columns=["text"], row_range=(100, 700)).data, df[["text"]][100:700])
