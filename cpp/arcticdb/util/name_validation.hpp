/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/util/error_code.hpp>

namespace arcticdb {

// entity/serialized_key.hpp expects the symbol to be <255 chars
constexpr size_t MAX_SYMBOL_LENGTH = std::numeric_limits<uint8_t>::max() - 1;

// Verifies whether a symbol_key is valid and raises UserInputException exceptions on invalid symbol names.
// Should be used only when writing new symbols to allow for backwards compatibility with old symbols.
[[nodiscard]] CheckOutcome verify_symbol_key(const StreamId &symbol_key);

// Similar to verify_symbol_key above.
[[nodiscard]] CheckOutcome verify_snapshot_id(const SnapshotId& snapshot_id);

// Does strict checks on library names and raises UserInputException if it encounters an error.
// Should be checked only when writing new libraries to allow for backwards compatibility
// with old invalid libraries.
void verify_library_path_on_write(const Store* store, const StringId& library_path);

// These two do relaxed checks which should always be run on each library operation (including
// already existing libraries). These raise friendly error messages instead of segfaulting or
// raising an obscure internal error.
void verify_library_path(const StringId& library_path, char delim);

void verify_library_path_part(const std::string& library_part, char delim);

}