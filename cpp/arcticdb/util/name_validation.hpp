/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

class Store;

// Verifies whether a symbol_key is valid and raises UserInputException exceptions on invalid symbol names.
// Should be used only when writing new symbols to allow for backwards compatibility with old symbols.
void verify_symbol_key(const entity::StreamId &symbol_key);

// Does strict checks on library names and raises UserInputException if it encounters an error.
// Should be checked only when writing new libraries to allow for backwards compatibility
// with old invalid libraries.
void verify_library_path_on_write(const Store* store, const entity::StringId& library_path);

// These two do relaxed checks which should always be run on each library operation (including
// already existing libraries). These raise friendly error messages instead of segfaulting or
// raising an obscure internal error.
void verify_library_path(const entity::StringId& library_path, char delim);

void verify_library_path_part(const std::string& library_part, char delim);

}