/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstring>

namespace arcticdb {

class PortableEncodingConversion {
  public:
    PortableEncodingConversion(const char*, const char*) {}

    static bool convert(const char* input, size_t input_size, uint8_t* output, const size_t& output_size) {
        memset(output, 0, output_size);
        auto pos = output;
        for (auto c = 0u; c < input_size; ++c) {
            *pos = *input++;
            pos += UNICODE_WIDTH;
        }
        return true;
    }
};

} // namespace arcticdb
