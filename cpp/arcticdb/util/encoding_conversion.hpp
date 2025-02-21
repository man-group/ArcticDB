/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <iconv.h>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

class EncodingConversion {
    iconv_t iconv_;

  public:
    EncodingConversion(const char* to, const char* from) : iconv_(iconv_open(to, from)) {
        if (iconv_t(-1) == iconv_)
            util::raise_rte("error from iconv_open()");
    }

    ~EncodingConversion() {
        if (iconv_t(-1) != iconv_)
            iconv_close(iconv_);
    }

    bool convert(const char* input, size_t input_size, uint8_t* output, size_t& output_size) {
        return iconv(iconv_, (char**)&input, &input_size, (char**)&output, &output_size) != size_t(-1);
    }
};

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