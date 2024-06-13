/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <cstdint>

namespace arcticdb {

    template <typename T>
    struct NullEncoding {


        std::optional<size_t> max_required_bytes(const T *, size_t num_rows) {
            return num_rows * sizeof(T);
        }

        size_t encode(const T *data_in, size_t num_rows, uint8_t *data_out) {
            if (num_rows == 0)
                return 0;

            memcpy(data_out, data_in, num_rows * sizeof(T));
            return num_rows * sizeof(T);
        }

        size_t decode(const uint8_t *data_in, size_t bytes, T *data_out) {
            if (bytes == 0)
                return 0;

            memcpy(data_out, data_in, bytes);
            return bytes;
        }
    };
}