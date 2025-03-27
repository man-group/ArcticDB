#pragma once

/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <cstdint>
#include <cstddef>
#include <limits>
#include <algorithm>
#include <bit>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <vector>
#include <cstdint>
#include <stdexcept>

namespace arcticdb {

template <typename T>
void rle_compress(const T* input, std::size_t size,
                  std::vector<T>& dict,
                  std::vector<uint16_t>& runs)
{
    if (!input && size > 0) {
        throw std::invalid_argument("Invalid input pointer");
    }

    dict.clear();
    runs.clear();

    if (size == 0) return;

    dict.reserve(size);
    runs.reserve(size);

    T last_val = input[0];
    dict.push_back(last_val);
    uint16_t run_id = 0;
    runs.push_back(run_id);

    for (std::size_t i = 1; i < size; ++i) {
        T current_val = input[i];

        // Compare with previous value; if different, start a new run
        if (current_val != last_val) {
            run_id++;
            dict.push_back(current_val);
            last_val = current_val;
        }
        runs.push_back(run_id);
    }
}

template<typename T>
void rle_decompress(const std::vector<T> &dict,
                    const std::vector<uint16_t> &runs,
                    std::vector<T> &output) {
    if (dict.empty() && !runs.empty()) {
        throw std::invalid_argument("Empty dictionary with non-empty runs");
    }

    output.clear();

    if (runs.empty())
        return;

    output.reserve(runs.size());

    // Validate all run IDs before decompression
    for (uint16_t run_id : runs) {
        if (run_id >= dict.size()) {
            throw std::out_of_range("Run ID exceeds dictionary size");
        }
    }

    output.resize(runs.size());
    for (std::size_t i = 0; i < runs.size(); ++i) {
        output[i] = dict[runs[i]];
    }
}

} // namespace arcticdb