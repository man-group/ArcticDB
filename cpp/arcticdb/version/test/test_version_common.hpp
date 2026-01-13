/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/test/stream_test_common.hpp>

using namespace arcticdb::storage;
using namespace arcticdb::stream;
using namespace arcticdb;

const uint64_t NumVersions = 10;
const uint64_t NumValues = 10;

inline std::vector<uint64_t> generate_version_values(uint64_t version) {
    std::vector<uint64_t> output;
    for (uint64_t i = 0; i < NumValues; ++i)
        output.push_back(version++ * 3);

    return output;
}
