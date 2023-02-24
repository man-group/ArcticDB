/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/stream/test/stream_test_common.hpp>

using namespace arcticdb::storage;
using namespace arcticdb::stream;
using namespace arcticdb;
namespace fg = folly::gen;

const uint64_t NumVersions = 10;
const uint64_t NumValues = 10;

inline std::vector<uint64_t> generate_version_values(uint64_t version) {
    std::vector<uint64_t> output;
    for (uint64_t i = 0; i < NumValues; ++i)
        output.push_back(version++ * 3);

    return output;
}

