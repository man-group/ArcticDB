#pragma once

#include <cstddef>
#include <concepts>

namespace arcticdb {

struct DecompressResult {
    size_t compressed_;
    size_t uncompressed_;
};

} // namespace arcticdb