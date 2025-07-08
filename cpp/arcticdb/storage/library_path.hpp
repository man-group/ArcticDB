/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <folly/Range.h>
#include <boost/container/small_vector.hpp>
#include <fmt/format.h>
#include <string>
#include <arcticdb/util/hash.hpp>

// forward declarations
namespace arcticdb {
    class HashAccum;
}

namespace arcticdb::storage {

class LibraryPath {
    static constexpr std::uint8_t NUM_LIBRARY_PARTS = 3;
public:
    template<class S>
    LibraryPath(std::initializer_list<S> values);

    template<class StringRange>
    LibraryPath(const StringRange &parts);

    bool empty() const {
        return parts_.empty();
    }

    LibraryPath(std::string_view delim_path, char delim);

    auto as_range() const {
        return folly::range(parts_.cbegin(), parts_.cend());
    }

    auto hash() const { return hash_; }

    static LibraryPath from_delim_path(std::string_view delim_path, char delim = '.') {
        return LibraryPath{delim_path, delim};
    }

    std::string to_delim_path(char delim = '.') const;  

private:
    HashedValue compute_hash();

#ifndef DEBUG_BUILD
    boost::container::small_vector<std::string, NUM_LIBRARY_PARTS> parts_;
#else
    std::vector<std::string> parts_;
#endif
    HashedValue hash_;
};

bool operator==(const LibraryPath &l, const LibraryPath &r);

} //namespace arcticdb::storage

namespace fmt {

template<>
struct formatter<arcticdb::storage::LibraryPath> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::storage::LibraryPath &lib, FormatContext &ctx) const {
        auto out = ctx.out();
        fmt::format_to(out, "{}", lib.to_delim_path());

        return out;
    }
};

}

namespace std {

template<>
struct hash<arcticdb::storage::LibraryPath> {
    inline arcticdb::HashedValue operator()(const arcticdb::storage::LibraryPath &v) const noexcept {
        return v.hash();
    }
};

}