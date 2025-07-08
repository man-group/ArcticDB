/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/name_validation.hpp>

#include <memory>
#include <numeric>
 
using namespace arcticdb::storage;


LibraryPath::LibraryPath(std::string_view delim_path, char delim) :
    parts_(),
    hash_() {
    // We verify the library name contains valid symbols, isn't too long etc.
    verify_library_path(std::string(delim_path), delim);
    folly::StringPiece p{delim_path};
    while (!p.empty()) {
        auto part = p.split_step(delim);
        auto part_string = part.empty() ? "" : part.data();
        verify_library_path_part(part_string, delim);
        parts_.push_back(std::string{part_string, part.size()});
    }
    hash_ = compute_hash();
}

std::string LibraryPath::to_delim_path(char delim) const {
    auto rg = as_range();
    auto delim_fold = [=](std::string a, const std::string& b) {
        return std::move(a) + delim + b;
    };

    return std::accumulate(std::next(rg.begin()), rg.end(), rg[0], delim_fold);
}    

arcticdb::HashedValue LibraryPath::compute_hash() {
    HashAccum accum;
    auto rg = as_range();
    std::for_each(rg.begin(), rg.end(), [&accum](const auto &part) {
        auto h = arcticdb::hash(std::string_view{part});
        accum(&h);
    });
    return accum.digest();
}

bool operator==(const LibraryPath &l, const LibraryPath &r) {
    auto l_rg = l.as_range();
    auto r_rg = r.as_range();
    return l.hash() == r.hash() && std::equal(l_rg.begin(), l_rg.end(), r_rg.begin());
}

// Template constructor implementations
template<class S>
LibraryPath::LibraryPath(std::initializer_list<S> values):
    parts_(values.begin(), values.end()),
    hash_(compute_hash()) {
}

template<class StringRange>
LibraryPath::LibraryPath(const StringRange &parts) :
    parts_(parts.begin(), parts.end()),
    hash_(compute_hash()) {
}

// Explicit template instantiations for common types
template LibraryPath::LibraryPath(std::initializer_list<std::string>);
template LibraryPath::LibraryPath(std::initializer_list<const char*>);
template LibraryPath::LibraryPath(const std::vector<std::string>&);