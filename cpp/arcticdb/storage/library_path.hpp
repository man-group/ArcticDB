/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/name_validation.hpp>

#include <boost/container/small_vector.hpp>
#include <fmt/format.h>
#include <memory>
#include <string>
#include <numeric>
#include <ranges>

namespace arcticdb::storage {

// Using StringViewable to make it easily pluggable with a custom internalized string class
class DefaultStringViewable : public std::shared_ptr<std::string> {
  public:
    using std::shared_ptr<std::string>::shared_ptr;

    template<class... Args>
    DefaultStringViewable(Args&&... args) :
        std::shared_ptr<std::string>::shared_ptr(std::make_shared<std::string>(args...)),
        hash_(arcticdb::hash(std::string_view{*this})) {}

    operator std::string_view() const { return *this->get(); }

    operator std::string() const { return *this->get(); }

    auto hash() const { return hash_; }

  private:
    HashedValue hash_;
};

inline bool operator==(const DefaultStringViewable& l, const DefaultStringViewable& r) {
    return static_cast<std::shared_ptr<std::string>>(l) == static_cast<std::shared_ptr<std::string>>(r) ||
           (l.hash() == r.hash() && std::string_view{l} == std::string_view{r});
}

} // namespace arcticdb::storage

// Formatters are defined here since they are used in implementations bellow.
namespace fmt {

template<>
struct formatter<arcticdb::storage::DefaultStringViewable> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::storage::DefaultStringViewable& dsv, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", std::string_view{dsv});
    }
};

} // namespace fmt

namespace std {

template<>
struct hash<arcticdb::storage::DefaultStringViewable> {

    inline arcticdb::HashedValue operator()(const arcticdb::storage::DefaultStringViewable& v) const noexcept {
        return v.hash();
    }
};

} // namespace std

namespace arcticdb::storage {

template<class StringViewable = DefaultStringViewable>
class LibraryPathImpl {
    static constexpr std::uint8_t NUM_LIBRARY_PARTS = 3;

  public:
    template<class S>
    LibraryPathImpl(std::initializer_list<S> values) : parts_(values.begin(), values.end()), hash_(compute_hash()) {}

    template<class StringViewableRange>
    LibraryPathImpl(const StringViewableRange& parts) : parts_(parts.begin(), parts.end()), hash_(compute_hash()) {}

    bool empty() const { return parts_.empty(); }

    LibraryPathImpl(std::string_view delim_path, char delim) : parts_(), hash_() {
        // We verify the library name contains valid symbols, isn't too long etc.
        verify_library_path(std::string(delim_path), delim);
        folly::StringPiece p{delim_path};
        while (!p.empty()) {
            auto part = p.split_step(delim);
            auto part_string = part.empty() ? "" : part.data();
            verify_library_path_part(part_string, delim);
            parts_.push_back(std::string_view{part_string, part.size()});
        }
        hash_ = compute_hash();
    }

    static LibraryPathImpl<StringViewable> from_delim_path(std::string_view delim_path, char delim = '.') {
        return LibraryPathImpl<StringViewable>{delim_path, delim};
    }

    std::string to_delim_path(char delim = '.') const {
        auto rg = as_range();
        auto delim_fold = [=](std::string a, DefaultStringViewable b) {
            return std::move(a) + delim + fmt::format("{}", b);
        };

        return std::accumulate(std::next(rg.begin()), rg.end(), fmt::format("{}", rg[0]), delim_fold);
    }

    auto as_range() const { return std::views::all(parts_); }

    auto hash() const { return hash_; }

  private:
    HashedValue compute_hash() {
        HashAccum accum;
        auto rg = as_range();
        std::for_each(rg.begin(), rg.end(), [&accum](auto& part) {
            auto h = part.hash();
            accum(&h);
        });
        return accum.digest();
    }

#ifndef DEBUG_BUILD
    boost::container::small_vector<StringViewable, NUM_LIBRARY_PARTS> parts_;
#else
    std::vector<StringViewable> parts_;
#endif
    HashedValue hash_;
};

template<class StringViewable = DefaultStringViewable>
inline bool operator==(const LibraryPathImpl<StringViewable>& l, const LibraryPathImpl<StringViewable>& r) {
    auto l_rg = l.as_range();
    auto r_rg = r.as_range();
    return l.hash() == r.hash() && std::equal(l_rg.begin(), l_rg.end(), r_rg.begin());
}

using LibraryPath = LibraryPathImpl<DefaultStringViewable>;

} // namespace arcticdb::storage

namespace fmt {

template<>
struct formatter<arcticdb::storage::LibraryPath> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::storage::LibraryPath& lib, FormatContext& ctx) const {
        auto out = ctx.out();
        fmt::format_to(out, "{}", lib.to_delim_path());

        return out;
    }
};

} // namespace fmt

namespace std {

template<class StringViewable>
struct hash<arcticdb::storage::LibraryPathImpl<StringViewable>> {
    inline arcticdb::HashedValue operator()(const arcticdb::storage::LibraryPathImpl<StringViewable>& v
    ) const noexcept {
        return v.hash();
    }
};

} // namespace std
