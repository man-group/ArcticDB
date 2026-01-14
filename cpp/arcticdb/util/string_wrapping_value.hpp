/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string>
#include <string_view>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/constructors.hpp>

/*
 * CRTP class implementing boilerplate code for classes that are just a typed
 * wrapper around a string
 */

namespace arcticdb::util {

template<typename BaseType>
struct StringWrappingValue : BaseType {
    std::string value;
    // TODO might be nice to have view_or_value
    StringWrappingValue() = default;
    explicit StringWrappingValue(std::string_view s) : value(s) {}
    explicit StringWrappingValue(const std::string& s) : value(s) {}
    explicit StringWrappingValue(const char* c) : value(c) {}

    ARCTICDB_MOVE_COPY_DEFAULT(StringWrappingValue)

    friend bool operator==(const StringWrappingValue& l, const StringWrappingValue& r) { return l.value == r.value; }

    friend bool operator!=(const StringWrappingValue& l, const StringWrappingValue& r) { return !(l == r); }
};
} // namespace arcticdb::util

namespace std {
template<typename TagType>
struct hash<arcticdb::util::StringWrappingValue<TagType>> {
    std::size_t operator()(const arcticdb::util::StringWrappingValue<TagType>& s) const {
        return std::hash<std::string>()(s.value);
    }
};
} // namespace std

namespace fmt {

using namespace arcticdb::util;

template<typename BaseType>
struct formatter<StringWrappingValue<BaseType>> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const StringWrappingValue<BaseType>& srv, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", srv.value);
    }
};

} // namespace fmt

// TODO format stuff, integrate with defaultstringviewable
