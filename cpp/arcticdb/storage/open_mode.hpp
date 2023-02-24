/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#ifdef _WIN32
//Windows #defines DELETE in winnt.h
#undef DELETE
#endif

#include <cstdint>
#include <fmt/format.h>

namespace arcticdb::storage {

enum class OpenMode : std::uint8_t {
    READ = 1,
    WRITE = 3, // implies READ
    DELETE = 7 // implies READ + WRITE
};

inline bool operator<(const OpenMode l, const OpenMode r) {
    return static_cast<std::uint8_t>(l) < static_cast<std::uint8_t>(r);
}
inline bool operator>(const OpenMode l, const OpenMode r) {
    return static_cast<std::uint8_t>(l) > static_cast<std::uint8_t>(r);
}
inline bool operator==(const OpenMode l, const OpenMode r) {
    return static_cast<std::uint8_t>(l) == static_cast<std::uint8_t>(r);
}
inline bool operator!=(const OpenMode l, const OpenMode r) {
    return static_cast<std::uint8_t>(l) != static_cast<std::uint8_t>(r);
}
inline bool operator<=(const OpenMode l, const OpenMode r) {
    return static_cast<std::uint8_t>(l) <= static_cast<std::uint8_t>(r);
}
inline bool operator>=(const OpenMode l, const OpenMode r) {
    return static_cast<std::uint8_t>(l) >= static_cast<std::uint8_t>(r);
}

}

namespace fmt {
template<>
struct formatter<arcticdb::storage::OpenMode> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(arcticdb::storage::OpenMode mode, FormatContext &ctx) const {
        char c = 'X';
        switch (mode) {
            case arcticdb::storage::OpenMode::READ:c = 'r';
                break;
            case arcticdb::storage::OpenMode::WRITE:c = 'w';
                break;
            case arcticdb::storage::OpenMode::DELETE:c = 'd';
                break;

        }
        return format_to(ctx.out(), "{:c}", c);
    }
};

}

