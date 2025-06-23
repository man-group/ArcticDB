/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma  once

#include <arcticdb/util/preconditions.hpp>

#include <cstdint>
#include <climits>

namespace arcticdb::util {
template<char a, char b, char c, char d>
struct MagicNum {
    static constexpr uint64_t Magic =
        a << (CHAR_BIT * 0) |
        b << (CHAR_BIT * 1) |
        c << (CHAR_BIT * 2) |
        d << (CHAR_BIT * 3);

    ~MagicNum() {
        magic_ = ~magic_;
    }

    // Set log_only to true if calling from destructors to avoid undefined behaviour of throwing
    void check(bool log_only = false) const {
        if (magic_ != Magic) {
            std::string_view expected(reinterpret_cast<const char*>(&Magic), 4);
            std::string message(fmt::format("Magic number failure, expected {}({}) got {}", Magic, expected, magic_));
            if (log_only) {
                log::version().warn(message);
            } else {
                util::raise_rte(message);
            }
        }
    }

  private:
    volatile uint64_t magic_ = Magic;
};

template<char a, char b>
struct SmallMagicNum {
    static constexpr uint16_t Magic =
        a << CHAR_BIT * 0 |
            b << CHAR_BIT * 1;

    ~SmallMagicNum() {
        magic_ = ~magic_;
    }

    [[nodiscard]] uint16_t magic() const { return magic_; }

    void check() const {
        std::string_view expected(reinterpret_cast<const char*>(&Magic), 2);
        util::check(magic_ == Magic, "Small magic number failure, expected {}({}) got {}", Magic, expected, magic_);
    }

private:
    volatile uint16_t magic_ = Magic;
};

template <typename MagicNumType>
void check_magic_in_place(const uint8_t*& pos) {
    const auto magic_num = reinterpret_cast<const MagicNumType*>(pos);
    magic_num->check();
}

template <typename MagicNumType>
void check_magic(const uint8_t*& pos) {
    check_magic_in_place<MagicNumType>(pos);
    pos += sizeof(MagicNumType);
}

template <typename MagicNumType>
void write_magic(uint8_t*& pos) {
    const auto magic_num = new(pos) MagicNumType;
    magic_num->check();
    pos += sizeof(MagicNumType);
}


} // namespace arcticdb
