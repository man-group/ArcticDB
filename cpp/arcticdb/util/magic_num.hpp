/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <climits>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::util {
template<char a, char b, char c, char d>
struct MagicNum {
    static constexpr uint64_t Magic = a << CHAR_BIT * 0 | b << CHAR_BIT * 1 | c << CHAR_BIT * 2 | d << CHAR_BIT * 3;

    MagicNum()
        : magic_(Magic)
    {
    }

    //uint64_t magic() const { return magic_; }

    ~MagicNum()
    {
        magic_ = ~magic_;
    }

    void check() const
    {
        util::check(magic_ == Magic, "Magic number failure, expected {} got {}", Magic, magic_);
    }

private:
    volatile uint64_t magic_;
};

template<char a, char b>
struct SmallMagicNum {
    static constexpr uint16_t Magic = a << CHAR_BIT * 0 | b << CHAR_BIT * 1;

    SmallMagicNum()
        : magic_(Magic)
    {
    }

    ~SmallMagicNum()
    {
        magic_ = ~magic_;
    }

    [[nodiscard]] uint16_t magic() const
    {
        return magic_;
    }

    void check() const
    {
        util::check(magic_ == Magic, "Magic number failure, expected {} got {}", Magic, magic_);
    }

private:
    volatile uint16_t magic_;
};

template<typename MagicNumType>
void check_magic(const uint8_t*& pos)
{
    const auto magic_num = reinterpret_cast<const MagicNumType*>(pos);
    magic_num->check();
    pos += sizeof(MagicNumType);
}

template<typename MagicNumType>
void write_magic(uint8_t*& pos)
{
    const auto magic_num = new (pos) MagicNumType;
    magic_num->check();
    pos += sizeof(MagicNumType);
}

} // namespace arcticdb::util
