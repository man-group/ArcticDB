// Copyright 2016 MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <string>

#include <bsoncxx/stdx/string_view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN

///
/// Represents an IEEE 754-2008 BSON Decimal128 value in a platform-independent way.
///
class BSONCXX_API decimal128 {
   public:
    ///
    /// Constructs a BSON Decimal128 value representing zero.
    ///
    decimal128() = default;

    ///
    /// Constructs a BSON Decimal128 from high and low 64-bit big-endian parts.
    ///
    /// @param high
    ///     The high 64-bits.
    /// @param low
    ///     The low 64-bits.
    ///
    decimal128(uint64_t high, uint64_t low) noexcept : _high(high), _low(low) {}

    ///
    /// Constructs a BSON Decimal128 from a string.
    ///
    /// @param str
    ///     A string representation of a decimal number.
    ///
    /// @throws bsoncxx::exception if the string isn't a valid BSON Decimal128
    /// representation.
    ///
    explicit decimal128(stdx::string_view str);

    ///
    /// Converts this decimal128 value to a string representation.
    ///
    /// @return A string representation of a IEEE 754-2008 decimal number.
    ///
    std::string to_string() const;

    ///
    /// @{
    ///
    /// Relational operators for decimal128
    ///
    /// @relates decimal128
    ///
    friend BSONCXX_API bool BSONCXX_CALL operator==(const decimal128& lhs, const decimal128& rhs);
    friend BSONCXX_API bool BSONCXX_CALL operator!=(const decimal128& lhs, const decimal128& rhs);
    ///
    /// @}
    ///

    ///
    /// Accessor for high 64 bits.
    ///
    BSONCXX_INLINE uint64_t high() const {
        return _high;
    }

    ///
    /// Accessor for low 64 bits.
    ///
    BSONCXX_INLINE uint64_t low() const {
        return _low;
    }

   private:
    uint64_t _high = 0;
    uint64_t _low = 0;
};

BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
