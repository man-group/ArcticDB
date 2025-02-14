// Copyright 2023 MongoDB Inc.
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

#include <mongocxx/options/range-fwd.hpp>

#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace options {

///
/// @brief `RangeOpts` specifies index options for a Queryable Encryption field supporting
/// "rangePreview" queries.
///
/// @note @ref min, @ref max, @ref sparsity, and @ref precision must match the values set in the
/// encryptedFields of the destination collection.
///
/// @note For double and decimal128, @ref min, @ref max, and @ref precision must all be set, or all
/// be unset.
///
/// @warning The Range algorithm is experimental only. It is not intended for public use. It is
/// subject to breaking changes.
class range {
   public:
    /// @brief Sets `RangeOpts.min`.
    /// @note Required if @ref precision is set.
    range& min(bsoncxx::v_noabi::types::bson_value::view_or_value value);

    /// @brief Gets `RangeOpts.min`.
    /// @note Required if @ref precision is set.
    const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value>& min() const;

    /// @brief Sets `RangeOpts.max`.
    /// @note Required if @ref precision is set.
    range& max(bsoncxx::v_noabi::types::bson_value::view_or_value value);

    /// @brief Gets `RangeOpts.max`.
    /// @note Required if @ref precision is set.
    const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value>& max() const;

    /// @brief Sets `RangeOpts.sparsity`.
    range& sparsity(std::int64_t value);

    /// @brief Gets `RangeOpts.sparsity`.
    const stdx::optional<std::int64_t>& sparsity() const;

    /// @brief Sets `RangeOpts.precision`.
    /// @note May only be set for `double` or `decimal128`.
    range& precision(std::int32_t value);

    /// @brief Gets `RangeOpts.precision`.
    /// @note May only be set for `double` or `decimal128`.
    const stdx::optional<std::int32_t>& precision() const;

   private:
    stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value> _min;
    stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value> _max;
    stdx::optional<std::int64_t> _sparsity;
    stdx::optional<std::int32_t> _precision;
};

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

// CXX-2770: missing include of postlude header.
#if defined(MONGOCXX_TEST_MACRO_GUARDS_FIX_MISSING_POSTLUDE)
#include <mongocxx/config/postlude.hpp>
#endif
