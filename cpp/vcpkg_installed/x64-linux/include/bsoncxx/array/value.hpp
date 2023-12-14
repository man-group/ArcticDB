// Copyright 2014 MongoDB Inc.
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

#include <cstdlib>
#include <memory>

#include <bsoncxx/array/view.hpp>
#include <bsoncxx/document/value.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace array {

///
/// A read-only BSON array that owns its underlying buffer. When a array::value goes
/// out of scope, the underlying buffer is freed. Generally this class should be used
/// sparingly; array::view should be used instead wherever possible.
///
class BSONCXX_API value {
   public:
    using deleter_type = void (*)(std::uint8_t*);
    using unique_ptr_type = std::unique_ptr<uint8_t[], deleter_type>;

    ///
    /// Constructs a value from a buffer.
    /// This constructor transfers ownership of the buffer to the resulting
    /// value. A user-provided deleter is used to destroy the buffer.
    ///
    /// @param data
    ///   A pointer to a buffer containing a valid BSON array.
    /// @param length
    ///   The length of the document.
    /// @param dtor
    ///   A user provided deleter.
    ///
    value(std::uint8_t* data, std::size_t length, deleter_type dtor);

    ///
    /// Constructs a value from a std::unique_ptr to a buffer. The ownership
    /// of the buffer is transferred to the resulting value.
    ///
    /// @param ptr
    ///   A pointer to a buffer containing a valid BSON array.
    /// @param length
    ///   The length of the document.
    ///
    value(unique_ptr_type ptr, std::size_t length);

    ///
    /// Constructs a value from a view of an array. The data referenced
    /// by the array::view will be copied into a new buffer managed by the
    /// constructed value.
    ///
    /// @param view
    ///   A view of another array to copy.
    ///
    explicit value(array::view view);

    value(const value&);
    value& operator=(const value&);

    value(value&&) noexcept = default;
    value& operator=(value&&) noexcept = default;

    ///
    /// Get a view over the document owned by this value.
    ///
    BSONCXX_INLINE array::view view() const noexcept;

    ///
    /// Conversion operator that provides a view given a value.
    ///
    /// @return A view over the value.
    ///
    BSONCXX_INLINE operator array::view() const noexcept;

    ///
    /// Transfer ownership of the underlying buffer to the caller.
    ///
    /// @warning
    ///   After calling release() it is illegal to call any methods
    ///   on this class, unless it is subsequently moved into.
    ///
    /// @return A std::unique_ptr with ownership of the buffer.
    ///
    unique_ptr_type release();

   private:
    unique_ptr_type _data;
    std::size_t _length{0};
};

BSONCXX_INLINE array::view value::view() const noexcept {
    return array::view{static_cast<uint8_t*>(_data.get()), _length};
}

BSONCXX_INLINE value::operator array::view() const noexcept {
    return view();
}

}  // namespace array
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
