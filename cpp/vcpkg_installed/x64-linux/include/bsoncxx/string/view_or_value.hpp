// Copyright 2015 MongoDB Inc.
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

#include <string>

#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/view_or_value.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace string {

///
/// Class representing a view-or-value variant type for strings.
///
/// This class adds several string-specific methods to the bsoncxx::view_or_value template:
/// - a constructor overload for const char*
/// - a constructor overload for std::string by l-value reference
/// - a safe c_str() operation to return null-terminated c-style strings.
///
class BSONCXX_API view_or_value : public bsoncxx::view_or_value<stdx::string_view, std::string> {
   public:
    ///
    /// Forward all bsoncxx::view_or_value constructors.
    ///
    using bsoncxx::view_or_value<stdx::string_view, std::string>::view_or_value;

    ///
    /// Default constructor, equivalent to using an empty string.
    ///
    BSONCXX_INLINE view_or_value() = default;

    ///
    /// Construct a string::view_or_value using a null-terminated const char *.
    /// The resulting view_or_value will keep a string_view of 'str', so it is
    /// important that the passed-in string outlive this object.
    ///
    /// @param str A null-terminated string
    ///
    BSONCXX_INLINE view_or_value(const char* str)
        : bsoncxx::view_or_value<stdx::string_view, std::string>(stdx::string_view(str)) {}

    ///
    /// Allow construction with an l-value reference to a std::string. The resulting
    /// view_or_value will keep a string_view of 'str', so it is important that the
    /// passed-in string outlive this object.
    ///
    /// Construction calls passing a std::string by r-value reference will use the
    /// constructor defined in the parent view_or_value class.
    ///
    /// @param str A std::string l-value reference.
    ///
    BSONCXX_INLINE view_or_value(const std::string& str)
        : bsoncxx::view_or_value<stdx::string_view, std::string>(stdx::string_view(str)) {}

    ///
    /// Return a string_view_or_value that is guaranteed to hold a null-terminated
    /// string. The lifetime of the returned object must be a subset of this object's
    /// lifetime, because the new view_or_value might hold a view into this one.
    ///
    /// It is recommended that this method be used before calling .data() on a
    /// view_or_value, as that method may return a non-null-terminated string.
    ///
    /// @return A new view_or_value object.
    ///
    view_or_value terminated() const;

    ///
    /// Call data() on this view_or_value's string_view. This method is not
    /// guaranteed to return a null-terminated string unless it is used in
    /// combination with terminated().
    ///
    /// @return A const char* of this string.
    ///
    const char* data() const;
};

///
/// @{
///
/// Comparison operators for comparing string::view_or_value directly with const char *.
///
/// @relates view_or_value
///
BSONCXX_INLINE bool operator==(const view_or_value& lhs, const char* rhs) {
    return lhs.view() == stdx::string_view(rhs);
}

BSONCXX_INLINE bool operator!=(const view_or_value& lhs, const char* rhs) {
    return !(lhs == rhs);
}

BSONCXX_INLINE bool operator==(const char* lhs, const view_or_value& rhs) {
    return rhs == lhs;
}

BSONCXX_INLINE bool operator!=(const char* lhs, const view_or_value& rhs) {
    return !(rhs == lhs);
}
///
/// @}
///

}  // namespace string
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
