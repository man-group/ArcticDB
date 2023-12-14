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

#include <bsoncxx/array/value.hpp>
#include <bsoncxx/array/view.hpp>
#include <bsoncxx/builder/basic/impl.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/sub_array.hpp>
#include <bsoncxx/builder/core.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace basic {

///
/// A traditional builder-style interface for constructing
/// a BSON array.
///
class array : public sub_array {
   public:
    ///
    /// Default constructor
    ///
    BSONCXX_INLINE array() : sub_array(&_core), _core(true) {}

    ///
    /// Move constructor
    ///
    BSONCXX_INLINE array(array&& arr) noexcept : sub_array(&_core), _core(std::move(arr._core)) {}

    ///
    /// Move assignment operator
    ///
    BSONCXX_INLINE array& operator=(array&& arr) noexcept {
        _core = std::move(arr._core);
        return *this;
    }

    ///
    /// @return A view of the BSON array.
    ///
    BSONCXX_INLINE bsoncxx::array::view view() const {
        return _core.view_array();
    }

    ///
    /// Conversion operator that provides a view of the current builder
    /// contents.
    ///
    /// @return A view of the current builder contents.
    ///
    BSONCXX_INLINE operator bsoncxx::array::view() const {
        return view();
    }

    ///
    /// Transfer ownership of the underlying array to the caller.
    ///
    /// @return An array::value with ownership of the array.
    ///
    /// @warning
    ///  After calling extract() it is illegal to call any methods
    ///  on this class, unless it is subsequenly moved into.
    ///
    BSONCXX_INLINE bsoncxx::array::value extract() {
        return _core.extract_array();
    }

    ///
    /// Reset the underlying BSON to an empty array.
    ///
    BSONCXX_INLINE void clear() {
        _core.clear();
    }

   private:
    core _core;
};

///
/// Creates an array from a list of elements.
///
/// @param args
///   A variadiac list of elements. The types of the elements can be anything that
///   builder::basic::sub_array::append accepts.
///
/// @return
///   A bsoncxx::array::value containing the elements.
///
template <typename... Args>
bsoncxx::array::value BSONCXX_CALL make_array(Args&&... args) {
    basic::array array;
    array.append(std::forward<Args>(args)...);

    return array.extract();
}

}  // namespace basic
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
