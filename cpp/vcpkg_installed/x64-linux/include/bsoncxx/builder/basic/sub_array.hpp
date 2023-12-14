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

#include <bsoncxx/builder/basic/helpers.hpp>
#include <bsoncxx/builder/concatenate.hpp>
#include <bsoncxx/builder/core.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace basic {

namespace impl {

template <typename T>
void value_append(core* core, T&& t);

}  // namespace impl

///
/// An internal class of builder::basic.
/// Users should almost always construct a builder::basic::array instead.
///
class sub_array {
   public:
    ///
    /// Default constructor
    ///
    BSONCXX_INLINE sub_array(core* core) : _core(core) {}

    ///
    /// Appends multiple BSON values.
    ///
    template <typename Arg, typename... Args>
    BSONCXX_INLINE void append(Arg&& a, Args&&... args) {
        append_(std::forward<Arg>(a));
        append(std::forward<Args>(args)...);
    }

    ///
    /// Inductive base-case for the variadic append(...)
    ///
    BSONCXX_INLINE
    void append() {}

   private:
    //
    // Appends a BSON value.
    //
    template <typename T>
    BSONCXX_INLINE void append_(T&& t) {
        impl::value_append(_core, std::forward<T>(t));
    }

    //
    // Concatenates another bson array directly.
    //
    BSONCXX_INLINE
    void append_(concatenate_array array) {
        _core->concatenate(array.view());
    }

    core* _core;
};

}  // namespace basic
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
