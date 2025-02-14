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

#include <bsoncxx/builder/basic/sub_array.hpp>
#include <bsoncxx/builder/basic/sub_document.hpp>
#include <bsoncxx/stdx/type_traits.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace builder {
namespace basic {
namespace impl {

template <typename T>
BSONCXX_INLINE detail::requires_t<void, detail::is_invocable<T, sub_document>>  //
generic_append(core* core, T&& func) {
    core->open_document();
    detail::invoke(std::forward<T>(func), sub_document(core));
    core->close_document();
}

template <typename T, typename Placeholder = void>  // placeholder 'void' for VS2015 compat
BSONCXX_INLINE detail::requires_t<void, detail::is_invocable<T, sub_array>>  //
generic_append(core* core, T&& func) {
    core->open_array();
    detail::invoke(std::forward<T>(func), sub_array(core));
    core->close_array();
}

template <typename T, typename = void, typename = void>
BSONCXX_INLINE detail::requires_not_t<void,  //
                                      detail::is_invocable<T, sub_document>,
                                      detail::is_invocable<T, sub_array>>
generic_append(core* core, T&& t) {
    core->append(std::forward<T>(t));
}

template <typename T>
BSONCXX_INLINE void value_append(core* core, T&& t) {
    generic_append(core, std::forward<T>(t));
}

}  // namespace impl
}  // namespace basic
}  // namespace builder
}  // namespace v_noabi
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
