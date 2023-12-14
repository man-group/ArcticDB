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
#include <bsoncxx/util/functor.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace basic {
namespace impl {

template <typename T>
using takes_document = typename util::is_functor<T, void(sub_document)>;

template <typename T>
using takes_array = typename util::is_functor<T, void(sub_array)>;

template <typename T>
BSONCXX_INLINE typename std::enable_if<takes_document<T>::value, void>::type generic_append(
    core* core, T&& func) {
    core->open_document();
    func(sub_document(core));
    core->close_document();
}

template <typename T>
BSONCXX_INLINE typename std::enable_if<takes_array<T>::value, void>::type generic_append(core* core,
                                                                                         T&& func) {
    core->open_array();
    func(sub_array(core));
    core->close_array();
}

template <typename T>
BSONCXX_INLINE
    typename std::enable_if<!takes_document<T>::value && !takes_array<T>::value, void>::type
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
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
