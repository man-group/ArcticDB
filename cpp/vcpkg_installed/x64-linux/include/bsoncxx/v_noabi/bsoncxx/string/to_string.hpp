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
#include <utility>

#include <bsoncxx/stdx/string_view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace string {

template <class CharT,
          class Traits = std::char_traits<CharT>,
          class Allocator = std::allocator<CharT>>
BSONCXX_INLINE std::basic_string<CharT, Traits, Allocator> to_string(
    stdx::basic_string_view<CharT, Traits> value, const Allocator& alloc = Allocator()) {
    return std::basic_string<CharT, Traits, Allocator>{value.data(), value.length(), alloc};
}

}  // namespace string
}  // namespace v_noabi
}  // namespace bsoncxx

namespace bsoncxx {
namespace string {

using ::bsoncxx::v_noabi::string::to_string;

}  // namespace string
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
