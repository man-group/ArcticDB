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

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
namespace v_noabi {

enum class type : std::uint8_t;
enum class binary_sub_type : std::uint8_t;

}  // namespace v_noabi
}  // namespace bsoncxx

namespace bsoncxx {
namespace v_noabi {
namespace types {

#pragma push_macro("BSONCXX_ENUM")
#undef BSONCXX_ENUM
#define BSONCXX_ENUM(name, val) struct BSONCXX_API b_##name;
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM
#pragma pop_macro("BSONCXX_ENUM")

}  // namespace types
}  // namespace v_noabi
}  // namespace bsoncxx

namespace bsoncxx {

using ::bsoncxx::v_noabi::binary_sub_type;
using ::bsoncxx::v_noabi::type;

}  // namespace bsoncxx

namespace bsoncxx {
namespace types {

#pragma push_macro("BSONCXX_ENUM")
#undef BSONCXX_ENUM
#define BSONCXX_ENUM(name, val) using ::bsoncxx::v_noabi::types::b_##name;
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM
#pragma pop_macro("BSONCXX_ENUM")

}  // namespace types
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
