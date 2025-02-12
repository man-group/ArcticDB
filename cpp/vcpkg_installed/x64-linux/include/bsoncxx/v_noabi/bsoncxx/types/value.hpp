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

#include <bsoncxx/types/bson_value/view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace types {

///
/// The bsoncxx::v_noabi::types::bson_value::view class has been renamed to
/// bsoncxx::v_noabi::types::bson_value::view.
///
/// @deprecated use bsoncxx::v_noabi::types::bson_value::view instead.
///
BSONCXX_DEPRECATED typedef types::bson_value::view value;

}  // namespace types
}  // namespace v_noabi
}  // namespace bsoncxx

namespace bsoncxx {
namespace types {

using ::bsoncxx::v_noabi::types::value;  // Deprecated

}  // namespace types
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
