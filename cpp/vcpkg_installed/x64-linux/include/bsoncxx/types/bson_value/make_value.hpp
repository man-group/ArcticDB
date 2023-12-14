// Copyright 2020 MongoDB Inc.
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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types/bson_value/value.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace types {
namespace bson_value {

///
/// Helper to construct a bson_value::value from a component bson type. The type
/// of the passed-in t can be anything that builder::basic::sub_document::append accepts.
///
template <typename T>
BSONCXX_INLINE bson_value::value make_value(T&& t) {
    auto doc = builder::basic::make_document(builder::basic::kvp("v", std::forward<T>(t)));
    return doc.view()["v"].get_owning_value();
}

}  // namespace bson_value
}  // namespace types
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
