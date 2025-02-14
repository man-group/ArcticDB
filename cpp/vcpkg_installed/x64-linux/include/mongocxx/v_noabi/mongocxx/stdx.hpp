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

#include <mongocxx/config/prelude.hpp>

// "Forward-declare" the `bsoncxx::v_noabi::stdx` namespace to permit the using-declaration below.
namespace bsoncxx {
namespace v_noabi {
namespace stdx {}
}  // namespace v_noabi
}  // namespace bsoncxx

namespace mongocxx {
namespace v_noabi {
namespace stdx {

using namespace bsoncxx::v_noabi::stdx;

}  // namespace stdx
}  // namespace v_noabi
}  // namespace mongocxx

namespace mongocxx {
namespace stdx {

using namespace mongocxx::v_noabi::stdx;

}  // namespace stdx
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
