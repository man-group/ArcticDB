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

#include <bsoncxx/config/prelude.hpp>

#if defined(BSONCXX_POLY_USE_MNMLSTC)

#if defined(MONGO_CXX_DRIVER_COMPILING) || defined(BSONCXX_POLY_USE_SYSTEM_MNMLSTC)
#include <core/memory.hpp>
#else
#include <bsoncxx/third_party/mnmlstc/core/memory.hpp>
#endif

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace stdx {

using ::core::make_unique;

}  // namespace stdx
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#elif defined(BSONCXX_POLY_USE_BOOST)

#include <boost/smart_ptr/make_unique.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace stdx {

using ::boost::make_unique;

}  // namespace stdx
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#elif __cplusplus >= 201402L || defined(_MSVC_LANG)

#include <memory>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace stdx {

using ::std::make_unique;

}  // namespace stdx
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#else
#error "Cannot find a valid polyfill for make_unique"
#endif

#include <bsoncxx/config/postlude.hpp>
