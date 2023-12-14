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

#include <bsoncxx/builder/core.hpp>
#include <bsoncxx/builder/stream/array_context.hpp>
#include <bsoncxx/builder/stream/closed_context.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/util/functor.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace stream {

///
/// A stream context which expects a value, which can later be followed by
/// more key/value pairs.
///
/// The template argument can be used to hold additional information about
/// containing documents or arrays. I.e. value_context<> implies that this
/// document is a sub_document in a document, while array_context would
/// indicated a sub_document in an array. These types can be nested, such that
/// contextual parsing (for key/value pairs) and depth (to prevent an invalid
/// document_close) are enforced by the type system.
///
/// When in document context, the first parameter will be in key_context, then
/// in value_context, then in key_context, etc.
///
/// I.e.
/// builder << key_context << value_context << key_context << ...
///
template <class base>
class value_context {
   public:
    ///
    /// Create a value_context given a core builder
    ///
    /// @param core
    ///   The core builder to orchestrate
    ///
    BSONCXX_INLINE value_context(core* core) : _core(core) {}

    ///
    /// << operator for accepting a real value and appending it to the core
    ///   builder.
    ///
    /// @param t
    ///   The value to append
    ///
    template <class T>
    BSONCXX_INLINE
        typename std::enable_if<!util::is_functor<T, void(single_context)>::value, base>::type
        operator<<(T&& t) {
        _core->append(std::forward<T>(t));
        return unwrap();
    }

    ///
    /// << operator for accepting a callable of the form void(single_context)
    ///   and invoking it to perform a value append to the core builder.
    ///
    /// @param func
    ///   The callback to invoke
    ///
    template <typename T>
    BSONCXX_INLINE
        typename std::enable_if<util::is_functor<T, void(single_context)>::value, base>::type
        operator<<(T&& func) {
        func(*this);
        return unwrap();
    }

    ///
    /// << operator for opening a new subdocument in the core builder.
    ///
    /// @param _
    ///   An open_document_type token
    ///
    BSONCXX_INLINE key_context<base> operator<<(const open_document_type) {
        _core->open_document();
        return wrap_document();
    }

    ///
    /// << operator for opening a new subarray in the core builder.
    ///
    /// @param _
    ///   An open_array_type token
    ///
    BSONCXX_INLINE array_context<base> operator<<(const open_array_type) {
        _core->open_array();
        return wrap_array();
    }

    ///
    /// Conversion operator for single_context.
    ///
    /// @relatesalso single_context
    ///
    operator single_context();

#if !defined(_MSC_VER) && !defined(__INTEL_COMPILER)
    // TODO(MSVC): Causes an ICE under VS2015U1
    static_assert(
        std::is_same<value_context, decltype(std::declval<value_context>() << 1 << "str")>::value,
        "value_context must be templatized on a key_context");
#endif

   private:
    BSONCXX_INLINE base unwrap() {
        return base(_core);
    }

    BSONCXX_INLINE array_context<base> wrap_array() {
        return array_context<base>(_core);
    }

    BSONCXX_INLINE key_context<base> wrap_document() {
        return key_context<base>(_core);
    }

    core* _core;
};

}  // namespace stream
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
