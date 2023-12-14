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

#include <bsoncxx/array/value.hpp>
#include <bsoncxx/builder/concatenate.hpp>
#include <bsoncxx/builder/core.hpp>
#include <bsoncxx/builder/stream/closed_context.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/util/functor.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace stream {

template <class T>
class key_context;

class single_context;

///
/// A stream context which expects any number of values.
///
/// The template argument can be used to hold additional information about
/// containing documents or arrays. I.e. value_context<> implies that this array
/// is a sub_array in a document, while array_context would indicated a sub_array
/// in an array. These types can be nested, such that contextual parsing (for
/// key/value pairs) and depth (to prevent an invalid array_close) are enforced
/// by the type system.
///
/// I.e.
/// builder << array_context << array_context << ...;
///
/// This builds a bson array with successively higher index keys
///
template <class base = closed_context>
class array_context {
   public:
    ///
    /// Create an array_context given a core builder
    ///
    /// @param core
    ///   The core builder to orchestrate
    ///
    BSONCXX_INLINE array_context(core* core) : _core(core) {}

    ///
    /// << operator for accepting a real value and appending it to the core
    ///   builder.
    ///
    /// @param t
    ///   The value to append
    ///
    template <class T>
    BSONCXX_INLINE typename std::enable_if<
        !(util::is_functor<T, void(array_context<>)>::value ||
          util::is_functor<T, void(single_context)>::value ||
          std::is_same<typename std::remove_reference<T>::type, const finalize_type>::value),
        array_context>::type&
    operator<<(T&& t) {
        _core->append(std::forward<T>(t));
        return *this;
    }

    ///
    /// << operator for accepting a callable of the form void(array_context) or
    ///   void(single_context) and invoking it to perform 1 or more value appends
    ///   to the core builder.
    ///
    /// @param func
    ///   The callback to invoke
    ///
    template <typename Func>
    BSONCXX_INLINE typename std::enable_if<(util::is_functor<Func, void(array_context<>)>::value ||
                                            util::is_functor<Func, void(single_context)>::value),
                                           array_context>::type&
    operator<<(Func&& func) {
        func(*this);
        return *this;
    }

    ///
    /// << operator for finalizing the stream.
    ///
    /// This operation finishes all processing necessary to fully encode the
    /// bson bytes and returns an owning value.
    ///
    /// @param _
    ///   A finalize_type token
    ///
    /// @return A value type which holds the complete bson document.
    ///
    template <typename T>
    BSONCXX_INLINE typename std::enable_if<
        std::is_same<base, closed_context>::value &&
            std::is_same<typename std::remove_reference<T>::type, const finalize_type>::value,
        // TODO(MSVC): This should just be 'array::value', but
        // VS2015U1 can't resolve the name.
        bsoncxx::array::value>::type
    operator<<(T&&) {
        return _core->extract_array();
    }

    ///
    /// << operator for opening a new subdocument in the core builder.
    ///
    /// @param _
    ///   An open_document_type token
    ///
    BSONCXX_INLINE key_context<array_context> operator<<(const open_document_type) {
        _core->open_document();
        return wrap_document();
    }

    ///
    /// << operator for concatenating another array.
    ///
    /// This operation concatenates all of the values from the passed document
    /// into the current stream. Keys are adjusted to match the existing array.
    ///
    /// @param array
    ///   An array to concatenate
    ///
    BSONCXX_INLINE array_context operator<<(concatenate_array array) {
        _core->concatenate(array.view());
        return *this;
    }

    ///
    /// << operator for opening a new subarray in the core builder.
    ///
    /// @param _
    ///   An open_document_type token
    ///
    BSONCXX_INLINE array_context<array_context> operator<<(const open_array_type) {
        _core->open_array();
        return wrap_array();
    }

    ///
    /// << operator for closing a subarray in the core builder.
    ///
    /// @param _
    ///   A close_array_type token
    ///
    BSONCXX_INLINE base operator<<(const close_array_type) {
        _core->close_array();
        return unwrap();
    }

    ///
    /// Conversion operator which provides a rooted array context given any
    /// stream currently in a nested array_context.
    ///
    BSONCXX_INLINE operator array_context<>() {
        return array_context<>(_core);
    }

    ///
    /// Conversion operator for single_context.
    ///
    /// @relatesalso single_context
    ///
    BSONCXX_INLINE operator single_context();

   private:
    BSONCXX_INLINE base unwrap() {
        return base(_core);
    }

    BSONCXX_INLINE array_context<array_context> wrap_array() {
        return array_context<array_context>(_core);
    }

    BSONCXX_INLINE key_context<array_context> wrap_document() {
        return key_context<array_context>(_core);
    }

    core* _core;
};

}  // namespace stream
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
