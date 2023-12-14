
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
#include <bsoncxx/builder/stream/closed_context.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/util/functor.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace stream {

///
/// A stream context which expects a key, which can later be followed by
/// value, then more key/value pairs.
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
template <class base = closed_context>
class key_context {
   public:
    ///
    /// Create a key_context given a core builder
    ///
    /// @param core
    ///   The core builder to orchestrate
    ///
    key_context(core* core) : _core(core) {}

    ///
    /// << operator for accepting a literal key and appending it to the core
    ///   builder.
    ///
    /// @param v
    ///   The key to append
    ///
    /// @throws bsoncxx::exception if the previous value appended to the builder was also a key.
    ///
    template <std::size_t n>
    BSONCXX_INLINE value_context<key_context> operator<<(const char (&v)[n]) {
        _core->key_view(stdx::string_view{v, n - 1});
        return value_context<key_context>(_core);
    }

    ///
    /// << operator for accepting a std::string key and appending it to the core
    ///   builder.
    ///
    /// @param str
    ///   The key to append
    ///
    /// @throws bsoncxx::exception if the previous value appended to the builder was also a key.
    ///
    BSONCXX_INLINE value_context<key_context> operator<<(std::string str) {
        _core->key_owned(std::move(str));
        return value_context<key_context>(_core);
    }

    ///
    /// << operator for accepting a stdx::string_view key and appending it to
    ///   the core builder.
    ///
    /// @param str
    ///   The key to append
    ///
    /// @throws bsoncxx::exception if the previous value appended to the builder was also a key.
    ///
    BSONCXX_INLINE value_context<key_context> operator<<(stdx::string_view str) {
        _core->key_view(std::move(str));
        return value_context<key_context>(_core);
    }

    ///
    /// << operator for accepting a callable of the form void(key_context)
    ///   and invoking it to perform 1 or more key, value appends to the core
    ///   builder.
    ///
    /// @param func
    ///   The callback to invoke
    ///
    template <typename T>
    BSONCXX_INLINE
        typename std::enable_if<util::is_functor<T, void(key_context<>)>::value, key_context>::type&
        operator<<(T&& func) {
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
        // TODO(MSVC): This should just be 'document::value', but
        // VS2015U1 can't resolve the name.
        bsoncxx::document::value>::type
    operator<<(T&&) {
        return _core->extract_document();
    }

    ///
    /// << operator for concatenating another document.
    ///
    /// This operation concatenates all of the keys and values from the passed
    /// document into the current stream.
    ///
    /// @param doc
    ///   A document to concatenate
    ///
    BSONCXX_INLINE key_context operator<<(concatenate_doc doc) {
        _core->concatenate(doc);
        return *this;
    }

    ///
    /// << operator for closing a subdocument in the core builder.
    ///
    /// @param _
    ///   A close_document_type token
    ///
    BSONCXX_INLINE base operator<<(const close_document_type) {
        _core->close_document();
        return unwrap();
    }

    ///
    /// Conversion operator which provides a rooted document given any stream
    /// currently in a nested key_context.
    ///
    BSONCXX_INLINE operator key_context<>() {
        return key_context<>(_core);
    }

   private:
    BSONCXX_INLINE base unwrap() {
        return base(_core);
    }

    core* _core;
};

}  // namespace stream
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
