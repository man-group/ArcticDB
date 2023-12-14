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
#include <bsoncxx/builder/stream/key_context.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace stream {

///
/// A stream context which appends a single value.
///
/// This type is useful as the argument to a callable passed to other stream
/// modes. Specifically, any callback that takes a single_context can be used to
/// write a value in value_context or array_context.
///
class single_context {
   public:
    ///
    /// Create a single_context given a core builder
    ///
    /// @param core
    ///   The core builder to orchestrate
    ///
    BSONCXX_INLINE single_context(core* core) : _core(core) {}

    ///
    /// << operator for opening a new subdocument in the core builder.
    ///
    /// @param _
    ///   An open_document_type token
    ///
    BSONCXX_INLINE key_context<> operator<<(open_document_type) {
        _core->open_document();

        return wrap_document();
    }

    ///
    /// << operator for opening a new subarray in the core builder.
    ///
    /// @param _
    ///   An open_array_type token
    ///
    BSONCXX_INLINE array_context<> operator<<(open_array_type) {
        _core->open_array();

        return wrap_array();
    }

    ///
    /// << operator for accepting a real value and appending it to the core
    ///   builder.
    ///
    /// @param t
    ///   The value to append
    ///
    template <class T>
    BSONCXX_INLINE void operator<<(T&& t) {
        _core->append(std::forward<T>(t));
    }

   private:
    BSONCXX_INLINE array_context<> wrap_array() {
        return array_context<>(_core);
    }

    BSONCXX_INLINE key_context<> wrap_document() {
        return key_context<>(_core);
    }

    core* _core;
};

///
/// Implementation of the single_context conversion operator for array_context.
///
template <class T>
BSONCXX_INLINE array_context<T>::operator single_context() {
    return single_context(_core);
}

///
/// Implementation of the single_context conversion operator for value_context.
///
template <class T>
BSONCXX_INLINE value_context<T>::operator single_context() {
    return single_context(_core);
}

}  // namespace stream
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
