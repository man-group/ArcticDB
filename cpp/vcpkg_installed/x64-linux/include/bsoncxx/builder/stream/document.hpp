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
#include <bsoncxx/builder/stream/key_context.hpp>
#include <bsoncxx/builder/stream/single_context.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace stream {

///
/// A streaming interface for constructing
/// a BSON document.
///
/// @note Use of the stream builder is discouraged. See
/// http://mongodb.github.io/mongo-cxx-driver/mongocxx-v3/working-with-bson/#stream-builder for more
/// details.
///
class document : public key_context<> {
   public:
    ///
    /// Default constructor.
    ///
    BSONCXX_INLINE document() : key_context<>(&_core), _core(false) {}

    ///
    /// @return A view of the BSON document.
    ///
    BSONCXX_INLINE bsoncxx::document::view view() const {
        return _core.view_document();
    }

    ///
    /// @return A view of the BSON document.
    ///
    BSONCXX_INLINE operator bsoncxx::document::view() const {
        return view();
    }

    ///
    /// Transfer ownership of the underlying document to the caller.
    ///
    /// @return A document::value with ownership of the document.
    ///
    /// @warning
    ///  After calling extract() it is illegal to call any methods
    ///  on this class, unless it is subsequenly moved into.
    ///
    BSONCXX_INLINE bsoncxx::document::value extract() {
        return _core.extract_document();
    }

    ///
    /// Reset the underlying BSON to an empty document.
    ///
    BSONCXX_INLINE void clear() {
        _core.clear();
    }

   private:
    core _core;
};

}  // namespace stream
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
