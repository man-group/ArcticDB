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

#include <bsoncxx/builder/basic/array-fwd.hpp>
#include <bsoncxx/builder/basic/document-fwd.hpp>

#include <bsoncxx/builder/basic/impl.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/sub_document.hpp>
#include <bsoncxx/builder/core.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace builder {
namespace basic {

///
/// A traditional builder-style interface for constructing
/// a BSON document.
///
class document : public sub_document {
   public:
    ///
    /// Default constructor
    ///
    BSONCXX_INLINE document() : sub_document(&_core), _core(false) {}

    ///
    /// Move constructor
    ///
    BSONCXX_INLINE document(document&& doc) noexcept
        : sub_document(&_core), _core(std::move(doc._core)) {}

    ///
    /// Move assignment operator
    ///
    BSONCXX_INLINE document& operator=(document&& doc) noexcept {
        _core = std::move(doc._core);
        return *this;
    }

    ///
    /// @return A view of the BSON document.
    ///
    BSONCXX_INLINE bsoncxx::v_noabi::document::view view() const {
        return _core.view_document();
    }

    ///
    /// Conversion operator that provides a view of the current builder
    /// contents.
    ///
    /// @return A view of the current builder contents.
    ///
    BSONCXX_INLINE operator bsoncxx::v_noabi::document::view() const {
        return view();
    }

    ///
    /// Transfer ownership of the underlying document to the caller.
    ///
    /// @return A document::value with ownership of the document.
    ///
    /// @warning
    ///  After calling extract() it is illegal to call any methods
    ///  on this class, unless it is subsequently moved into.
    ///
    BSONCXX_INLINE bsoncxx::v_noabi::document::value extract() {
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

///
/// Creates a document from a list of key-value pairs.
///
/// @param args
///   A variadic list of key-value pairs. The types of the keys and values can be anything that
///   builder::basic::sub_document::append accepts.
///
/// @return
///   A bsoncxx::v_noabi::document::value containing the elements.
///
template <typename... Args>
bsoncxx::v_noabi::document::value BSONCXX_CALL make_document(Args&&... args) {
    document document;
    document.append(std::forward<Args>(args)...);
    return document.extract();
}

}  // namespace basic
}  // namespace builder
}  // namespace v_noabi
}  // namespace bsoncxx

namespace bsoncxx {
namespace builder {
namespace basic {

using ::bsoncxx::v_noabi::builder::basic::make_document;

}  // namespace basic
}  // namespace builder
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
