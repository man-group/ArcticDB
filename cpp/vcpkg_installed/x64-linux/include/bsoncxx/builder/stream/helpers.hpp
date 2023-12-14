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

#include <bsoncxx/builder/concatenate.hpp>
#include <bsoncxx/document/view_or_value.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
namespace stream {

using bsoncxx::builder::concatenate;

///
/// The type of a stream manipulator to open a subdocument.
///
struct BSONCXX_API open_document_type {
    constexpr open_document_type() {}
};

///
/// A stream manipulator to open a subdocument.
///
constexpr open_document_type open_document;

///
/// The type of a stream manipulator to close a subdocument.
///
struct BSONCXX_API close_document_type {
    constexpr close_document_type() {}
};

///
/// A stream manipulator to close a subdocument.
///
constexpr close_document_type close_document;

///
/// The type of a stream manipulator to open a subarray.
///
struct BSONCXX_API open_array_type {
    constexpr open_array_type() {}
};

///
/// A stream manipulator to open a subarray.
///
/// @see https://mongodb.github.io/mongo-cxx-driver/mongocxx-v3/working-with-bson/#builders for help
/// building arrays in loops.
///
constexpr open_array_type open_array;

///
/// The type of a stream manipulator to close a subarray.
///
struct BSONCXX_API close_array_type {
    constexpr close_array_type() {}
};

///
/// A stream manipulator to close a subarray.
///
constexpr close_array_type close_array;

///
/// The type of a stream manipulator to finalize a document.
///
struct BSONCXX_API finalize_type {
    constexpr finalize_type() {}
};

///
/// A stream manipulator to finalize a document. When finalize is passed,
/// the expression will evaluate to an owning document::value or array::value.
///
constexpr finalize_type finalize;

}  // namespace stream
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
