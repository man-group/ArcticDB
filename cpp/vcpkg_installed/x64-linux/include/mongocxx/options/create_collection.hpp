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

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/validation_criteria.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {

///
/// Class representing the optional arguments to a MongoDB createCollection command
///
/// This class is deprecated, as are the database class methods that use this class.
/// Please use the new database::create_collection methods that take options as a
/// BSON document.
///
class MONGOCXX_API create_collection_deprecated {
   public:
    ///
    /// To create a capped collection, specify true.
    ///
    /// @note If you specify true, you must also set a maximum size using the size() method.
    ///
    /// @param capped
    ///   Whether or not this collection will be capped.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/glossary/#term-capped-collection
    ///
    create_collection_deprecated& capped(bool capped);

    ///
    /// Gets the current capped setting.
    ///
    /// @return
    ///   Whether or not this collection will be capped.
    ///
    /// @see https://docs.mongodb.com/manual/reference/glossary/#term-capped-collection
    ///
    const stdx::optional<bool>& capped() const;

    ///
    /// Sets the default collation for this collection.
    ///
    /// @param collation
    ///   The default collation for the collection.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/collation/
    ///
    create_collection_deprecated& collation(bsoncxx::document::view_or_value collation);

    ///
    /// Gets the default collation for this collection.
    ///
    /// @return
    ///   The default collation for the collection.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/collation/
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& collation() const;

    ///
    /// The maximum number of documents allowed in the capped collection.
    ///
    /// @note The size limit takes precedence over this limit. If a capped collection reaches
    ///   the size limit before it reaches the maximum number of documents, MongoDB removes
    ///   old documents.
    ///
    /// @param max_documents
    ///   Maximum number of documents allowed in the collection (if capped).
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    create_collection_deprecated& max(std::int64_t max_documents);

    ///
    /// Gets the current setting for the maximum number of documents allowed in the capped
    /// collection.
    ///
    /// @return
    ///   Maximum number of documents allowed in the collection (if capped).
    ///
    const stdx::optional<std::int64_t>& max() const;

    ///
    /// When true, disables the power of 2 sizes allocation for the collection.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/create/
    ///
    /// @param no_padding
    ///   When true, disables power of 2 sizing for this collection.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    create_collection_deprecated& no_padding(bool no_padding);

    ///
    /// Gets the current value of the "no padding" option for the collection.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/create/
    ///
    /// @return
    ///   When true, power of 2 sizing is disabled for this collection.
    ///
    const stdx::optional<bool>& no_padding() const;

    ///
    /// A maximum size, in bytes, for a capped collection.
    ///
    /// @note Once a capped collection reaches its maximum size, MongoDB removes older
    ///   documents to make space for new documents.
    ///
    /// @note Size is required for capped collections and ignored for other collections.
    ///
    /// @param max_size
    ///   Maximum size, in bytes, of this collection (if capped).
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    create_collection_deprecated& size(std::int64_t max_size);

    ///
    /// Gets the current size setting, for a capped collection.
    ///
    /// @return
    ///   Maximum size, in bytes, of this collection (if capped).
    ///
    const stdx::optional<std::int64_t>& size() const;

    ///
    /// Specify configuration to the storage on a per-collection basis.
    ///
    /// @note This option is currently only available with the WiredTiger storage engine.
    ///
    /// @param storage_engine_opts
    ///   Configuration options specific to the storage engine.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    create_collection_deprecated& storage_engine(
        bsoncxx::document::view_or_value storage_engine_opts);

    ///
    /// Gets the current storage engine configuration for this collection.
    ///
    /// @return
    ///   Configuration options specific to the storage engine.
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& storage_engine() const;

    ///
    /// Specify validation criteria for this collection.
    ///
    /// @param validation
    ///   Validation criteria for this collection.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/core/document-validation/
    ///
    create_collection_deprecated& validation_criteria(class validation_criteria validation);

    ///
    /// Gets the current validation criteria for this collection.
    ///
    /// @return
    ///   Validation criteria for this collection.
    ///
    /// @see https://docs.mongodb.com/manual/core/document-validation/
    ///
    const stdx::optional<class validation_criteria>& validation_criteria() const;

    ///
    /// Return a bson document representing the options set on this object.
    ///
    /// @deprecated
    ///   This method is deprecated. To determine which options are set on this object, use the
    ///   provided accessors instead.
    ///
    /// @return Options, as a document.
    ///
    MONGOCXX_DEPRECATED bsoncxx::document::value to_document() const;
    bsoncxx::document::value to_document_deprecated() const;

    ///
    /// @deprecated
    ///   This method is deprecated. To determine which options are set on this object, use the
    ///   provided accessors instead.
    ///
    MONGOCXX_DEPRECATED MONGOCXX_INLINE operator bsoncxx::document::value() const;

   private:
    stdx::optional<bool> _capped;
    stdx::optional<bsoncxx::document::view_or_value> _collation;
    stdx::optional<std::int64_t> _max_documents;
    stdx::optional<std::int64_t> _max_size;
    stdx::optional<bool> _no_padding;
    stdx::optional<bsoncxx::document::view_or_value> _storage_engine_opts;
    stdx::optional<class validation_criteria> _validation;
};

MONGOCXX_DEPRECATED typedef create_collection_deprecated create_collection;

MONGOCXX_INLINE create_collection_deprecated::operator bsoncxx::document::value() const {
    return to_document_deprecated();
}

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
