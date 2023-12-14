// Copyright 2018-present MongoDB Inc.
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

#include <chrono>

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/string/view_or_value.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client;
class collection;
class database;

namespace options {

///
/// Class representing MongoDB change stream options.
///
class MONGOCXX_API change_stream {
   public:
    change_stream();

    ///
    /// Sets the fullDocument option for the $changeStream.
    ///
    /// Allowed values: 'default', 'updateLookup', 'whenAvailable', 'required'.
    ///
    /// The default is to not send a value, which is equivalent to 'default'. By default, the change
    /// notification for partial updates will include a delta describing the changes to the
    /// document.
    ///
    /// When set to 'updateLookup', the change notification for partial updates will include both a
    /// delta describing the changes to the document as well as a copy of the entire document that
    /// was changed from some time after the change occurred.
    ///
    /// When set to 'whenAvailable', configures the change stream to return the post-image of the
    /// modified document for replace and update change events if the post-image for this event is
    /// available.
    ///
    /// When set to 'required', the same behavior as 'whenAvailable' except that an error is raised
    /// if the post-image is not available.
    ///
    /// @param full_doc
    ///   The fullDocument option to use on this stream.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    change_stream& full_document(bsoncxx::string::view_or_value full_doc);

    ///
    /// Gets the current fullDocument option.
    ///
    /// @return
    ///   The current fullDocument option.
    ///
    const bsoncxx::stdx::optional<bsoncxx::string::view_or_value>& full_document() const;

    ///
    /// Sets the fullDocumentBeforeChange option for the $changeStream.
    ///
    /// The allowed values are: 'whenAvailable', 'required', 'off'.
    /// If none set, defaults to 'off'.
    ///
    /// When set to 'whenAvailable', configures the change stream to return the pre-image of the
    /// modified document for replace, update, and delete change events if it is available.
    ///
    /// When set to 'required', the same behavior as 'whenAvailable' except that an error is raised
    /// if the pre-image is not available.
    ///
    /// @param full_doc_before_change
    ///   The fullDocumentBeforeChange option to use on this stream.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    change_stream& full_document_before_change(
        bsoncxx::string::view_or_value full_doc_before_change);

    ///
    /// Gets the current fullDocumentBeforeChange option.
    ///
    /// @return
    ///   The current fullDocumentBeforeChange option.
    ///
    const bsoncxx::stdx::optional<bsoncxx::string::view_or_value>& full_document_before_change()
        const;

    ///
    /// Sets the number of documents to return per batch.
    ///
    /// @param batch_size
    ///   The size of the batches to request.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    change_stream& batch_size(std::int32_t batch_size);

    ///
    /// The current batch size setting.
    ///
    /// @return
    ///   The current batch size.
    ///
    const stdx::optional<std::int32_t>& batch_size() const;

    ///
    /// Sets the current value of the comment option.
    ///
    /// @param comment
    ///   The new comment option.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    change_stream& comment(bsoncxx::types::bson_value::view_or_value comment);

    ///
    /// Gets the current value of the comment option.
    ///
    /// @return
    ///   The current comment option.
    ///
    const stdx::optional<bsoncxx::types::bson_value::view_or_value>& comment() const;

    ///
    /// Specifies the logical starting point for the new change stream.
    ///
    /// The value returned by calling change_stream::get_resume_token can be used here.
    ///
    /// start_after, resume_after, and start_at_operation_time are mutually exclusive options.
    /// Setting more than one of these will result in a server error.
    ///
    /// @param resume_after
    ///   The resumeToken to use when starting the change stream.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    change_stream& resume_after(bsoncxx::document::view_or_value resume_after);

    ///
    /// Retrieves the current resumeToken for this change stream.
    ///
    /// @return
    ///   The current resumeToken.
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& resume_after() const;

    ///
    /// Specifies the logical starting point of the new change stream. The new stream will
    /// return the first notification after the given token.
    ///
    /// The value returned by calling change_stream::get_resume_token can be used here.
    ///
    /// Unlike resumeAfter, this can resume notifications after an "invalidate" event.
    ///
    /// start_after, resume_after, and start_at_operation_time are mutually exclusive options.
    /// Setting more than one of these will result in a server error.
    ///
    /// @param token
    ///   The token representing the logical starting point of the change stream.
    ///
    /// @return
    ///   A reference to the object on which this function is being called.
    ///
    change_stream& start_after(bsoncxx::document::view_or_value token);

    ///
    /// Retrieves the current startAfter token for this change stream.
    ///
    /// @return
    ///   The current startAfter token.
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& start_after() const;

    ///
    /// Sets the collation for this operation.
    ///
    /// @param collation
    ///   The new collation.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    change_stream& collation(bsoncxx::document::view_or_value collation);

    ///
    /// Retrieves the current collation for this operation.
    ///
    /// @return
    ///   The current collation.
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& collation() const;

    ///
    /// Sets the maximum amount of time for for the server to wait on new documents to satisfy a
    /// change stream query.
    ///
    /// @param max_time
    ///   The max amount of time (in milliseconds) for the server to wait on new documents.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    change_stream& max_await_time(std::chrono::milliseconds max_time);

    ///
    /// The current max_time setting.
    ///
    /// @return
    ///   The current max time (in milliseconds).
    ///
    const stdx::optional<std::chrono::milliseconds>& max_await_time() const;

    ///
    /// Specifies the logical starting point for the new change stream. Changes are returned at or
    /// after the specified operation time.
    ///
    /// start_after, resume_after, and start_at_operation_time are mutually exclusive options.
    /// Setting more than one of these will result in a server error.
    ///
    /// @param timestamp
    ///   The starting operation time.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    change_stream& start_at_operation_time(bsoncxx::types::b_timestamp timestamp);

   private:
    friend class ::mongocxx::client;
    friend class ::mongocxx::collection;
    friend class ::mongocxx::database;

    bsoncxx::document::value as_bson() const;
    stdx::optional<bsoncxx::string::view_or_value> _full_document;
    stdx::optional<bsoncxx::string::view_or_value> _full_document_before_change;
    stdx::optional<std::int32_t> _batch_size;
    stdx::optional<bsoncxx::types::bson_value::view_or_value> _comment;
    stdx::optional<bsoncxx::document::view_or_value> _collation;
    stdx::optional<bsoncxx::document::view_or_value> _resume_after;
    stdx::optional<bsoncxx::document::view_or_value> _start_after;
    stdx::optional<std::chrono::milliseconds> _max_await_time;
    // _start_at_operation_time is not wrapped in a stdx::optional because of a longstanding bug in
    // the MNMLSTC polyfill that has been fixed on master, but not in the latest release:
    // https://github.com/mnmlstc/core/pull/23
    bsoncxx::types::b_timestamp _start_at_operation_time = {};
    bool _start_at_operation_time_set = false;
};
}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
