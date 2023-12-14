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

#include <memory>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/optional.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client;
class collection;
class database;

///
/// Class representing a MongoDB change stream.
///
class MONGOCXX_API change_stream {
   public:
    /// A change stream iterator.
    class MONGOCXX_API iterator;

    ///
    /// Move constructs a change_stream.
    ///
    change_stream(change_stream&& other) noexcept;

    ///
    /// Move assigns a change_stream.
    ///
    change_stream& operator=(change_stream&& other) noexcept;

    ///
    /// Destroys a change_stream.
    ///
    ~change_stream();

    ///
    /// A change_stream::iterator points to the beginning of any
    /// available notifications. Each call to begin() advances to the next
    /// available notification. The state of all iterators is tracked by the
    /// change_stream itself, so advancing one iterator advances all iterators.
    ///
    /// change_stream::begin() and the increment operators are blocking operations.
    /// They will not return until a notification is available, the max_await_time (from
    /// the options::change_stream) milliseconds have elapsed, or a server
    /// error is encountered.
    ///
    /// When change_stream.begin() == change_stream.end(), no notifications
    /// are available. Each call to change_stream.begin() checks again for
    /// newly-available notifications.
    ///
    /// @return
    ///   The change_stream::iterator
    /// @exception
    ///   Throws mongocxx::query_exception if the query failed.
    ///
    iterator begin() const;

    ///
    /// A change_stream::iterator indicating stream exhaustion, meaning that
    /// no notifications are available from the stream.
    ///
    /// @return
    ///   The change_stream::iterator indicating exhaustion
    ///
    iterator end() const;

    ///
    /// Returns a resume token for this change stream.
    ///
    /// If the change stream has not been iterated, and either resume_after or
    /// start_after was specified in the options to this change stream, the
    /// specified value will be returned by this method. If neither resume_after or
    /// start_after was set on the options for this change stream, and it has
    /// not been iterated, this method will return no token.
    ///
    /// Once this change stream has been iterated, this method will return the
    /// resume token of the most recently returned document in the stream, or a
    /// postBatchResumeToken if the current batch of documents has been exhausted.
    ///
    /// @see https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token
    ///
    /// The returned document::view is valid for the lifetime of the stream and
    /// its data may be updated if the change stream is iterated after this function.
    /// The value may be copied to extend its lifetime or preserve the
    /// current resume token.
    ///
    /// @return
    ///   The token.
    ///
    bsoncxx::stdx::optional<bsoncxx::document::view> get_resume_token() const;

   private:
    friend class client;
    friend class collection;
    friend class database;
    friend class change_stream::iterator;

    MONGOCXX_PRIVATE change_stream(void* change_stream_ptr);

    class MONGOCXX_PRIVATE impl;
    std::unique_ptr<impl> _impl;
};

///
/// Class representing a MongoDB change stream iterator.
///
class MONGOCXX_API change_stream::iterator {
   public:
    // Support input-iterator (caveat of post-increment returning void)
    using difference_type = std::int64_t;
    using value_type = const bsoncxx::document::view;
    using pointer = std::add_pointer<value_type>::type;
    using reference = std::add_lvalue_reference<value_type>::type;
    using iterator_category = std::input_iterator_tag;

    ///
    /// Default-construct an iterator.
    /// Default-constucted iterators can be compared (all default-constructed
    /// iterators are ==), assigned, and copied.
    ///
    iterator();

    ///
    /// Dereferences the view for the document currently being pointed to.
    ///
    const bsoncxx::document::view& operator*() const;

    ///
    /// Accesses a member of the dereferenced document currently being pointed to.
    ///
    const bsoncxx::document::view* operator->() const;

    ///
    /// Pre-increments the iterator to move to the next document.
    ///
    /// change_stream::begin() and increment operators are blocking operations.
    /// They will not return until a notification is available, the max_await_time (from
    /// the options::change_stream) miliseconds have elapsed, or a server
    /// error is encountered.
    ///
    /// @throws mongocxx::query_exception if the query failed
    ///
    iterator& operator++();

    ///
    /// Post-increments the iterator to move to the next document.
    ///
    /// change_stream::begin() and increment operators are blocking operations.
    /// They will not return until a notification is available, the max_await_time (from
    /// the options::change_stream) miliseconds have elapsed, or a server
    /// error is encountered.
    ///
    /// @throws mongocxx::query_exception if the query failed
    ///
    void operator++(int);

   private:
    friend class change_stream;
    enum class iter_type { k_tracking, k_default_constructed, k_end };

    MONGOCXX_PRIVATE explicit iterator(iter_type type, const change_stream* change_stream);

    ///
    /// @{
    ///
    /// Compare two iterators for (in)-equality.  Iterators compare equal if
    /// they point to the same underlying change_stream or if both are exhausted.
    ///
    /// @relates iterator
    ///
    friend MONGOCXX_API bool MONGOCXX_CALL operator==(const change_stream::iterator&,
                                                      const change_stream::iterator&) noexcept;

    friend MONGOCXX_API bool MONGOCXX_CALL operator!=(const change_stream::iterator&,
                                                      const change_stream::iterator&) noexcept;
    ///
    /// @}
    ///

    MONGOCXX_PRIVATE bool is_exhausted() const;

    // iter_type==k_default_constructed is equivalent to _change_stream==nullptr
    iter_type _type;
    const change_stream* _change_stream;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
