// Copyright 2017-present MongoDB Inc.
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

#include <functional>
#include <memory>

#include <mongocxx/bulk_write-fwd.hpp>
#include <mongocxx/client-fwd.hpp>
#include <mongocxx/client_session-fwd.hpp>
#include <mongocxx/collection-fwd.hpp>
#include <mongocxx/database-fwd.hpp>
#include <mongocxx/index_view-fwd.hpp>
#include <mongocxx/search_index_view-fwd.hpp>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/options/client_session.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {

///
/// Use a session for a sequence of operations, optionally with either causal consistency
/// or snapshots.
///
/// Note that client_session is not thread-safe. See
/// https://mongocxx.org/mongocxx-v3/thread-safety/ for more details.
///
/// @see
/// https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/#causal-consistency
///
class client_session {
   public:
    enum class transaction_state {
        k_transaction_none,
        k_transaction_starting,
        k_transaction_in_progress,
        k_transaction_committed,
        k_transaction_aborted,
    };

    ///
    /// Move constructs a session.
    ///
    client_session(client_session&&) noexcept;

    ///
    /// Move assigns a session.
    ///
    client_session& operator=(client_session&&) noexcept;

    client_session(const client_session&) = delete;
    client_session& operator=(const client_session&) = delete;

    ///
    /// Ends and destroys the session.
    ///
    ~client_session() noexcept;

    ///
    /// Gets the client that started this session.
    ///
    const mongocxx::v_noabi::client& client() const noexcept;

    ///
    /// Gets the options this session was created with.
    ///
    const options::client_session& options() const noexcept;

    ///
    /// Get the server-side "logical session ID" associated with this session, as a BSON document.
    /// This view is invalid after the session is destroyed.
    ///
    bsoncxx::v_noabi::document::view id() const noexcept;

    ///
    /// Get the session's clusterTime, as a BSON document. This is an opaque value suitable for
    /// passing to advance_cluster_time(). The document is empty if the session has
    /// not been used for any operation and you have not called advance_cluster_time().
    /// This view is invalid after the session is destroyed.
    ///
    bsoncxx::v_noabi::document::view cluster_time() const noexcept;

    ///
    /// Get the session's operationTime, as a BSON timestamp. This is an opaque value suitable for
    /// passing to advance_operation_time(). The timestamp is zero if the session has not been used
    /// for any operation and you have not called advance_operation_time().
    ///
    bsoncxx::v_noabi::types::b_timestamp operation_time() const noexcept;

    ///
    /// Get the server_id the session is pinned to. The server_id is zero if the session is not
    /// pinned to a server.
    ///
    std::uint32_t server_id() const noexcept;

    ///
    /// Returns the current transaction state for this session.
    ///
    transaction_state get_transaction_state() const noexcept;

    ///
    /// Returns whether or not this session is dirty.
    ///
    bool get_dirty() const noexcept;

    ///
    /// Advance the cluster time for a session. Has an effect only if the new cluster time is
    /// greater than the session's current cluster time.
    ///
    /// Use advance_operation_time() and advance_cluster_time() to copy the operationTime and
    /// clusterTime from another session, ensuring subsequent operations in this session are
    /// causally consistent with the last operation in the other session.
    ///
    void advance_cluster_time(const bsoncxx::v_noabi::document::view& cluster_time);

    ///
    /// Advance the session's operation time, expressed as a BSON timestamp. Has an effect only if
    /// the new operation time is greater than the session's current operation time.
    ///
    /// Use advance_operation_time() and advance_cluster_time() to copy the operationTime and
    /// clusterTime from another session, ensuring subsequent operations in this session are
    /// causally consistent with the last operation in the other session.
    ///
    void advance_operation_time(const bsoncxx::v_noabi::types::b_timestamp& operation_time);

    ///
    /// Starts a transaction on the current client session.
    ///
    /// @param transaction_opts (optional)
    ///    The options to use in the transaction.
    ///
    /// @throws mongocxx::v_noabi::operation_exception if the options are misconfigured, if there
    /// are network or other transient failures, or if there are other errors such as a session with
    /// a transaction already in progress.
    ///
    void start_transaction(const stdx::optional<options::transaction>& transaction_opts = {});

    ///
    /// Commits a transaction on the current client session.
    ///
    /// @throws mongocxx::v_noabi::operation_exception if the options are misconfigured, if there
    /// are network or other transient failures, or if there are other errors such as a session with
    /// no transaction in progress.
    ///
    void commit_transaction();

    ///
    /// Aborts a transaction on the current client session.
    ///
    /// @throws mongocxx::v_noabi::operation_exception if the options are misconfigured or if there
    /// are other errors such as a session with no transaction in progress.
    ///
    void abort_transaction();

    ///
    /// Helper to run a user-provided callback within a transaction.
    ///
    /// This method will start a new transaction on this client session,
    /// run the callback, then commit the transaction. If it cannot commit
    /// the transaction, the entire sequence may be retried, and the callback
    /// may be run multiple times.
    ///
    /// If the user callback calls driver methods that run operations against the
    /// server that can throw an operation_exception (ex: collection::insert_one),
    /// the user callback should allow those exceptions to propagate up the stack
    /// so they can be caught and processed by the with_transaction helper.
    ///
    /// @param cb
    ///   The callback to run inside of a transaction.
    /// @param opts (optional)
    ///   The options to use to run the transaction.
    ///
    /// @throws mongocxx::v_noabi::operation_exception if there are errors completing the
    /// transaction.
    ///
    using with_transaction_cb = std::function<void MONGOCXX_CALL(client_session*)>;
    void with_transaction(with_transaction_cb cb, options::transaction opts = {});

   private:
    friend ::mongocxx::v_noabi::bulk_write;
    friend ::mongocxx::v_noabi::client;
    friend ::mongocxx::v_noabi::collection;
    friend ::mongocxx::v_noabi::database;
    friend ::mongocxx::v_noabi::index_view;
    friend ::mongocxx::v_noabi::search_index_view;

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE client_session(const mongocxx::v_noabi::client* client,
                                    const options::client_session& options);

    MONGOCXX_PRIVATE impl& _get_impl();
    MONGOCXX_PRIVATE const impl& _get_impl() const;

    std::unique_ptr<impl> _impl;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>

///
/// @example examples/mongocxx/client_session.cpp
/// Use a mongocxx::v_noabi::client_session for a sequence of operations with causal consistency.
///
