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

#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/options/transaction.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {

///
/// Class representing the optional arguments to mongocxx::client::start_session.
///
class MONGOCXX_API client_session {
   public:
    ///
    /// Sets the causal_consistency option.
    ///
    /// If true (the default), each operation in the session will be causally ordered after the
    /// previous read or write operation. Set to false to disable causal consistency.
    ///
    /// Unacknowledged writes are not causally consistent. If you execute a write operation with an
    /// unacknowledged write concern (a mongocxx::write_concern with
    /// mongocxx::write_concern::acknowledge_level of @c k_unacknowledged), the write does not
    /// participate in causal consistency.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see
    /// https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/#causal-consistency
    ///
    client_session& causal_consistency(bool causal_consistency) noexcept;

    ///
    /// Gets the value of the causal_consistency option.
    ///
    ///
    bool causal_consistency() const noexcept;

    ///
    /// Sets the read concern "snapshot" (not enabled by default).
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see
    /// https://docs.mongodb.com/manual/reference/read-concern-snapshot/
    ///
    /// @note Snapshot reads and causal consistency are mutually exclusive: only one or the
    /// other may be active at a time. Attempting to do so will result in an error being thrown
    /// by mongocxx::client::start_session.
    ///
    client_session& snapshot(bool enable_snapshot_reads) noexcept;

    ///
    /// Gets the value of the snapshot_reads option.
    ///
    bool snapshot() const noexcept;

    ///
    /// Sets the default transaction options.
    ///
    /// @param default_transaction_opts
    ///   The default transaction options.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    client_session& default_transaction_opts(transaction default_transaction_opts);

    ///
    /// Gets the current default transaction options.
    ///
    /// @return The default transaction options.
    ///
    const stdx::optional<transaction>& default_transaction_opts() const;

   private:
    // Allow the implementation of client_session to see these:
    friend mongocxx::client_session;

    stdx::optional<bool> _causal_consistency;
    stdx::optional<bool> _enable_snapshot_reads;

    stdx::optional<transaction> _default_transaction_opts;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
