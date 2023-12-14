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
#include <memory>

#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client_session;
class read_concern;
class write_concern;
class read_preference;

namespace options {

///
/// Class representing the optional arguments for a transaction.
///
class MONGOCXX_API transaction {
   public:
    transaction();

    ///
    /// Copy constructs transaction options.
    ///
    transaction(const transaction&);

    ///
    /// Copy assigns transaction options.
    ///
    transaction& operator=(const transaction&);

    ///
    /// Move constructs transaction options.
    ///
    transaction(transaction&&) noexcept;

    ///
    /// Move assigns transaction options.
    ///
    transaction& operator=(transaction&&) noexcept;

    ///
    /// Destroys the transaction options.
    ///
    ~transaction() noexcept;

    ///
    /// Sets the transaction read concern.
    ///
    /// @param rc
    ///   The read concern.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    transaction& read_concern(const class read_concern& rc);

    ///
    /// Gets the current transaction read concern.
    ///
    /// @return
    ///    An optional containing the read concern. If the read concern has not been set, a
    ///    disengaged optional is returned.
    stdx::optional<class read_concern> read_concern() const;

    ///
    /// Sets the transaction write concern.
    ///
    /// @param wc
    ///   The write concern.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    transaction& write_concern(const class write_concern& wc);

    ///
    /// Gets the current transaction write concern.
    ///
    /// @return The write concern.
    ///
    /// @return
    ///    An optional containing the write concern. If the write concern has not been set, a
    ///    disengaged optional is returned.
    stdx::optional<class write_concern> write_concern() const;

    ///
    /// Sets the transaction read preference.
    ///
    /// @param rp
    ///   The read preference.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    transaction& read_preference(const class read_preference& rp);

    ///
    /// Gets the current transaction read preference.
    ///
    /// @return
    ///    An optional containing the read preference. If the read preference has not been set, a
    ///    disengaged optional is returned.
    stdx::optional<class read_preference> read_preference() const;

    ///
    /// Sets the transaction's max commit time, in milliseconds.
    ///
    /// @param ms
    ///   The max commit time in milliseconds.
    ///
    /// @return
    ///   A reference to the object on which this function is being called.
    ///
    transaction& max_commit_time_ms(std::chrono::milliseconds ms);

    ///
    /// Gets the current transaction commit time, in milliseconds.
    ///
    /// @return
    ///   An optional containing the timeout. If the max commit time has not been set,
    ///   a disengaged optional is returned.
    ///
    stdx::optional<std::chrono::milliseconds> max_commit_time_ms() const;

   private:
    friend class ::mongocxx::client_session;

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE impl& _get_impl();
    MONGOCXX_PRIVATE const impl& _get_impl() const;
    std::unique_ptr<impl> _impl;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
