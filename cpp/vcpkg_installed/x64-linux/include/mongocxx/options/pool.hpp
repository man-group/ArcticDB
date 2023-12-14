// Copyright 2016 MongoDB Inc.
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

#include <mongocxx/options/client.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {

///
/// Class representing the optional arguments to a MongoDB driver pool object. Pool options
/// logically extend client options.
///
class MONGOCXX_API pool {
   public:
    ///
    /// Constructs a new pool options object. Note that options::pool is implictly convertible from
    /// options::client.
    ///
    /// @param client_opts
    ///   The client options.
    ///
    pool(client client_opts = client());

    ///
    /// The current client options.
    ///
    /// @return The client options.
    ///
    const client& client_opts() const;

   private:
    client _client_opts;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
