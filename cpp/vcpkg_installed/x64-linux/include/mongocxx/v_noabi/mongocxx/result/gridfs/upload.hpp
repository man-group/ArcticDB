// Copyright 2017 MongoDB Inc.
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

#include <mongocxx/result/gridfs/upload-fwd.hpp>

#include <bsoncxx/array/value.hpp>
#include <bsoncxx/types/bson_value/view.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace result {
namespace gridfs {

/// Class representing the result of a GridFS upload operation.
class upload {
   public:
    upload(bsoncxx::v_noabi::types::bson_value::view id);

    ///
    /// Gets the id of the uploaded GridFS file.
    ///
    /// @return The id of the uploaded file.
    ///
    const bsoncxx::v_noabi::types::bson_value::view& id() const;

   private:
    // Array with a single element, containing the value of the _id field for the inserted files
    // collection document.
    bsoncxx::v_noabi::array::value _id_owned;

    // Points into _id_owned.
    bsoncxx::v_noabi::types::bson_value::view _id;

    friend MONGOCXX_API bool MONGOCXX_CALL operator==(const upload&, const upload&);
    friend MONGOCXX_API bool MONGOCXX_CALL operator!=(const upload&, const upload&);
};

}  // namespace gridfs
}  // namespace result
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
