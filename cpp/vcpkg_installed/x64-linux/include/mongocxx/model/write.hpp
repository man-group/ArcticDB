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

#include <cstdint>

#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/model/delete_many.hpp>
#include <mongocxx/model/delete_one.hpp>
#include <mongocxx/model/insert_one.hpp>
#include <mongocxx/model/replace_one.hpp>
#include <mongocxx/model/update_many.hpp>
#include <mongocxx/model/update_one.hpp>
#include <mongocxx/write_type.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace model {

///
/// Models a single write operation within a mongocxx::bulk_write.
///
class MONGOCXX_API write {
   public:
    ///
    /// Constructs a write from a model::insert_one.
    ///
    write(insert_one value);

    ///
    /// Constructs a write from a model::update_one.
    ///
    write(update_one value);

    ///
    /// Constructs a write from a model::update_many.
    ///
    write(update_many value);

    ///
    /// Constructs a write from a model::delete_one.
    ///
    write(delete_one value);

    ///
    /// Constructs a write from a model::delete_many.
    ///
    write(delete_many value);

    ///
    /// Constructs a write from a model::replace_one.
    ///
    write(replace_one value);

    ///
    /// Move constructs a write.
    ///
    write(write&& rhs) noexcept;

    ///
    /// Move assigns a write.
    ///
    write& operator=(write&& rhs) noexcept;

    write(const write& rhs) = delete;
    write& operator=(const write& rhs) = delete;

    ///
    /// Destroys a write.
    ///
    ~write();

    ///
    /// Returns the current type of this write. You must call this
    /// method before calling any of the get methods below.
    ///
    write_type type() const;

    ///
    /// Accesses the write as a model::insert_one. It is illegal to call
    /// this method if the return of type() does not indicate
    /// that this object currently contains the applicable type.
    ///
    const insert_one& get_insert_one() const;

    ///
    /// Accesses the write as an model::update_one. It is illegal to call
    /// this method if the return of type() does not indicate
    /// that this object currently contains the applicable type.
    ///
    const update_one& get_update_one() const;

    ///
    /// Accesses the write as an model::update_many. It is illegal to call
    /// this method if the return of type() does not indicate
    /// that this object currently contains the applicable type.
    ///
    const update_many& get_update_many() const;

    ///
    /// Accesses the write as a model::delete_one. It is illegal to call
    /// this method if the return of type() does not indicate
    /// that this object currently contains the applicable type.
    ///
    const delete_one& get_delete_one() const;

    ///
    /// Accesses the write as a model::delete_many. It is illegal to call
    /// this method if the return of type() does not indicate
    /// that this object currently contains the applicable type.
    ///
    const delete_many& get_delete_many() const;

    ///
    /// Accesses the write as a model::replace_one. It is illegal to call
    /// this method if the return of type() does not indicate
    /// that this object currently contains the applicable type.
    ///
    const replace_one& get_replace_one() const;

   private:
    MONGOCXX_PRIVATE void destroy_member() noexcept;

    write_type _type;

    union {
        insert_one _insert_one;
        update_one _update_one;
        update_many _update_many;
        delete_one _delete_one;
        delete_many _delete_many;
        replace_one _replace_one;
    };
};

}  // namespace model
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
