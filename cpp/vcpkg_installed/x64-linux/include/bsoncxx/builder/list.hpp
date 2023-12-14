// Copyright 2020 MongoDB Inc.
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

#include <sstream>

#include <bsoncxx/builder/core.hpp>
#include <bsoncxx/exception/error_code.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/types/bson_value/value.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN
namespace builder {
using namespace bsoncxx::types;

///
/// A JSON-like builder for creating documents and arrays.
///
class list {
    using initializer_list_t = std::initializer_list<list>;

   public:
    ///
    /// Creates an empty document.
    ///
    list() : list({}) {}

    ///
    /// Creates a bsoncxx::builder::list from a value of type T. T must be a
    /// bsoncxx::types::bson_value::value or implicitly convertible to a
    /// bsoncxx::types::bson_value::value.
    ///
    /// @param value
    ///     the BSON value
    ///
    /// @see bsoncxx::types::bson_value::value.
    ///
    template <typename T>
    list(T value) : val{value} {}

    ///
    /// Creates a BSON document, if possible. Otherwise, it will create a BSON array. A document is
    /// possible if:
    //      1. The initializer list's size is even; this implies a list of
    //         key-value pairs or an empty document if the size is zero.
    //      2. Each 'key' is a string type. In a list of key-value pairs, the 'key' is every other
    //         element starting at the 0th element.
    //
    /// @param init
    ///     the initializer list used to construct the BSON document or array
    ///
    /// @note
    ///     to enforce the creation of a BSON document or array use the bsoncxx::builder::document
    ///     or bsoncxx::builder::array constructor, respectively.
    ///
    /// @see bsoncxx::builder::document
    /// @see bsoncxx::builder::array
    ///
    list(initializer_list_t init) : list(init, true, true) {}

    ///
    /// Provides a view of the underlying BSON value.
    ///
    /// @see bsoncxx::types::bson_value::view.
    ///
    operator bson_value::view() {
        return view();
    }

    ///
    /// Provides a view of the underlying BSON value.
    ///
    /// @see bsoncxx::types::bson_value::view.
    ///
    bson_value::view view() {
        return val.view();
    }

   private:
    bson_value::value val;

    friend class document;
    friend class array;

    list(initializer_list_t init, bool type_deduction, bool is_array) : val{nullptr} {
        std::stringstream err_msg{"cannot construct document"};
        bool valid_document = false;
        if (type_deduction || !is_array) {
            valid_document = [&] {
                if (init.size() % 2 != 0) {
                    err_msg << " : must be list of key-value pairs";
                    return false;
                }
                for (size_t i = 0; i < init.size(); i += 2) {
                    auto t = (begin(init) + i)->val.view().type();
                    if (t != type::k_utf8) {
                        err_msg << " : all keys must be string type. ";
                        err_msg << "Found type=" << to_string(t);
                        return false;
                    }
                }
                return true;
            }();
        }

        if (valid_document) {
            core _core{false};
            for (size_t i = 0; i < init.size(); i += 2) {
                _core.key_owned(std::string((begin(init) + i)->val.view().get_string().value));
                _core.append((begin(init) + i + 1)->val);
            }
            val = bson_value::value(_core.extract_document());
        } else if (type_deduction || is_array) {
            core _core{true};
            for (auto&& ele : init)
                _core.append(ele.val);
            val = bson_value::value(_core.extract_array());
        } else {
            throw bsoncxx::exception{error_code::k_unmatched_key_in_builder, err_msg.str()};
        }
    }
};

///
/// A JSON-like builder for creating documents.
///
class document : public list {
    using initializer_list_t = std::initializer_list<list>;

   public:
    ///
    /// Creates an empty document.
    ///
    document() : list({}, false, false){};

    ///
    /// Creates a BSON document.
    ///
    /// @param init
    ///     the initializer list used to construct the BSON document
    ///
    /// @see bsoncxx::builder::list
    /// @see bsoncxx::builder::array
    ///
    document(initializer_list_t init) : list(init, false, false) {}
};

///
/// A JSON-like builder for creating arrays.
///
class array : public list {
    using initializer_list_t = std::initializer_list<list>;

   public:
    ///
    /// Creates an empty array.
    ///
    array() : list({}, false, true){};

    ///
    /// Creates a BSON array.
    ///
    /// @param init
    ///     the initializer list used to construct the BSON array
    ///
    /// @see bsoncxx::builder::list
    /// @see bsoncxx::builder::document
    ///
    array(initializer_list_t init) : list(init, false, true) {}
};
}  // namespace builder
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx
