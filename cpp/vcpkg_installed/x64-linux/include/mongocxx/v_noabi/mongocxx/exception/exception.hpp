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

#include <string>
#include <system_error>

#include <mongocxx/exception/exception-fwd.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {

///
/// A class to be used as the base class for all mongocxx exceptions.
///
class exception : public std::system_error {
    using system_error::system_error;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
