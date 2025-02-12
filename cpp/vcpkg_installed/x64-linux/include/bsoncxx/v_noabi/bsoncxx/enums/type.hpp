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

#ifndef BSONCXX_ENUM
#error "This header is only meant to be included as an X-macro over BSONCXX_ENUM"
#endif

BSONCXX_ENUM(double, 0x01)
BSONCXX_ENUM(string, 0x02)
BSONCXX_ENUM(document, 0x03)
BSONCXX_ENUM(array, 0x04)
BSONCXX_ENUM(binary, 0x05)
BSONCXX_ENUM(undefined, 0x06)
BSONCXX_ENUM(oid, 0x07)
BSONCXX_ENUM(bool, 0x08)
BSONCXX_ENUM(date, 0x09)
BSONCXX_ENUM(null, 0x0A)
BSONCXX_ENUM(regex, 0x0B)
BSONCXX_ENUM(dbpointer, 0x0C)
BSONCXX_ENUM(code, 0x0D)
BSONCXX_ENUM(symbol, 0x0E)
BSONCXX_ENUM(codewscope, 0x0F)
BSONCXX_ENUM(int32, 0x10)
BSONCXX_ENUM(timestamp, 0x11)
BSONCXX_ENUM(int64, 0x12)
BSONCXX_ENUM(decimal128, 0x13)
BSONCXX_ENUM(maxkey, 0x7F)
BSONCXX_ENUM(minkey, 0xFF)
