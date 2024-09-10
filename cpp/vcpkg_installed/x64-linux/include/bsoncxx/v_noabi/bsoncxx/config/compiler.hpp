// Copyright 2015 MongoDB Inc.
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

// clang-format off

#define BSONCXX_IF_MSVC(...)
#define BSONCXX_IF_GCC(...)
#define BSONCXX_IF_CLANG(...)
#define BSONCXX_IF_GNU_LIKE(...) \
    BSONCXX_IF_GCC(__VA_ARGS__) \
    BSONCXX_IF_CLANG(__VA_ARGS__)

#ifdef __GNUC__
    #ifdef __clang__
        #undef BSONCXX_IF_CLANG
        #define BSONCXX_IF_CLANG(...) __VA_ARGS__
    #else
        #undef BSONCXX_IF_GCC
        #define BSONCXX_IF_GCC(...) __VA_ARGS__
    #endif
#elif defined(_MSC_VER)
    #undef BSONCXX_IF_MSVC
    #define BSONCXX_IF_MSVC(...) __VA_ARGS__
#endif

// clang-format on

// Disable MSVC warnings that cause a lot of noise related to DLL visibility
// for types that we don't control (like std::unique_ptr).
BSONCXX_PUSH_WARNINGS();
BSONCXX_DISABLE_WARNING(MSVC(4251));
BSONCXX_DISABLE_WARNING(MSVC(5275));

#define BSONCXX_INLINE inline BSONCXX_PRIVATE
#define BSONCXX_CALL BSONCXX_IF_MSVC(__cdecl)
