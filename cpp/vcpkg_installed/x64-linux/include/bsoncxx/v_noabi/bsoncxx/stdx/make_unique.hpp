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

#include <memory>

#include <bsoncxx/config/prelude.hpp>

#pragma push_macro("BSONCXX_DETAIL_USE_STD_MAKE_UNIQUE")
#undef BSONCXX_DETAIL_USE_STD_MAKE_UNIQUE

#if (defined(__cplusplus) && __cplusplus >= 201402L) || \
    (defined(_MSVC_LANG) && _MSVC_LANG >= 201402L)
#define BSONCXX_DETAIL_USE_STD_MAKE_UNIQUE
#endif

// Only define bsoncxx implementation details when necessary.
#if !defined(BSONCXX_DETAIL_USE_STD_MAKE_UNIQUE) || !defined(__cpp_lib_smart_ptr_for_overwrite)

#include <cstddef>
#include <type_traits>
#include <utility>

#include <bsoncxx/stdx/type_traits.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace stdx {
namespace detail {

// Switch backend of make_unique by the type we are creating.
// It would be easier to 'if constexpr' on whether we are an array and whether to direct-init or
// value-init, but we don't have if-constexpr and we need it to guard against an uterance of a
// possibly-illegal 'new' expression.
template <typename T>
struct make_unique_impl {
    // For make_unique:
    template <typename... Args,
              // Guard on constructible-from:
              typename = decltype(new T(std::declval<Args>()...))>
    static std::unique_ptr<T> make(std::true_type /* direct-init */, Args&&... args) {
        return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
    }

    // For make_unique_for_overwrite:
    template <typename U = T,
              // Guard on whether T is value-initializable:
              // (Hide behind a deduced 'U' to defer the evaluation of
              // this default template argument until overload resolution)
              typename = decltype(new U)>
    static std::unique_ptr<T> make(std::false_type /* value-init */) {
        return std::unique_ptr<T>(new T);
    }
};

// For unbounded arrays:
template <typename Elem>
struct make_unique_impl<Elem[]> {
    template <typename ShouldDirectInit,
              // Guard on whether the new-expression will be legal:
              typename = decltype(new Elem[std::declval<std::size_t>()])>
    static std::unique_ptr<Elem[]> make(ShouldDirectInit, std::size_t count) {
        // These can share a function via a plain if, because both new expressions
        // must be semantically valid
        if (ShouldDirectInit()) {
            return std::unique_ptr<Elem[]>(new Elem[count]());
        } else {
            return std::unique_ptr<Elem[]>(new Elem[count]);
        }
    }
};

// Bounded arrays are disallowed:
template <typename Elem, std::size_t N>
struct make_unique_impl<Elem[N]> {};

// References are nonsense:
template <typename T>
struct make_unique_impl<T&> {};

// References are nonsense:
template <typename T>
struct make_unique_impl<T&&> {};

}  // namespace detail
}  // namespace stdx
}  // namespace v_noabi
}  // namespace bsoncxx

#endif  // !defined(BSONCXX_DETAIL_USE_STD_MAKE_UNIQUE) ||
        // !defined(__cpp_lib_smart_ptr_for_overwrite)

namespace bsoncxx {
namespace v_noabi {
namespace stdx {

// Unlike other C++17 polyfill features, this is a C++14 feature.
// Use feature testing rather than polyfill library selection macros.
#if defined(BSONCXX_DETAIL_USE_STD_MAKE_UNIQUE)
using ::std::make_unique;
#else

/// Equivalent to `std::make_unique<T>(args...)` where `T` is a non-array type.
template <typename T,
          typename... Args,
          typename Impl = detail::make_unique_impl<T>,
          typename std::enable_if<!std::is_array<T>::value,
                                  decltype(Impl::make(std::true_type{}, std::declval<Args>()...),
                                           void())>::type* = nullptr>
std::unique_ptr<T> make_unique(Args&&... args) {
    return Impl::make(std::true_type{}, std::forward<Args>(args)...);
}

/// Equivalent to `std::make_unique<T>(count)` where `T` is an array type.
template <
    typename T,
    typename Impl = detail::make_unique_impl<T>,
    typename std::enable_if<std::is_array<T>::value,
                            decltype(Impl::make(std::true_type{}, std::declval<std::size_t>()),
                                     void())>::type* = nullptr>
std::unique_ptr<T> make_unique(std::size_t count) {
    return Impl::make(std::true_type{}, count);
}

#endif

// Unlike other C++17 polyfill features, this is a C++20 feature.
// Use feature testing rather than polyfill library selection macros.
#if defined(__cpp_lib_smart_ptr_for_overwrite)
using ::std::make_unique_for_overwrite;
#else

/// Equivalent to `std::make_unique_for_overwrite<T>()` where `T` is a non-array type.
template <typename T,
          typename Impl = detail::make_unique_impl<T>,
          typename std::enable_if<!std::is_array<T>::value,
                                  decltype(Impl::make(std::false_type{}), void())>::type* = nullptr>
std::unique_ptr<T> make_unique_for_overwrite() {
    return Impl::make(std::false_type{});
}

/// Equivalent to `std::make_unique_for_overwrite<T>(count)` where `T` is an array type.
template <
    typename T,
    typename Impl = detail::make_unique_impl<T>,
    typename std::enable_if<std::is_array<T>::value,
                            decltype(Impl::make(std::false_type{}, std::declval<std::size_t>()),
                                     void())>::type* = nullptr>
std::unique_ptr<T> make_unique_for_overwrite(std::size_t count) {
    return Impl::make(std::false_type{}, count);
}

#endif

}  // namespace stdx
}  // namespace v_noabi
}  // namespace bsoncxx

#pragma pop_macro("BSONCXX_DETAIL_USE_STD_MAKE_UNIQUE")

#include <bsoncxx/config/postlude.hpp>

namespace bsoncxx {
namespace stdx {

using ::bsoncxx::v_noabi::stdx::make_unique;
using ::bsoncxx::v_noabi::stdx::make_unique_for_overwrite;

}  // namespace stdx
}  // namespace bsoncxx
