#pragma once

#include <cstdint>
#include <limits>
#include <type_traits>

// Check compiler support for vector extensions
#if defined(__GNUC__) || defined(__clang__)
#define HAS_VECTOR_EXTENSIONS 1
#else
#define HAS_VECTOR_EXTENSIONS 0
#endif

namespace arcticdb {

#if HAS_VECTOR_EXTENSIONS

template<typename T>
using vector_type __attribute__((vector_size(64))) = T;

template<typename T>
struct is_supported_int : std::false_type {};

template<> struct is_supported_int<int8_t> : std::true_type {};
template<> struct is_supported_int<uint8_t> : std::true_type {};
template<> struct is_supported_int<int16_t> : std::true_type {};
template<> struct is_supported_int<uint16_t> : std::true_type {};
template<> struct is_supported_int<int32_t> : std::true_type {};
template<> struct is_supported_int<uint32_t> : std::true_type {};
template<> struct is_supported_int<int64_t> : std::true_type {};
template<> struct is_supported_int<uint64_t> : std::true_type {};

template<typename T>
struct is_supported_float : std::false_type {};

template<> struct is_supported_float<float> : std::true_type {};
template<> struct is_supported_float<double> : std::true_type {};

template <typename T>
struct is_supported_type : std::integral_constant<bool, is_supported_int<T>::value || is_supported_float<T>::value> {};

#endif

} // namespace arcticdb