#include <cstdint>
#include <limits>
#include <type_traits>
#include <cmath>

namespace arcticdb {

#include <cstdint>
#include <limits>
#include <type_traits>

// Check compiler support for vector extensions
#if defined(__GNUC__) || defined(__clang__)
#define HAS_VECTOR_EXTENSIONS 1
#else
#define HAS_VECTOR_EXTENSIONS 0
#endif

#include <cstdint>
#include <limits>
#include <type_traits>

template<typename T>
using vector_type = T __attribute__((vector_size(64)));

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
struct MinMax {
    T min;
    T max;
};

template<typename T>
class MinMaxFinder {
    static_assert(is_supported_int<T>::value, "Type must be integer");
    static_assert(std::is_integral_v<T>, "Type must be integral");

public:
    static MinMax<T> find(const T* data, size_t n) {
        using vec_t = vector_type<T>;

        vec_t vmin, vmax;
        T min_val, max_val;

        if constexpr(std::is_signed_v<T>) {
            min_val = std::numeric_limits<T>::max();
            max_val = std::numeric_limits<T>::min();
        } else {
            min_val = std::numeric_limits<T>::max();
            max_val = 0;
        }

        for(size_t i = 0; i < sizeof(vec_t)/sizeof(T); i++) {
            reinterpret_cast<T*>(&vmin)[i] = min_val;
            reinterpret_cast<T*>(&vmax)[i] = max_val;
        }

        const vec_t* vdata = reinterpret_cast<const vec_t*>(data);
        const size_t elements_per_vector = sizeof(vec_t) / sizeof(T);
        const size_t vlen = n / elements_per_vector;

        for(size_t i = 0; i < vlen; i++) {
            vec_t v = vdata[i];
            vmin = (v < vmin) ? v : vmin;
            vmax = (v > vmax) ? v : vmax;
        }

        const T* min_arr = reinterpret_cast<const T*>(&vmin);
        const T* max_arr = reinterpret_cast<const T*>(&vmax);

        min_val = min_arr[0];
        max_val = max_arr[0];
        for(size_t i = 1; i < elements_per_vector; i++) {
            min_val = std::min(min_val, min_arr[i]);
            max_val = std::max(max_val, max_arr[i]);
        }

        const T* remain = data + (vlen * elements_per_vector);
        for(size_t i = 0; i < n % elements_per_vector; i++) {
            min_val = std::min(min_val, remain[i]);
            max_val = std::max(max_val, remain[i]);
        }

        return {min_val, max_val};
    }
};

template<typename T>
class MinFinder {
    static_assert(is_supported_int<T>::value, "Type must be integer");
    static_assert(std::is_integral_v<T>, "Type must be integral");

public:
    static T find(const T* data, size_t n) {
        using vec_t = vector_type<T>;

        vec_t vmin;
        T min_val = std::numeric_limits<T>::max();

        for(size_t i = 0; i < sizeof(vec_t)/sizeof(T); i++) {
            reinterpret_cast<T*>(&vmin)[i] = min_val;
        }

        const vec_t* vdata = reinterpret_cast<const vec_t*>(data);
        const size_t elements_per_vector = sizeof(vec_t) / sizeof(T);
        const size_t vlen = n / elements_per_vector;

        for(size_t i = 0; i < vlen; i++) {
            vec_t v = vdata[i];
            vmin = (v < vmin) ? v : vmin;
        }

        const T* min_arr = reinterpret_cast<const T*>(&vmin);
        min_val = min_arr[0];
        for(size_t i = 1; i < elements_per_vector; i++) {
            min_val = std::min(min_val, min_arr[i]);
        }

        const T* remain = data + (vlen * elements_per_vector);
        for(size_t i = 0; i < n % elements_per_vector; i++) {
            min_val = std::min(min_val, remain[i]);
        }

        return min_val;
    }
};

template<typename T>
class MaxFinder {
    static_assert(is_supported_int<T>::value, "Type must be integer");
    static_assert(std::is_integral_v<T>, "Type must be integral");

public:
    static T find(const T* data, size_t n) {
        using vec_t = vector_type<T>;

        vec_t vmax;
        T max_val;

        if constexpr(std::is_signed_v<T>) {
            max_val = std::numeric_limits<T>::min();
        } else {
            max_val = 0;
        }

        for(size_t i = 0; i < sizeof(vec_t)/sizeof(T); i++) {
            reinterpret_cast<T*>(&vmax)[i] = max_val;
        }

        const vec_t* vdata = reinterpret_cast<const vec_t*>(data);
        const size_t elements_per_vector = sizeof(vec_t) / sizeof(T);
        const size_t vlen = n / elements_per_vector;

        for(size_t i = 0; i < vlen; i++) {
            vec_t v = vdata[i];
            vmax = (v > vmax) ? v : vmax;
        }

        const T* max_arr = reinterpret_cast<const T*>(&vmax);
        max_val = max_arr[0];
        for(size_t i = 1; i < elements_per_vector; i++) {
            max_val = std::max(max_val, max_arr[i]);
        }

        const T* remain = data + (vlen * elements_per_vector);
        for(size_t i = 0; i < n % elements_per_vector; i++) {
            max_val = std::max(max_val, remain[i]);
        }

        return max_val;
    }
};

template<typename T>
MinMax<T> find_min_max(const T* data, size_t n) {
    return MinMaxFinder<T>::find(data, n);
}

template<typename T>
T find_min(const T* data, size_t n) {
    return MinFinder<T>::find(data, n);
}

template<typename T>
T find_max(const T* data, size_t n) {
    return MaxFinder<T>::find(data, n);
}


} // namespace arcticdb