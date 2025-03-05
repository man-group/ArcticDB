#include <cstdint>
#include <limits>
#include <type_traits>
#include <cstddef>
#include <algorithm>

#include <arcticdb/util/vector_common.hpp>

namespace arcticdb {
#ifndef _WIN32

template<typename T>
using vector_type __attribute__((vector_size(64))) = T;

template<typename T>
class FloatMinFinder {
    static_assert(is_supported_float<T>::value, "Type must be float or double");
    static_assert(std::is_floating_point_v<T>, "Type must be floating point");

public:
    static T find(const T* data, size_t n) {
        using vec_t = vector_type<T>;

        vec_t vmin;
        for(size_t i = 0; i < sizeof(vec_t)/sizeof(T); i++) {
            reinterpret_cast<T*>(&vmin)[i] = std::numeric_limits<T>::infinity();
        }

        const vec_t* vdata = reinterpret_cast<const vec_t*>(data);
        const size_t elements_per_vector = sizeof(vec_t) / sizeof(T);
        const size_t vlen = n / elements_per_vector;

        for(size_t i = 0; i < vlen; i++) {
            vec_t v = vdata[i];
            vmin = (v < vmin) ? v : vmin;
        }

        T min_val = std::numeric_limits<T>::infinity();
        const T* min_arr = reinterpret_cast<const T*>(&vmin);
        for(size_t i = 0; i < elements_per_vector; i++) {
            if (min_arr[i] == min_arr[i]) {  // Not NaN
                min_val = std::min(min_val, min_arr[i]);
            }
        }

        const T* remain = data + (vlen * elements_per_vector);
        for(size_t i = 0; i < n % elements_per_vector; i++) {
            if (remain[i] == remain[i]) {  // Not NaN
                min_val = std::min(min_val, remain[i]);
            }
        }

        return min_val;
    }
};

template<typename T>
class FloatMaxFinder {
    static_assert(is_supported_float<T>::value, "Type must be float or double");
    static_assert(std::is_floating_point_v<T>, "Type must be floating point");

public:
    static T find(const T* data, size_t n) {
        using vec_t = vector_type<T>;

        vec_t vmax;
        for(size_t i = 0; i < sizeof(vec_t)/sizeof(T); i++) {
            reinterpret_cast<T*>(&vmax)[i] = -std::numeric_limits<T>::infinity();
        }

        const vec_t* vdata = reinterpret_cast<const vec_t*>(data);
        const size_t elements_per_vector = sizeof(vec_t) / sizeof(T);
        const size_t vlen = n / elements_per_vector;

        for(size_t i = 0; i < vlen; i++) {
            vec_t v = vdata[i];
            vmax = (v > vmax) ? v : vmax;
        }

        T max_val = -std::numeric_limits<T>::infinity();
        const T* max_arr = reinterpret_cast<const T*>(&vmax);
        for(size_t i = 0; i < elements_per_vector; i++) {
            if (max_arr[i] == max_arr[i]) {  // Not NaN
                max_val = std::max(max_val, max_arr[i]);
            }
        }

        const T* remain = data + (vlen * elements_per_vector);
        for(size_t i = 0; i < n % elements_per_vector; i++) {
            if (remain[i] == remain[i]) {  // Not NaN
                max_val = std::max(max_val, remain[i]);
            }
        }

        return max_val;
    }
};

template<typename T>
T find_float_min(const T *data, size_t n) {
    return FloatMinFinder<T>::find(data, n);
}

template<typename T>
T find_float_max(const T *data, size_t n) {
    return FloatMaxFinder<T>::find(data, n);
}

#else

template<typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type
find_float_min(const T *data, size_t n) {
    return *std::min_element(data, data + n);
}

template<typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type
find_float_max(const T *data, size_t n) {
    return *std::max_element(data, data + n);
}

#endif
} // namespace arcticdb