#include <cstdint>
#include <limits>
#include <type_traits>
#include <cmath>

#include <cstdint>
#include <limits>
#include <type_traits>

#include <arcticdb/util/vector_common.hpp>

namespace arcticdb {

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
        using VectorType = vector_type<T>;

        VectorType vector_min;
        VectorType vector_max;
        T min_val;
        T max_val;

        if constexpr(std::is_signed_v<T>) {
            min_val = std::numeric_limits<T>::max();
            max_val = std::numeric_limits<T>::min();
        } else {
            min_val = std::numeric_limits<T>::max();
            max_val = 0;
        }

        for(size_t i = 0; i < sizeof(VectorType)/sizeof(T); i++) {
            reinterpret_cast<T*>(&vector_min)[i] = min_val;
            reinterpret_cast<T*>(&vector_max)[i] = max_val;
        }

        const auto* vdata = reinterpret_cast<const VectorType*>(data);
        const size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        const size_t vector_len = n / elements_per_vector;

        for(size_t i = 0; i < vector_len; i++) {
            VectorType v = vdata[i];
            vector_min = (v < vector_min) ? v : vector_min;
            vector_max = (v > vector_max) ? v : vector_max;
        }

        const T* min_arr = reinterpret_cast<const T*>(&vector_min);
        const T* max_arr = reinterpret_cast<const T*>(&vector_max);

        min_val = min_arr[0];
        max_val = max_arr[0];
        for(size_t i = 1; i < elements_per_vector; i++) {
            min_val = std::min(min_val, min_arr[i]);
            max_val = std::max(max_val, max_arr[i]);
        }

        const T* remain = data + (vector_len * elements_per_vector);
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
        using VectorType = vector_type<T>;

        VectorType vector_min;
        T min_val = std::numeric_limits<T>::max();

        for(size_t i = 0; i < sizeof(VectorType)/sizeof(T); i++) {
            reinterpret_cast<T*>(&vector_min)[i] = min_val;
        }

        const auto* vdata = reinterpret_cast<const VectorType*>(data);
        const size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        const size_t vector_len = n / elements_per_vector;

        for(size_t i = 0; i < vector_len; i++) {
            VectorType v = vdata[i];
            vector_min = (v < vector_min) ? v : vector_min;
        }

        const T* min_arr = reinterpret_cast<const T*>(&vector_min);
        min_val = min_arr[0];
        for(size_t i = 1; i < elements_per_vector; i++) {
            min_val = std::min(min_val, min_arr[i]);
        }

        const T* remain = data + (vector_len * elements_per_vector);
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
        using VectorType = vector_type<T>;

        VectorType vector_max;
        T max_val;

        if constexpr(std::is_signed_v<T>) {
            max_val = std::numeric_limits<T>::min();
        } else {
            max_val = 0;
        }

        for(size_t i = 0; i < sizeof(VectorType)/sizeof(T); i++) {
            reinterpret_cast<T*>(&vector_max)[i] = max_val;
        }

        const auto* vdata = reinterpret_cast<const VectorType*>(data);
        const size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        const size_t vector_len = n / elements_per_vector;

        for(size_t i = 0; i < vector_len; i++) {
            VectorType v = vdata[i];
            vector_max = (v > vector_max) ? v : vector_max;
        }

        const auto* max_arr = reinterpret_cast<const T*>(&vector_max);
        max_val = max_arr[0];
        for(size_t i = 1; i < elements_per_vector; i++) {
            max_val = std::max(max_val, max_arr[i]);
        }

        const T* remain = data + (vector_len * elements_per_vector);
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