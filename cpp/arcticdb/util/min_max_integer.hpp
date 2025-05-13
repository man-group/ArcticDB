#include <cstdint>
#include <limits>
#include <type_traits>
#include <cmath>
#include <algorithm>

#include <arcticdb/util/vector_common.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

#if HAS_VECTOR_EXTENSIONS
template<typename T>
struct MinMax {
    T min;
    T max;
};

template<bool ComputeMin, bool ComputeMax, typename T>
class ExtremumFinder {
public:
    // Return type depends on what is computed.
    using ReturnType = std::conditional_t<
        (ComputeMin && ComputeMax),
        MinMax<T>,
        T
    >;

    static ReturnType find(const T* data, size_t n) {
        util::check(n > 0, "Empty array provided");
        static_assert(is_supported_int<T>::value, "Type must be integer");
        static_assert(std::is_integral_v<T>, "Type must be integral");

        using VectorType = vector_type<T>;
        constexpr size_t lane_count = sizeof(VectorType) / sizeof(T);

        T init_min = std::numeric_limits<T>::max();
        T init_max = std::numeric_limits<T>::min();

        VectorType vector_min, vector_max;
        if constexpr (ComputeMin) {
            T* lanes = reinterpret_cast<T*>(&vector_min);
            for (size_t i = 0; i < lane_count; i++) {
                lanes[i] = init_min;
            }
        }
        if constexpr (ComputeMax) {
            T* lanes = reinterpret_cast<T*>(&vector_max);
            for (size_t i = 0; i < lane_count; i++) {
                lanes[i] = init_max;
            }
        }

        size_t num_vectors = n / lane_count;
        const VectorType* vdata = reinterpret_cast<const VectorType*>(data);
        for (size_t i = 0; i < num_vectors; i++) {
            VectorType v = vdata[i];
            if constexpr (ComputeMin) {
                vector_min = (v < vector_min) ? v : vector_min;
            }
            if constexpr (ComputeMax) {
                vector_max = (v > vector_max) ? v : vector_max;
            }
        }

        T final_min = init_min;
        T final_max = init_max;
        if constexpr (ComputeMin) {
            const T* lanes = reinterpret_cast<const T*>(&vector_min);
            for (size_t i = 0; i < lane_count; i++) {
                final_min = std::min(final_min, lanes[i]);
            }
        }
        if constexpr (ComputeMax) {
            const T* lanes = reinterpret_cast<const T*>(&vector_max);
            for (size_t i = 0; i < lane_count; i++) {
                final_max = std::max(final_max, lanes[i]);
            }
        }

        const T* remain = data + (num_vectors * lane_count);
        size_t rem = n % lane_count;
        for (size_t i = 0; i < rem; i++) {
            if constexpr (ComputeMin)
                final_min = std::min(final_min, remain[i]);
            if constexpr (ComputeMax)
                final_max = std::max(final_max, remain[i]);
        }

        if constexpr (ComputeMin && ComputeMax) {
            return MinMax<T>{ final_min, final_max };
        } else if constexpr (ComputeMin) {
            return final_min;
        } else {
            return final_max;
        }
    }
};

template<typename T>
T find_min(const T* data, size_t n) {
    return ExtremumFinder<true, false, T>::find(data, n);
}

template<typename T>
T find_max(const T* data, size_t n) {
    return ExtremumFinder<false, true, T>::find(data, n);
}

template<typename T>
MinMax<T> find_min_max(const T* data, size_t n) {
    return ExtremumFinder<true, true, T>::find(data, n);
}

#else

template<typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type
find_min(const T *data, size_t n) {
    util::check(n != 0, "Got zero size in find_min");
    return *std::min_element(data, data + n);
}

template<typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type
find_max(const T *data, size_t n) {
    util::check(n != 0, "Got zero size in find_max");
    return *std::max_element(data, data + n);
}

#endif

} // namespace arcticdb