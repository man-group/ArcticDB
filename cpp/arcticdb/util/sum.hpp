#pragma once

#include <cstdint>
#include <limits>
#include <type_traits>
#include <cstdint>
#include <cstddef>

namespace arcticdb {

#ifdef _WIN32

template<typename T>
struct is_supported_type : std::false_type {};

template<> struct is_supported_type<int8_t> : std::true_type {};
template<> struct is_supported_type<uint8_t> : std::true_type {};
template<> struct is_supported_type<int16_t> : std::true_type {};
template<> struct is_supported_type<uint16_t> : std::true_type {};
template<> struct is_supported_type<int32_t> : std::true_type {};
template<> struct is_supported_type<uint32_t> : std::true_type {};
template<> struct is_supported_type<int64_t> : std::true_type {};
template<> struct is_supported_type<uint64_t> : std::true_type {};
template<> struct is_supported_type<float> : std::true_type {};
template<> struct is_supported_type<double> : std::true_type {};

template<typename T>
class SumFinder {
    static_assert(is_supported_type<T>::value, "Unsupported type");
    static_assert(std::is_arithmetic_v<T>, "Type must be numeric");

public:
    static double find(const T *data, size_t n) {
        using VectorType __attribute__((vector_size(64))) = T;
        using acc_vec_t __attribute((vector_size(64))) = double;

        acc_vec_t vsum = {0.0};
        const size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        const size_t doubles_per_vector = sizeof(acc_vec_t) / sizeof(double);

        const VectorType *vdata = reinterpret_cast<const VectorType*>(data);
        const size_t vector_len = n / elements_per_vector;

        for (size_t i = 0; i < vector_len; i++) {
            VectorType v = vdata[i];

            const T *v_arr = reinterpret_cast<const T *>(&v);
            for (size_t j = 0; j < elements_per_vector; j++) {
                size_t acc_idx = j % doubles_per_vector;
                reinterpret_cast<double *>(&vsum)[acc_idx] +=
                    static_cast<double>(v_arr[j]);
            }
        }

        double total = 0.0;
        const double *sum_arr = reinterpret_cast<const double *>(&vsum);
        for (size_t i = 0; i < doubles_per_vector; i++) {
            total += sum_arr[i];
        }

        const T *remain = data + (vector_len * elements_per_vector);
        for (size_t i = 0; i < n % elements_per_vector; i++) {
            total += static_cast<double>(remain[i]);
        }

        return total;
    }
};

template<typename T>
double find_sum(const T *data, size_t n) {
    return SumFinder<T>::find(data, n);
}


#else

template<typename T>
double find_sum(const T *data, size_t n) {
    return std::accumulate(data, data + n, T(0));
}

#endif
} // namespace arcticdb