#pragma once

#include <cstdint>
#include <limits>
#include <type_traits>
#include <cstdint>
#include <cstddef>

#include <arcticdb/util/vector_common.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

#if HAS_VECTOR_EXTENSIONS

template<typename Vec, typename Elem>
inline void fill_vector(Vec &vec, Elem value) {
    Elem* p = reinterpret_cast<Elem*>(&vec);
    constexpr size_t count = sizeof(Vec) / sizeof(Elem);
    for (size_t i = 0; i < count; ++i) {
        p[i] = value;
    }
}

template<typename T>
class SumFinder {
    static_assert(is_supported_type<T>::value, "Unsupported type");
    static_assert(std::is_arithmetic_v<T>, "Type must be numeric");
public:
    static double find(const T* data, size_t n) {
        util::check(n > 0, "Empty array provided");

        using VectorType __attribute__((vector_size(64))) = T;
        using acc_vec_t __attribute__((vector_size(64))) = double;

        constexpr size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        constexpr size_t doubles_per_vector  = sizeof(acc_vec_t) / sizeof(double);
        constexpr size_t group_count = elements_per_vector / doubles_per_vector;

        acc_vec_t vsum;
        fill_vector<acc_vec_t, double>(vsum, 0.0);

        const VectorType* vdata = reinterpret_cast<const VectorType*>(data);
        const size_t vector_len = n / elements_per_vector;

        for (size_t i = 0; i < vector_len; i++) {
            VectorType v = vdata[i];
            const T* v_arr = reinterpret_cast<const T*>(&v);
            double* acc = reinterpret_cast<double*>(&vsum);
            for (size_t j = 0; j < doubles_per_vector; j++) {
                double group_sum = 0.0;
                for (size_t k = 0; k < group_count; k++) {
                    size_t idx = j * group_count + k;
                    group_sum += static_cast<double>(v_arr[idx]);
                }
                acc[j] += group_sum;
            }
        }

        double total = 0.0;
        const double* acc = reinterpret_cast<const double*>(&vsum);
        for (size_t j = 0; j < doubles_per_vector; j++) {
            total += acc[j];
        }

        const T* remain = data + (vector_len * elements_per_vector);
        size_t rem = n % elements_per_vector;
        for (size_t i = 0; i < rem; i++) {
            total += static_cast<double>(remain[i]);
        }

        return total;
    }
};

template<typename T>
double find_sum(const T* data, size_t n) {
    return SumFinder<T>::find(data, n);
}

#else

template<typename T>
double find_sum(const T *data, size_t n) {
    util::check(size != 0, "Got zero size in find_sum");
    return std::accumulate(data, data + n, T(0));
}

#endif
} // namespace arcticdb