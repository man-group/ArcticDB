#include <cstdint>
#include <limits>
#include <type_traits>
#include <cstddef>

#include <arcticdb/util/vector_common.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

#if HAS_VECTOR_EXTENSIONS

template<typename Vec, typename U>
inline void fill_vector(Vec &vec, U value) {
    U* p = reinterpret_cast<U*>(&vec);
    constexpr size_t count = sizeof(Vec) / sizeof(U);
    for (size_t i = 0; i < count; i++) {
        p[i] = value;
    }
}

template<typename T>
class MeanFinder {
    static_assert(is_supported_int<T>::value || is_supported_float<T>::value, "Unsupported type");
public:
    static double find(const T* __restrict data, size_t n) {
        util::check(n > 0, "Empty array provided");
        using VectorType = vector_type<T>;
        using AccumVectorType = vector_type<double>;
        constexpr size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        constexpr size_t doubles_per_vector = sizeof(AccumVectorType) / sizeof(double);
        constexpr size_t vectors_per_acc = elements_per_vector / doubles_per_vector;
        AccumVectorType vsum;
        double* vsum_ptr = reinterpret_cast<double*>(&vsum);
        fill_vector<AccumVectorType, double>(vsum, 0.0);
        size_t valid_count = 0;
        const VectorType* vdata = reinterpret_cast<const VectorType*>(data);
        const size_t vector_len = n / elements_per_vector;
        for (size_t i = 0; i < vector_len; i++) {
            VectorType v = vdata[i];
            const T* v_arr = reinterpret_cast<const T*>(&v);
            if constexpr (std::is_floating_point_v<T>) {
                bool has_nan = false;
                for (size_t j = 0; j < elements_per_vector; j++) {
                    if (std::isnan(v_arr[j])) { has_nan = true; break; }
                }
                if (!has_nan) {
                    for (size_t chunk = 0; chunk < vectors_per_acc; chunk++) {
                        size_t base = chunk * doubles_per_vector;
                        for (size_t j = 0; j < doubles_per_vector; j++) {
                            if constexpr (std::is_same_v<T, double>)
                                vsum_ptr[j] += v_arr[base + j];
                            else
                                vsum_ptr[j] += static_cast<double>(v_arr[base + j]);
                        }
                    }
                    valid_count += elements_per_vector;
                } else {
                    for (size_t chunk = 0; chunk < vectors_per_acc; chunk++) {
                        size_t base = chunk * doubles_per_vector;
                        for (size_t j = 0; j < doubles_per_vector; j++) {
                            size_t idx = base + j;
                            if (!std::isnan(v_arr[idx])) {
                                if constexpr (std::is_same_v<T, double>)
                                    vsum_ptr[j] += v_arr[idx];
                                else
                                    vsum_ptr[j] += static_cast<double>(v_arr[idx]);
                                valid_count++;
                            }
                        }
                    }
                }
            } else {
                for (size_t chunk = 0; chunk < vectors_per_acc; chunk++) {
                    size_t base = chunk * doubles_per_vector;
                    for (size_t j = 0; j < doubles_per_vector; j++) {
                        vsum_ptr[j] += static_cast<double>(v_arr[base + j]);
                    }
                }
                valid_count += elements_per_vector;
            }
        }
        double total = 0.0;
        for (size_t j = 0; j < doubles_per_vector; j++) {
            total += vsum_ptr[j];
        }
        const T* remain = data + (vector_len * elements_per_vector);
        size_t rem = n % elements_per_vector;
        for (size_t i = 0; i < rem; i++) {
            if constexpr (std::is_floating_point_v<T>) {
                if (!std::isnan(remain[i])) {
                    if constexpr (std::is_same_v<T, double>)
                        total += remain[i];
                    else
                        total += static_cast<double>(remain[i]);
                    valid_count++;
                }
            } else {
                total += static_cast<double>(remain[i]);
                valid_count++;
            }
        }
        return valid_count > 0 ? total / static_cast<double>(valid_count) : 0.0;
    }
};

template<typename T>
double find_mean(const T* __restrict data, size_t n) {
    return MeanFinder<T>::find(data, n);
}

#else

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
double find_mean(const T* data, std::size_t size) {
    util::check(size > 0, "Cannot compute mean of an empty array.");
    double sum = std::accumulate(data, data + size, 0.0);
    return sum / size;
}

#endif

} // namespace arcticdb