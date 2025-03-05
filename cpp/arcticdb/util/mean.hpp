#include <cstdint>
#include <limits>
#include <type_traits>
#include <cstddef>

#include <arcticdb/util/vector_common.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

#ifndef _WIN32

template<typename T>
class MeanFinder {
    static_assert(is_supported_int<T>::value || is_supported_float<T>::value, "Unsupported type");

public:
    static double find(const T* data, size_t n) {
        using VectorType = vector_type<T>;
        using AccumVectorType = vector_type<double>;

        AccumVectorType vsum = {0.0};
        const size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        const size_t doubles_per_vector = sizeof(AccumVectorType) / sizeof(double);
        const size_t vectors_per_acc = elements_per_vector / doubles_per_vector;

        size_t valid_count = 0;

        const auto* vdata = reinterpret_cast<const VectorType*>(data);
        const size_t vector_len = n / elements_per_vector;

        for(size_t i = 0; i < vector_len; i++) {
            VectorType v = vdata[i];

            if constexpr(std::is_floating_point_v<T>) {
                VectorType mask = v == v;
                v = v & mask;

                const T* mask_arr = reinterpret_cast<const T*>(&mask);
                for(size_t j = 0; j < elements_per_vector; j++) {
                    if(mask_arr[j] != 0) valid_count++;
                }
            } else {
                valid_count += elements_per_vector;
            }

            const T* v_arr = reinterpret_cast<const T*>(&v);
            for(size_t chunk = 0; chunk < vectors_per_acc; chunk++) {
                for(size_t j = 0; j < doubles_per_vector; j++) {
                    size_t idx = chunk * doubles_per_vector + j;
                    reinterpret_cast<double*>(&vsum)[j] += static_cast<double>(v_arr[idx]);
                }
            }
        }

        double total = 0.0;
        const auto* sum_arr = reinterpret_cast<const double*>(&vsum);
        for(size_t i = 0; i < doubles_per_vector; i++) {
            total += sum_arr[i];
        }

        const T* remain = data + (vector_len * elements_per_vector);
        for(size_t i = 0; i < n % elements_per_vector; i++) {
            if constexpr(std::is_floating_point_v<T>) {
                if (remain[i] == remain[i]) {  // Not NaN
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
double find_mean(const T *data, size_t n) {
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