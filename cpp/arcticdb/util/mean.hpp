#include <cstdint>
#include <limits>
#include <type_traits>
#include <cstddef>


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
struct MeanResult {
    T mean;
    size_t count;  // Useful for floating point to know how many non-NaN values
};

template<typename T>
class MeanFinder {
    static_assert(is_supported_type<T>::value, "Unsupported type");

    using VectorType = T __attribute__((vector_size(64)));

public:

    static double find(const T* data, size_t n) {

        using AccumVectorType = double __attribute__((vector_size(64)));

        const size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        const size_t vlen = n / elements_per_vector;

        AccumVectorType sum_vec = {0};
        AccumVectorType count_vec = {0};
        double total_sum = 0;
        size_t valid_count = 0;

        for(size_t i = 0; i < vlen; i++) {
            VectorType v = reinterpret_cast<const VectorType*>(data)[i];

            if constexpr(std::is_floating_point_v<T>) {
                VectorType mask = v == v;  // !NaN
                VectorType valid = v & mask;
                VectorType replaced = VectorType{0} & ~mask;
                v = valid | replaced;

                AccumVectorType count_mask;
                for(size_t j = 0; j < elements_per_vector; j++) {
                    count_mask[j] = reinterpret_cast<const T*>(&mask)[j] != 0 ? 1.0 : 0.0;
                }
                count_vec += count_mask;
            } else {
                count_vec += AccumVectorType{1};
            }

            AccumVectorType v_double;
            for(size_t j = 0; j < elements_per_vector; j++) {
                v_double[j] = static_cast<double>(reinterpret_cast<const T*>(&v)[j]);
            }
            sum_vec += v_double;
        }

        const double* sum_arr = reinterpret_cast<const double*>(&sum_vec);
        const double* count_arr = reinterpret_cast<const double*>(&count_vec);
        for(size_t i = 0; i < elements_per_vector; i++) {
            total_sum += sum_arr[i];
            valid_count += static_cast<size_t>(count_arr[i]);
        }

        const T* remain = data + (vlen * elements_per_vector);
        for(size_t i = 0; i < n % elements_per_vector; i++) {
            if constexpr(std::is_floating_point_v<T>) {
                if (remain[i] == remain[i]) {  // Not NaN
                    total_sum += static_cast<double>(remain[i]);
                    valid_count++;
                }
            } else {
                total_sum += static_cast<double>(remain[i]);
                valid_count++;
            }
        }

        double mean = valid_count > 0 ? total_sum / valid_count : 0.0;
        return mean;
    }
};

template<typename T>
double find_mean(const T* data, size_t n) {
    return MeanFinder<T>::find(data, n);
}
