#include <cstdint>
#include <limits>
#include <type_traits>
#include <cmath>

template<typename T>
using vector_type = T __attribute__((vector_size(64)));

template<typename T>
struct MinMax {
    T min;
    T max;
};

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
class MinMaxFinder {
    static_assert(is_supported_type<T>::value, "Unsupported type");

public:
    static MinMax<T> find(const T* data, size_t n) {
        using VectorType = vector_type<T>;

        VectorType vec_min;
        VectorType vec_max;
        T min_val;
        T max_val;

        if constexpr(std::is_floating_point_v<T>) {
            vec_min = VectorType{std::numeric_limits<T>::infinity()};
            vec_max = VectorType{-std::numeric_limits<T>::infinity()};
            min_val = std::numeric_limits<T>::infinity();
            max_val = -std::numeric_limits<T>::infinity();
        }
        else if constexpr(std::is_signed_v<T>) {
            vec_min = VectorType{std::numeric_limits<T>::max()};
            vec_max = VectorType{std::numeric_limits<T>::min()};
            min_val = std::numeric_limits<T>::max();
            max_val = std::numeric_limits<T>::min();
        } else {
            vec_min = VectorType{std::numeric_limits<T>::max()};
            vec_max = VectorType{0};
            min_val = std::numeric_limits<T>::max();
            max_val = 0;
        }

        const VectorType* vdata = reinterpret_cast<const VectorType*>(data);
        const size_t elements_per_vector = sizeof(VectorType) / sizeof(T);
        const size_t vlen = n / elements_per_vector;

        for(size_t i = 0; i < vlen; i++) {
            VectorType v = vdata[i];
            if constexpr(std::is_floating_point_v<T>) {
                VectorType mask = v == v;  
                v = __builtin_choose_expr(mask, v, vec_min);
                vec_min = __builtin_min(vec_min, v);
                vec_max = __builtin_max(vec_max, v);
            } else {
                vec_min = __builtin_min(vec_min, v);
                vec_max = __builtin_max(vec_max, v);
            }
        }

        const T* min_arr = reinterpret_cast<const T*>(&vec_min);
        const T* max_arr = reinterpret_cast<const T*>(&vec_max);

        for(size_t i = 0; i < elements_per_vector; i++) {
            if constexpr(std::is_floating_point_v<T>) {
                // Skip NaN values in reduction
                if (min_arr[i] == min_arr[i]) {  // Not NaN
                    min_val = std::min(min_val, min_arr[i]);
                }
                if (max_arr[i] == max_arr[i]) {  // Not NaN
                    max_val = std::max(max_val, max_arr[i]);
                }
            } else {
                min_val = std::min(min_val, min_arr[i]);
                max_val = std::max(max_val, max_arr[i]);
            }
        }

        const T* remain = data + (vlen * elements_per_vector);
        for(size_t i = 0; i < n % elements_per_vector; i++) {
            if constexpr(std::is_floating_point_v<T>) {
                if (remain[i] == remain[i]) {  //  !NaN
                    min_val = std::min(min_val, remain[i]);
                    max_val = std::max(max_val, remain[i]);
                }
            } else {
                min_val = std::min(min_val, remain[i]);
                max_val = std::max(max_val, remain[i]);
            }
        }

        return {min_val, max_val};
    }
};

template<typename T>
MinMax<T> find_min_max(const T* data, size_t n) {
    return MinMaxFinder<T>::find(data, n);
}
