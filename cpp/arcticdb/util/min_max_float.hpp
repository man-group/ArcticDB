#include <cstdint>
#include <limits>
#include <type_traits>
#include <cstddef>
#include <algorithm>

#include <arcticdb/util/vector_common.hpp>

namespace arcticdb {

#if HAS_VECTOR_EXTENSIONS

template<typename T, typename Comparator>
class FloatExtremumFinder {
    static_assert(is_supported_float<T>::value, "Type must be float or double");
    static_assert(std::is_floating_point_v<T>, "Type must be floating point");
public:
    static T find(const T* data, size_t n) {
        if (n == 0)
            return Comparator::identity();
        using vec_t = vector_type<T>;
        constexpr size_t lane_count = sizeof(vec_t) / sizeof(T);
        vec_t vext;
        for (size_t i = 0; i < lane_count; i++)
            reinterpret_cast<T*>(&vext)[i] = Comparator::identity();

        const vec_t* vdata = reinterpret_cast<const vec_t*>(data);
        size_t vlen = n / lane_count;
        for (size_t i = 0; i < vlen; i++) {
            vec_t v = vdata[i];
            if constexpr (Comparator::is_min)
                vext = (v < vext) ? v : vext;
            else
                vext = (v > vext) ? v : vext;
        }
        T result = Comparator::identity();
        const T* lanes = reinterpret_cast<const T*>(&vext);
        for (size_t i = 0; i < lane_count; i++) {
            if (lanes[i] == lanes[i])
                result = Comparator::compare(lanes[i], result);
        }
        const T* remain = data + (vlen * lane_count);
        size_t remain_count = n % lane_count;
        for (size_t i = 0; i < remain_count; i++) {
            if (remain[i] == remain[i])
                result = Comparator::compare(remain[i], result);
        }
        return result;
    }
};

template<typename T>
struct FloatMinComparator {
    static constexpr bool is_min = true;
    static T identity() { return std::numeric_limits<T>::infinity(); }
    static T compare(T a, T b) { return std::min(a, b); }
};

template<typename T>
struct FloatMaxComparator {
    static constexpr bool is_min = false;
    static T identity() { return -std::numeric_limits<T>::infinity(); }
    static T compare(T a, T b) { return std::max(a, b); }
};

template<typename T>
T find_float_min(const T* data, size_t n) {
    return FloatExtremumFinder<T, FloatMinComparator<T>>::find(data, n);
}

template<typename T>
T find_float_max(const T* data, size_t n) {
    return FloatExtremumFinder<T, FloatMaxComparator<T>>::find(data, n);
}

#else

template<typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type
find_float_min(const T *data, size_t n) {
    util::check(size != 0, "Got zero size in find_float_min");
    return *std::min_element(data, data + n);
}

template<typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type
find_float_max(const T *data, size_t n) {
    util::check(size != 0, "Got zero size in find_float_max");
    return *std::max_element(data, data + n);
}

#endif
} // namespace arcticdb