#pragma once

#include <vector>
#include <random>

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/column.hpp>

namespace arcticdb {

template <typename T>
DataType data_type_for_type() {
    if constexpr(std::is_same_v<T, uint8_t>)
        return DataType::UINT8;
    else if constexpr(std::is_same_v<T, int8_t>)
        return DataType::INT8;
    else if constexpr(std::is_same_v<T, uint16_t>)
        return DataType::UINT16;
    else if constexpr(std::is_same_v<T, int16_t>)
        return DataType::INT16;
    else if constexpr(std::is_same_v<T, int32_t>)
        return DataType::INT32;
    else if constexpr(std::is_same_v<T, uint32_t>)
        return DataType::UINT32;
    else if constexpr(std::is_same_v<T, int64_t>)
        return DataType::INT64;
    else if constexpr(std::is_same_v<T, uint64_t>)
        return DataType::UINT64;
    else if constexpr(std::is_same_v<T, float>)
        return DataType::FLOAT32;
    else if constexpr(std::is_same_v<T, double>)
        return DataType::FLOAT64;
    else
        util::raise_rte("Unknown data type");
}

template<typename T>
constexpr TypeDescriptor type_desc_for_type() {
    return make_scalar_type(data_type_for_type<T>());
}

template <typename T>
std::vector<T> random_numbers_with_runs(std::size_t count, uint64_t seed, size_t max_run_length=100) {
    std::vector<T> numbers;
    numbers.reserve(count);
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<T> rand_num_dist(0, std::numeric_limits<T>::max());
    std::uniform_int_distribution<std::size_t> run_length_dist(1, max_run_length);

    while (numbers.size() < count) {
        uint64_t num = rand_num_dist(rng);
        std::size_t run_length = run_length_dist(rng);
        for (std::size_t i = 0; i < run_length && numbers.size() < count; ++i) {
            numbers.push_back(num);
        }
    }

    return numbers;
}

// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
inline uint32_t reduce(uint32_t x, uint32_t N) {
    return ((uint64_t)x * (uint64_t)N) >> 32 ;
}

template <typename T>
std::vector<T> random_numbers_with_leader(size_t length, T leader, double percentage, unsigned int seed = 42) {
    if (percentage < 0.0 || percentage > 1.0 || length <= 0) {
        throw std::invalid_argument("Invalid length or percentage");
    }

    std::mt19937 gen(seed);
    std::uniform_int_distribution<> dis(1, 100);

    std::vector<T> vec(length);
    auto num_leaders = static_cast<size_t>(std::ceil(length * percentage));
    std::generate(vec.begin(), vec.end(), [&]() { return dis(gen); });
    std::fill_n(vec.begin(), num_leaders, leader);
    // std::shuffle is very slow and we would mostly be swapping numbers that are the same.
    for(auto i = num_leaders; i < vec.size(); ++i) {
        auto rnd = dis(gen);
        auto pos = reduce(rnd, num_leaders);
        std::swap(vec[i], vec[pos]);
    }
    return vec;
}

struct ColumnDataWrapper {
    ColumnDataWrapper(ChunkedBuffer&& buffer, TypeDescriptor type, size_t row_count) :
        buffer_(std::move(buffer)),
        data_(&buffer_, nullptr, type, nullptr, nullptr, row_count) {
    }

    ChunkedBuffer buffer_;
    ColumnData data_;
};

template <typename T>
ColumnDataWrapper from_vector(const std::vector<T>& data, TypeDescriptor type) {
    ChunkedBuffer buffer;
    buffer.add_external_block(reinterpret_cast<const uint8_t*>(data.data()), data.size() * sizeof(T), 0);
    return {std::move(buffer), type, data.size()};
}


template <typename T>
Column column_from_vector(const std::vector<T>& data, TypeDescriptor type) {
    Column column{type, data.size(), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED};
    const auto bytes =  data.size() * sizeof(T);
    memcpy(column.ptr_cast<T>(0, bytes), data.data(), bytes);
    return column;
}


template <typename T>
std::vector<T> values_within_bitwidth(T start, unsigned int bitwidth, std::size_t count) {
    T max_increment = static_cast<T>(1) << bitwidth;
    T max_value = start + max_increment - 1;

    util::check(max_value >= start, "Overflow detected: start value plus bitwidth exceeds type limits.");
    util::check(max_value <= std::numeric_limits<T>::max(),"Bitwidth too large for given type");

    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<T> dist(start, max_value);

    std::vector<T> output;
    output.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        output.push_back(dist(engine));
    }
    return output;
}

template <typename T>
std::vector<T> delta_values(T start, unsigned int bitwidth, std::size_t count) {
    T max_increment = static_cast<T>(1) << bitwidth;

    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<T> dist(0, max_increment);

    std::vector<T> output;
    output.reserve(count);
    T value = start;
    for (std::size_t i = 0; i < count; ++i) {
        output.push_back(value);
        value += dist(engine);
    }
    return output;
}

template <typename T>
std::vector<T> values_with_duplicates(
    size_t count,
    double percentage,
    const T &duplicateValue) {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dist(0, 100);

    std::vector<T> vec(count);
    for (size_t i = 0; i < count; ++i) {
        vec[i] = dist(mt);
    }

    size_t repeats = static_cast<size_t>(std::round(count * percentage));
    if (repeats > count) {
        repeats = count;
    }

    std::vector<size_t> indices(count);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), mt);

    for (size_t i = 0; i < repeats; ++i) {
        vec[indices[i]] = duplicateValue;
    }

    return vec;
}

inline std::vector<double> random_doubles(size_t num_rows = 1024, double min = 0.0, double max = 1000.0) {
    std::default_random_engine generator(43);
    auto dist = std::uniform_real_distribution<double>(min, max);
    std::vector<double> data;
    data.reserve(num_rows);
    for(auto i = 0UL; i < num_rows; ++i) {
        data.push_back(dist(generator));
    }
    return data;
}

inline std::vector<double> compressible_doubles(
        size_t num_rows = 1024,
        double start = 0.0,
        double step = 1.0,
        double jitter_factor = 0.05) {
    std::default_random_engine generator(43);
    std::uniform_real_distribution<double> jitter_dist(-step * jitter_factor, step * jitter_factor);
    std::vector<double> data;
    data.reserve(num_rows);
    double current = start;
    for (size_t i = 0; i < num_rows; ++i) {
        data.push_back(current);
        current += step + jitter_dist(generator);
    }
    return data;
}


inline std::vector<double> random_decimal_floats(int64_t lower, int64_t upper, size_t decimal_places, size_t count) {
    std::vector<double> result;
    result.reserve(count);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(lower, upper);

    for (auto i = 0UL; i < count; ++i) {
        auto value = dis(gen);
        result.push_back(static_cast<double>(value) / pow(10, decimal_places));
    }

    return result;
}
} //namespace arcticdb