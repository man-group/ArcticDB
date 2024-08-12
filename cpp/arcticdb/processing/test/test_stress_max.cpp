#include <gtest/gtest.h>

#include <arcticdb/util/timer.hpp>

#include <iostream>
#include <random>
#include <vector>
#include <type_traits>

template <typename T>
std::vector<T> get_random_ints(size_t length, T min_val=std::numeric_limits<T>::min(), T max_val=std::numeric_limits<T>::max()) {
    static_assert(std::is_integral<T>::value, "Template parameter must be an integral type.");

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<T> dist(min_val, max_val);

    std::vector<T> random_integers;
    random_integers.reserve(length);

    for (size_t i = 0; i < length; ++i) {
        random_integers.push_back(dist(gen));
    }

    return random_integers;
}

template <typename T>
T scalar_sum(T* pos, size_t num_rows) {
    auto sum = 0;
    for(auto i = 0U; i < num_rows; ++i) {
        sum += pos[i];
    }
    return sum;
}

template <typename T>
T unrolled_sum(T* pos, size_t num_rows) {
    T buckets[8];
    std::fill(std::begin(buckets), std::end(buckets), std::numeric_limits<T>::min());
    for(auto i = 0U; i < num_rows; i += 8) {
        buckets[0] = std::max(pos[i], buckets[0]);
        buckets[1] = std::max(pos[i], buckets[1]);
        buckets[2] = std::max(pos[i], buckets[2]);
        buckets[3] = std::max(pos[i], buckets[3]);
        buckets[4] = std::max(pos[i], buckets[4]);
        buckets[5] = std::max(pos[i], buckets[5]);
        buckets[6] = std::max(pos[i], buckets[6]);
        buckets[7] = std::max(pos[i], buckets[7]);
    }
    return scalar_sum(buckets, 8);
}

template <typename T>
T scalar_max(T* pos, size_t num_rows) {
    auto max = std::numeric_limits<T>::min();
    for(auto i = 0U; i < num_rows; ++i) {
        max = std::max(pos[i], max);
    }
    return max;
}

template <typename T>
T unrolled_max(T* pos, size_t num_rows) {
    T buckets[8];
    std::fill(std::begin(buckets), std::end(buckets), std::numeric_limits<T>::min());
    for(auto i = 0U; i < num_rows; i += 8) {
        buckets[0] = std::max(pos[i], buckets[0]);
        buckets[1] = std::max(pos[i], buckets[1]);
        buckets[2] = std::max(pos[i], buckets[2]);
        buckets[3] = std::max(pos[i], buckets[3]);
        buckets[4] = std::max(pos[i], buckets[4]);
        buckets[5] = std::max(pos[i], buckets[5]);
        buckets[6] = std::max(pos[i], buckets[6]);
        buckets[7] = std::max(pos[i], buckets[7]);
    }
    return scalar_max(buckets, 8);
}

template <typename T>
T unordered_max(T* pos, size_t num_rows) {
    T buckets[8];
    std::fill(std::begin(buckets), std::end(buckets), std::numeric_limits<T>::min());
    for(auto i = 0U; i < num_rows; i += 8) {
        buckets[0] = std::max(pos[i], buckets[0]);
        buckets[4] = std::max(pos[i], buckets[4]);
        buckets[2] = std::max(pos[i], buckets[2]);
        buckets[6] = std::max(pos[i], buckets[6]);
        buckets[1] = std::max(pos[i], buckets[1]);
        buckets[5] = std::max(pos[i], buckets[5]);
        buckets[3] = std::max(pos[i], buckets[3]);
        buckets[7] = std::max(pos[i], buckets[7]);
    }
    return scalar_max(buckets, 8);
}

template<typename T>
inline T my_max(T t, T u) {
    return t > u ? t : u;
}

template<typename Func>
void unroll_function_8(const uint8_t *__restrict a_left_p,  const uint8_t* __restrict a_right_p, uint8_t* __restrict a_out_p, size_t num_rows, Func&& func)
{

    [[maybe_unused]] uint8_t lval;
    [[maybe_unused]] uint8_t rval;
    const auto outer = num_rows / 1024;
    for(auto j = 0UL; j < outer; ++j) {
        [[maybe_unused]] auto out = reinterpret_cast<uint8_t *>(a_out_p) + (j * 1024);
        [[maybe_unused]] const auto left = reinterpret_cast<const uint8_t *>(a_left_p) + (j * 1024);
        const auto right =  reinterpret_cast<const uint8_t *>(a_right_p)+ (j * 1024);
        for (int i = 0; i < 128; ++i) {
            lval = *(left + i + 0);
            rval = *(right + i);
            lval = func(lval, rval);
            out[i + 0] = lval;
            lval = *(left + i + 128);
            rval = *(right + i + 128);
            lval = func(lval, rval);
            out[i + 128] = lval;
            lval = *(left + i + 256);
            rval = *(right + i + 256);
            lval = func(lval, rval);
            out[i + 256] = lval;
            lval = *(left + i + 384);
            rval = *(right + i + 384);
            lval = func(lval, rval);
            out[i + 384] = lval;
            lval = *(left + i + 512);
            rval = *(right + i + 512);
            lval = func(lval, rval);
            out[i + 512] = lval;
            lval = *(left + i + 640);
            rval = *(right + i + 640);
            lval = func(lval, rval);
            out[i + 640] = lval;
            lval = *(left + i + 768);
            rval = *(right + i + 768);
            lval = func(lval, rval);
            out[i + 768] = lval;
            lval = *(left + i + 896);
            rval = *(right + i + 896);
            lval = func(lval, rval);
            out[i + 896] = lval;
        }
    }
}

template <typename Func>
void loop_unroll_64(const uint64_t *__restrict a_left_p,  const uint64_t* __restrict a_right_p, uint64_t* __restrict a_out_p, size_t num_rows, Func&& func) {
    [[maybe_unused]] uint64_t l;
    [[maybe_unused]] uint64_t r;
    const auto outer = num_rows / 1024;
    for (auto j = 0UL; j < outer; ++j) {
        auto out = reinterpret_cast<uint64_t *>(a_out_p) + (j * 1024);
        const auto left = reinterpret_cast<const uint64_t*>(a_left_p) + (j * 1024);
        const auto right = reinterpret_cast<const uint64_t*>(a_right_p) + (j * 1024);
        for (int i = 0; i < 16; ++i) {
            l = *(left + 16 * 0 + i);
            r = *(right + 16 * 0 + i);
            out[i + 16 * 0] = func(l, r);
            l = *(left + 16 * 1 + i);
            r = *(right + 16 * 1 + i);
            out[i + 16 * 1] = func(l, r);
            l = *(left + 16 * 2 + i);
            r = *(right + 16 * 2 + i);
            out[i + 16 * 2] = func(l, r);
            l = *(left + 16 * 3 + i);
            r = *(right + 16 * 3 + i);
            out[i + 16 * 3] = func(l, r);
            l = *(left + 16 * 4 + i);
            r = *(right + 16 * 4 + i);
            out[i + 16 * 4] = func(l, r);
            l = *(left + 16 * 5 + i);
            r = *(right + 16 * 5 + i);
            out[i + 16 * 5] = func(l, r);
            l = *(left + 16 * 6 + i);
            r = *(right + 16 * 6 + i);
            out[i + 16 * 6] = func(l, r);
            l = *(left + 16 * 7 + i);
            r = *(right + 16 * 7 + i);
            out[i + 16 * 7] = func(l, r);
            l = *(left + 16 * 8 + i);
            r = *(right + 16 * 8 + i);
            out[i + 16 * 8] = func(l, r);
            l = *(left + 16 * 9 + i);
            r = *(right + 16 * 9 + i);
            out[i + 16 * 9] = func(l, r);
            l = *(left + 16 * 10 + i);
            r = *(right + 16 * 10 + i);
            out[i + 16 * 10] = func(l, r);
            l = *(left + 16 * 11 + i);
            r = *(right + 16 * 11 + i);
            out[i + 16 * 11] = func(l, r);
            l = *(left + 16 * 12 + i);
            r = *(right + 16 * 12 + i);
            out[i + 16 * 12] = func(l, r);
            l = *(left + 16 * 13 + i);
            r = *(right + 16 * 13 + i);
            out[i + 16 * 13] = func(l, r);
            l = *(left + 16 * 14 + i);
            r = *(right + 16 * 14 + i);
            out[i + 16 * 14] = func(l, r);
            l = *(left + 16 * 15 + i);
            r = *(right + 16 * 15 + i);
            out[i + 16 * 15] = func(l, r);
            l = *(left + 16 * 16 + i);
            r = *(right + 16 * 16 + i);
            out[i + 16 * 16] = func(l, r);
            l = *(left + 16 * 17 + i);
            r = *(right + 16 * 17 + i);
            out[i + 16 * 17] = func(l, r);
            l = *(left + 16 * 18 + i);
            r = *(right + 16 * 18 + i);
            out[i + 16 * 18] = func(l, r);
            l = *(left + 16 * 19 + i);
            r = *(right + 16 * 19 + i);
            out[i + 16 * 19] = func(l, r);
            l = *(left + 16 * 20 + i);
            r = *(right + 16 * 20 + i);
            out[i + 16 * 20] = func(l, r);
            l = *(left + 16 * 21 + i);
            r = *(right + 16 * 21 + i);
            out[i + 16 * 21] = func(l, r);
            l = *(left + 16 * 22 + i);
            r = *(right + 16 * 22 + i);
            out[i + 16 * 22] = func(l, r);
            l = *(left + 16 * 23 + i);
            r = *(right + 16 * 23 + i);
            out[i + 16 * 23] = func(l, r);
            l = *(left + 16 * 24 + i);
            r = *(right + 16 * 24 + i);
            out[i + 16 * 24] = func(l, r);
            l = *(left + 16 * 25 + i);
            r = *(right + 16 * 25 + i);
            out[i + 16 * 25] = func(l, r);
            l = *(left + 16 * 26 + i);
            r = *(right + 16 * 26 + i);
            out[i + 16 * 26] = func(l, r);
            l = *(left + 16 * 27 + i);
            r = *(right + 16 * 27 + i);
            out[i + 16 * 27] = func(l, r);
            l = *(left + 16 * 28 + i);
            r = *(right + 16 * 28 + i);
            out[i + 16 * 28] = func(l, r);
            l = *(left + 16 * 29 + i);
            r = *(right + 16 * 29 + i);
            out[i + 16 * 29] = func(l, r);
            l = *(left + 16 * 30 + i);
            r = *(right + 16 * 30 + i);
            out[i + 16 * 30] = func(l, r);
            l = *(left + 16 * 31 + i);
            r = *(right + 16 * 31 + i);
            out[i + 16 * 31] = func(l, r);
            l = *(left + 16 * 32 + i);
            r = *(right + 16 * 32 + i);
            out[i + 16 * 32] = func(l, r);
            l = *(left + 16 * 33 + i);
            r = *(right + 16 * 33 + i);
            out[i + 16 * 33] = func(l, r);
            l = *(left + 16 * 34 + i);
            r = *(right + 16 * 34 + i);
            out[i + 16 * 34] = func(l, r);
            l = *(left + 16 * 35 + i);
            r = *(right + 16 * 35 + i);
            out[i + 16 * 35] = func(l, r);
            l = *(left + 16 * 36 + i);
            r = *(right + 16 * 36 + i);
            out[i + 16 * 36] = func(l, r);
            l = *(left + 16 * 37 + i);
            r = *(right + 16 * 37 + i);
            out[i + 16 * 37] = func(l, r);
            l = *(left + 16 * 38 + i);
            r = *(right + 16 * 38 + i);
            out[i + 16 * 38] = func(l, r);
            l = *(left + 16 * 39 + i);
            r = *(right + 16 * 39 + i);
            out[i + 16 * 39] = func(l, r);
            l = *(left + 16 * 40 + i);
            r = *(right + 16 * 40 + i);
            out[i + 16 * 40] = func(l, r);
            l = *(left + 16 * 41 + i);
            r = *(right + 16 * 41 + i);
            out[i + 16 * 41] = func(l, r);
            l = *(left + 16 * 42 + i);
            r = *(right + 16 * 42 + i);
            out[i + 16 * 42] = func(l, r);
            l = *(left + 16 * 43 + i);
            r = *(right + 16 * 43 + i);
            out[i + 16 * 43] = func(l, r);
            l = *(left + 16 * 44 + i);
            r = *(right + 16 * 44 + i);
            out[i + 16 * 44] = func(l, r);
            l = *(left + 16 * 45 + i);
            r = *(right + 16 * 45 + i);
            out[i + 16 * 45] = func(l, r);
            l = *(left + 16 * 46 + i);
            r = *(right + 16 * 46 + i);
            out[i + 16 * 46] = func(l, r);
            l = *(left + 16 * 47 + i);
            r = *(right + 16 * 47 + i);
            out[i + 16 * 47] = func(l, r);
            l = *(left + 16 * 48 + i);
            r = *(right + 16 * 48 + i);
            out[i + 16 * 48] = func(l, r);
            l = *(left + 16 * 49 + i);
            r = *(right + 16 * 49 + i);
            out[i + 16 * 49] = func(l, r);
            l = *(left + 16 * 50 + i);
            r = *(right + 16 * 50 + i);
            out[i + 16 * 50] = func(l, r);
            l = *(left + 16 * 51 + i);
            r = *(right + 16 * 51 + i);
            out[i + 16 * 51] = func(l, r);
            l = *(left + 16 * 52 + i);
            r = *(right + 16 * 52 + i);
            out[i + 16 * 52] = func(l, r);
            l = *(left + 16 * 53 + i);
            r = *(right + 16 * 53 + i);
            out[i + 16 * 53] = func(l, r);
            l = *(left + 16 * 54 + i);
            r = *(right + 16 * 54 + i);
            out[i + 16 * 54] = func(l, r);
            l = *(left + 16 * 55 + i);
            r = *(right + 16 * 55 + i);
            out[i + 16 * 55] = func(l, r);
            l = *(left + 16 * 56 + i);
            r = *(right + 16 * 56 + i);
            out[i + 16 * 56] = func(l, r);
            l = *(left + 16 * 57 + i);
            r = *(right + 16 * 57 + i);
            out[i + 16 * 57] = func(l, r);
            l = *(left + 16 * 58 + i);
            r = *(right + 16 * 58 + i);
            out[i + 16 * 58] = func(l, r);
            l = *(left + 16 * 59 + i);
            r = *(right + 16 * 59 + i);
            out[i + 16 * 59] = func(l, r);
            l = *(left + 16 * 60 + i);
            r = *(right + 16 * 60 + i);
            out[i + 16 * 60] = func(l, r);
            l = *(left + 16 * 61 + i);
            r = *(right + 16 * 61 + i);
            out[i + 16 * 61] = func(l, r);
            l = *(left + 16 * 62 + i);
            r = *(right + 16 * 62 + i);
            out[i + 16 * 62] = func(l, r);
            l = *(left + 16 * 63 + i);
            r = *(right + 16 * 63 + i);
            out[i + 16 * 63] = func(l, r);
        }
    }
}


TEST(AddColumns, Simple) {
    using namespace arcticdb;
    const auto num_rows = 100000U * 1024;
    auto left = get_random_ints<uint8_t>(num_rows);
    auto right = get_random_ints<uint8_t>(num_rows);
    std::vector<uint8_t> output(num_rows);
    auto left_p = left.data();
    auto right_p = right.data();
    auto out_p = output.data();

    interval_timer timer;
    timer.start_timer("ScalarAdd");
    for(auto i = 0U; i < num_rows; ++i)
        *out_p++ = *left_p++ + *right_p++;
    timer.stop_timer("ScalarAdd");
    log::codec().info("{}", timer.display_timer("ScalarAdd"));
}


TEST(AddColumns64, Simple64) {
    using namespace arcticdb;
    const auto num_rows = 100000U * 1024;
    auto left = get_random_ints<uint64_t>(num_rows);
    auto right = get_random_ints<uint64_t>(num_rows);
    std::vector<uint64_t> output(num_rows);
    auto left_p = left.data();
    auto right_p = right.data();
    auto out_p = output.data();

    interval_timer timer;
    timer.start_timer("ScalarAdd64");
    for(auto i = 0U; i < num_rows; ++i)
        *out_p++ = *left_p++ + *right_p++;
    timer.stop_timer("ScalarAdd64");
    log::codec().info("{}", timer.display_timer("ScalarAdd64"));
}


TEST(AddColumns, Vectorized) {
    using namespace arcticdb;
    const auto num_rows = 100000U * 1024;
    auto left = get_random_ints<uint8_t>(num_rows);
    auto right = get_random_ints<uint8_t>(num_rows);
    std::vector<uint8_t> output(num_rows);
    auto left_p = left.data();
    auto right_p = right.data();
    auto out_p = output.data();

    interval_timer timer;
    timer.start_timer("FastLanesAdd");
    unroll_function_8(left_p, right_p, out_p, num_rows, [] (auto l, auto r) { return l + r; });
    timer.stop_timer("FastLanesAdd");
    log::codec().info("{}", timer.display_timer("FastLanesAdd"));
}

TEST(AddColumns64, Vectorized64) {
    using namespace arcticdb;
    const auto num_rows = 100000U * 1024;
    auto left = get_random_ints<uint64_t>(num_rows);
    auto right = get_random_ints<uint64_t>(num_rows);
    std::vector<uint64_t> output(num_rows);
    auto left_p = left.data();
    auto right_p = right.data();
    auto out_p = output.data();

    interval_timer timer;
    timer.start_timer("FastLanesAdd64");
    loop_unroll_64(left_p, right_p, out_p, num_rows, [] (auto l, auto r) { return l + r; });
    timer.stop_timer("FastLanesAdd64");
    log::codec().info("{}", timer.display_timer("FastLanesAdd64"));
}

TEST(AddColumns, Compare) {
    const auto num_rows = 100000U * 1024;
    auto left = get_random_ints<uint8_t>(num_rows);
    auto right = get_random_ints<uint8_t>(num_rows);
    std::vector<uint8_t> output1(num_rows);
    std::vector<uint8_t> output2(num_rows);
    auto left_p = left.data();
    auto right_p = right.data();
    auto out_p1 = output1.data();
    auto out_p2 = output2.data();

    for(auto i = 0U; i < num_rows; ++i)
        *out_p1++ = *left_p++ + *right_p++;

    left_p = left.data();
    right_p = right.data();
    unroll_function_8(left_p, right_p, out_p2, num_rows, [] (auto l, auto r) { return l + r; });
    ASSERT_EQ(output1, output2);
}

TEST(AddColumns64, Compare) {
    const auto num_rows = 100000U * 1024;
    auto left = get_random_ints<uint64_t>(num_rows, 0, 40);
    auto right = get_random_ints<uint64_t>(num_rows, 0, 40);
    std::vector<uint64_t> output1(num_rows);
    std::vector<uint64_t> output2(num_rows);
    auto left_p = left.data();
    auto right_p = right.data();
    auto out_p1 = output1.data();
    auto out_p2 = output2.data();

    for(auto i = 0U; i < num_rows; ++i)
        *out_p1++ = *left_p++ + *right_p++;

    left_p = left.data();
    right_p = right.data();
    loop_unroll_64(left_p, right_p, out_p2, num_rows, [] (auto l, auto r) { return l + r; });
    ASSERT_EQ(output1, output2);
}

template <typename T>
T fastlanes_max(const T* __restrict data, size_t num_rows) {
    T buckets[64 * 8];
    std::fill(std::begin(buckets), std::end(buckets), std::numeric_limits<T>::min());
    T src;
    auto outer = num_rows / 1024;
    for (auto j = 0U; j < outer; ++j) {
        const auto* in = data + (j * 1024);
        for (auto i = 0U; i < 16; ++i) {
            src = *(in + 16 * 0 + i);
            buckets[0] = std::max(src, buckets[0 * 8]);
            src = *(in + 16 * 1 + i);
            buckets[1] = std::max(src, buckets[1 * 8]);
            src = *(in + 16 * 2 + i);
            buckets[2] = std::max(src, buckets[2 * 8]);
            src = *(in + 16 * 3 + i);
            buckets[3] = std::max(src, buckets[3 * 8]);
            src = *(in + 16 * 4 + i);
            buckets[4] = std::max(src, buckets[4 * 8]);
            src = *(in + 16 * 5 + i);
            buckets[5] = std::max(src, buckets[5 * 8]);
            src = *(in + 16 * 6 + i);
            buckets[6] = std::max(src, buckets[6 * 8]);
            src = *(in + 16 * 7 + i);
            buckets[7] = std::max(src, buckets[7 * 8]);
            src = *(in + 16 * 8 + i);
            buckets[8] = std::max(src, buckets[8 * 8]);
            src = *(in + 16 * 9 + i);
            buckets[9] = std::max(src, buckets[9 * 8]);
            src = *(in + 16 * 10 + i);
            buckets[10] = std::max(src, buckets[10 * 8]);
            src = *(in + 16 * 11 + i);
            buckets[11] = std::max(src, buckets[11 * 8]);
            src = *(in + 16 * 12 + i);
            buckets[12] = std::max(src, buckets[12 * 8]);
            src = *(in + 16 * 13 + i);
            buckets[13] = std::max(src, buckets[13 * 8]);
            src = *(in + 16 * 14 + i);
            buckets[14] = std::max(src, buckets[14 * 8]);
            src = *(in + 16 * 15 + i);
            buckets[15] = std::max(src, buckets[15 * 8]);
            src = *(in + 16 * 16 + i);
            buckets[16] = std::max(src, buckets[16 * 8]);
            src = *(in + 16 * 17 + i);
            buckets[17] = std::max(src, buckets[17 * 8]);
            src = *(in + 16 * 18 + i);
            buckets[18] = std::max(src, buckets[18 * 8]);
            src = *(in + 16 * 19 + i);
            buckets[19] = std::max(src, buckets[19 * 8]);
            src = *(in + 16 * 20 + i);
            buckets[20] = std::max(src, buckets[20 * 8]);
            src = *(in + 16 * 21 + i);
            buckets[21] = std::max(src, buckets[21 * 8]);
            src = *(in + 16 * 22 + i);
            buckets[22] = std::max(src, buckets[22 * 8]);
            src = *(in + 16 * 23 + i);
            buckets[23] = std::max(src, buckets[23 * 8]);
            src = *(in + 16 * 24 + i);
            buckets[24] = std::max(src, buckets[24 * 8]);
            src = *(in + 16 * 25 + i);
            buckets[25] = std::max(src, buckets[25 * 8]);
            src = *(in + 16 * 26 + i);
            buckets[26] = std::max(src, buckets[26 * 8]);
            src = *(in + 16 * 27 + i);
            buckets[27] = std::max(src, buckets[27 * 8]);
            src = *(in + 16 * 28 + i);
            buckets[28] = std::max(src, buckets[28 * 8]);
            src = *(in + 16 * 29 + i);
            buckets[29] = std::max(src, buckets[29 * 8]);
            src = *(in + 16 * 30 + i);
            buckets[30] = std::max(src, buckets[30 * 8]);
            src = *(in + 16 * 31 + i);
            buckets[31] = std::max(src, buckets[31 * 8]);
            src = *(in + 16 * 32 + i);
            buckets[32] = std::max(src, buckets[32 * 8]);
            src = *(in + 16 * 33 + i);
            buckets[33] = std::max(src, buckets[33 * 8]);
            src = *(in + 16 * 34 + i);
            buckets[34] = std::max(src, buckets[34 * 8]);
            src = *(in + 16 * 35 + i);
            buckets[35] = std::max(src, buckets[35 * 8]);
            src = *(in + 16 * 36 + i);
            buckets[36] = std::max(src, buckets[36 * 8]);
            src = *(in + 16 * 37 + i);
            buckets[37] = std::max(src, buckets[37 * 8]);
            src = *(in + 16 * 38 + i);
            buckets[38] = std::max(src, buckets[38 * 8]);
            src = *(in + 16 * 39 + i);
            buckets[39] = std::max(src, buckets[39 * 8]);
            src = *(in + 16 * 40 + i);
            buckets[40] = std::max(src, buckets[40 * 8]);
            src = *(in + 16 * 41 + i);
            buckets[41] = std::max(src, buckets[41 * 8]);
            src = *(in + 16 * 42 + i);
            buckets[42] = std::max(src, buckets[42 * 8]);
            src = *(in + 16 * 43 + i);
            buckets[43] = std::max(src, buckets[43 * 8]);
            src = *(in + 16 * 44 + i);
            buckets[44] = std::max(src, buckets[44 * 8]);
            src = *(in + 16 * 45 + i);
            buckets[45] = std::max(src, buckets[45 * 8]);
            src = *(in + 16 * 46 + i);
            buckets[46] = std::max(src, buckets[46 * 8]);
            src = *(in + 16 * 47 + i);
            buckets[47] = std::max(src, buckets[47 * 8]);
            src = *(in + 16 * 48 + i);
            buckets[48] = std::max(src, buckets[48 * 8]);
            src = *(in + 16 * 49 + i);
            buckets[49] = std::max(src, buckets[49 * 8]);
            src = *(in + 16 * 50 + i);
            buckets[50] = std::max(src, buckets[50 * 8]);
            src = *(in + 16 * 51 + i);
            buckets[51] = std::max(src, buckets[51 * 8]);
            src = *(in + 16 * 52 + i);
            buckets[52] = std::max(src, buckets[52 * 8]);
            src = *(in + 16 * 53 + i);
            buckets[53] = std::max(src, buckets[53 * 8]);
            src = *(in + 16 * 54 + i);
            buckets[54] = std::max(src, buckets[54 * 8]);
            src = *(in + 16 * 55 + i);
            buckets[55] = std::max(src, buckets[55 * 8]);
            src = *(in + 16 * 56 + i);
            buckets[56] = std::max(src, buckets[56 * 8]);
            src = *(in + 16 * 57 + i);
            buckets[57] = std::max(src, buckets[57 * 8]);
            src = *(in + 16 * 58 + i);
            buckets[58] = std::max(src, buckets[58 * 8]);
            src = *(in + 16 * 59 + i);
            buckets[59] = std::max(src, buckets[59 * 8]);
            src = *(in + 16 * 60 + i);
            buckets[60] = std::max(src, buckets[60 * 8]);
            src = *(in + 16 * 61 + i);
            buckets[61] = std::max(src, buckets[61 * 8]);
            src = *(in + 16 * 62 + i);
            buckets[62] = std::max(src, buckets[62 * 8]);
            src = *(in + 16 * 63 + i);
            buckets[63] = std::max(src, buckets[63 * 8]);
        }
    }
    return scalar_max(buckets, 64 * 8);
}

TEST(GetSum, Scalar) {
    using namespace arcticdb;
    auto num_ints = 100000 * 1024;
    auto ints = get_random_ints<uint64_t>(num_ints, 0, 16543);
    interval_timer timer;
    timer.start_timer("ScalarMax");
    auto max = scalar_sum(ints.data(), ints.size());
    timer.stop_timer("ScalarMax");
    log::codec().info("Max {}: {}", max, timer.display_timer("ScalarMax"));
}

TEST(GetSum, Unrolled) {
    using namespace arcticdb;
    auto num_ints = 100000 * 1024;
    auto ints = get_random_ints<uint64_t>(num_ints, 0, 16543);
    interval_timer timer;
    timer.start_timer("UnrolledMax");
    auto max = unrolled_sum(ints.data(), ints.size());
    timer.stop_timer("UnrolledMax");
    log::codec().info("Max {}: {}", max, timer.display_timer("UnrolledMax"));
}


TEST(GetMax, Scalar) {
    using namespace arcticdb;
    auto num_ints = 100000 * 1024;
    auto ints = get_random_ints<uint64_t>(num_ints, 0, 16543);
    interval_timer timer;
    timer.start_timer("ScalarMax");
    auto max = scalar_max(ints.data(), ints.size());
    timer.stop_timer("ScalarMax");
    log::codec().info("Max {}: {}", max, timer.display_timer("ScalarMax"));
}

TEST(GetMax, Unrolled) {
    using namespace arcticdb;
    auto num_ints = 100000 * 1024;
    auto ints = get_random_ints<uint64_t>(num_ints, 0, 16543);
    interval_timer timer;
    timer.start_timer("UnrolledMax");
    auto max = unrolled_max(ints.data(), ints.size());
    timer.stop_timer("UnrolledMax");
    log::codec().info("Max {}: {}", max, timer.display_timer("UnrolledMax"));
}

TEST(GetMax, Unordered) {
    using namespace arcticdb;
    auto num_ints = 100000 * 1024;
    auto ints = get_random_ints<uint64_t>(num_ints, 0, 16543);
    interval_timer timer;
    timer.start_timer("UnorderedMax");
    auto max = unordered_max(ints.data(), ints.size());
    timer.stop_timer("UnorderedMax");
    log::codec().info("Max {}: {}", max, timer.display_timer("UnorderedMax"));
}

TEST(GetMax, Fastlanes) {
    using namespace arcticdb;
    auto num_ints = 100000 * 1024;
    auto ints = get_random_ints<uint64_t>(num_ints, 0, 16543);
    interval_timer timer;
    timer.start_timer("FastlanesMax");
    auto max = fastlanes_max(ints.data(), ints.size());
    timer.stop_timer("FastlanesMax");
    log::codec().info("Max {}: {}", max, timer.display_timer("FastlanesMax"));
}