#pragma once

#include <cstdint>
#include <algorithm>
#include <random>
#include <arcticdb/util/preconditions.hpp>

/* The state must be seeded so that it is not all zero */
namespace arcticdb {

typedef std::array<uint64_t, 2> seed;

inline seed &ts() {
    thread_local seed _ts;
    return _ts;
}

inline void init_random(uint64_t n) {
    ts()[0] = ts()[1] = n;
};

// Fast PRNG: see https://en.wikipedia.org/wiki/Xorshift
inline uint64_t xorshiftadd(seed &s) {
    util::check_arg(s[0] != 0 || s[1] != 0, "Need to initialize seed before using random");
    uint64_t x = s[0];
    uint64_t const y = s[1];
    s[0] = y;
    x ^= x << 23; // a
    s[1] = x ^ y ^ (x >> 17) ^ (y >> 26); // b, c
    return s[1] + y;
}

inline uint64_t random_int() {
    return xorshiftadd(ts());
}

inline double random_double() {
    auto i = random_int();
    double output;
    memcpy(static_cast<void *>(&output), static_cast<void *>(&i), sizeof(double));
    return output;
}

inline double random_probability() {
    return double(random_int()) / static_cast<double>(std::numeric_limits< uint64_t >::max());
}

inline char random_char() {
    auto val = static_cast<char>(random_int());
    char c = 'A' + (val & 0x1F);
    return c; // not quite all of them
}

inline size_t random_length() {
    const size_t MaxLength = 0x1F;
    size_t length = 0;
    while (length == 0)
        length = random_int() & MaxLength;

    return length;
}

inline std::string random_string(size_t length) {
    std::string output;
    std::generate_n(std::back_inserter(output), length, random_char);
    return output;
}

inline std::vector<std::string> random_string_vector(size_t n) {
    std::vector<std::string> output;
    std::generate_n(std::back_inserter(output), n, [&] { return random_string(random_length()); });
    return output;
}

struct RandomSelector
{
    using RandomGenerator = std::default_random_engine;

    RandomSelector(RandomGenerator gen = RandomGenerator(std::random_device()()))
    : gen_(gen) {}

    template <typename Iter>
    Iter select(Iter start, Iter end) {
        std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
        std::advance(start, dis(gen_));
        return start;
    }

    template <typename Iter>
    Iter operator()(Iter start, Iter end) {
        return select(start, end);
    }

    template <typename Container>
    auto operator()(const Container& c) -> decltype(*begin(c))& {
        return *select(begin(c), end(c));
    }

private:
    RandomGenerator gen_;
};
}