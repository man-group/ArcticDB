/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once
#include <arcticdb/util/preconditions.hpp>
// from https://en.wikipedia.org/wiki/MurmurHash

namespace arcticdb {
static inline uint32_t murmur_32_scramble(uint32_t k) {
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    return k;
}

// This is here so that we have deterministic hashing for keys.
// If you change anything including the seed, you will break things.
inline uint32_t murmur3_32(const std::string& str) {
    constexpr uint32_t seed = 0xbeef;
    uint32_t hash = seed;
    uint32_t k;
    auto key = reinterpret_cast<const uint8_t*>(str.data());
    const size_t len = str.size();
    for (size_t i = len >> 2; i; i--) {
        memcpy(&k, key, sizeof(uint32_t));
        key += sizeof(uint32_t);
        hash ^= murmur_32_scramble(k);
        hash = (hash << 13) | (hash >> 19);
        hash = hash * 5 + 0xe6546b64;
    }
    k = 0;
    for (size_t i = len & 3; i; i--) {
        k <<= 8;
        k |= key[i - 1];
    }

    hash ^= murmur_32_scramble(k);
    hash ^= len;
    hash ^= hash >> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >> 16;
    return hash;
}

inline size_t bucketize(const std::string& name, std::optional<size_t> num_buckets) {
    auto hash = murmur3_32(name);
    if (!num_buckets.has_value())
        return hash;
    return hash % num_buckets.value();
}
}
