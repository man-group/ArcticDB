#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <cstdint>

namespace arcticdb {
template<typename T>
struct ConstantEncoding {

#pragma pack(push, 1)
    struct Data {
        uint64_t size_;
        T value_;
    };
#pragma pack(pop)

    std::optional<size_t> max_required_bytes(const T* data_in, size_t num_rows) {
        if (num_rows == 0)
            return 0;

        const auto *pos = data_in;
        const auto *end = pos + num_rows;
        T first = *pos;
        ++pos;
        do {
            if (*pos != first)
                return std::nullopt;

            ++pos;
        } while (pos != end);

        return sizeof(Data);
    }

    size_t encode(const T *data_in, size_t num_rows, uint8_t *data_out) {
        if (num_rows == 0)
            return 0;

        auto *state = reinterpret_cast<Data*>(data_out);
        state->size_ = num_rows;
        state->value_ = *data_in;
        return sizeof(Data);
    }

    size_t decode(const uint8_t *data_in, size_t bytes, T *data_out) {
        util::check(bytes == sizeof(Data), "Not enough bytes in constant encoding");

        const auto *state = reinterpret_cast<const Data*>(data_in);
        auto *target = data_out;
        auto *target_end = target + state->size_;
        std::fill(target, target_end, state->value_);
        return state->size_;
    }
};
}