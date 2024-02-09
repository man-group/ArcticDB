/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <cstdint>

namespace arcticdb {

template <typename T>
struct RunLengthEncoding {
#pragma pack(push, 1)
    struct State {
        T current_;
        uint16_t count_;

        void check(uint8_t *&ptr, T val) {
            if (val != current_ || count_ == std::numeric_limits<uint16_t>::max()) {
                flush(ptr);
                reset(val);
            } else {
                ++count_;
            }
        }

        void flush(uint8_t *&ptr) {
            ARCTICDB_DEBUG(log::codec(), "Flushing {}: {}", current_, count_);
            *reinterpret_cast<T *>(ptr) = current_;
            ptr += sizeof(T);
            *reinterpret_cast<uint16_t *>(ptr) = count_;
            ptr += sizeof(uint16_t);
        }

        void reset(T t) {
            count_ = 0;
            current_ = t;
        }
    };
#pragma pack(pop)

    std::optional<size_t> max_required_bytes(const T *data_in, size_t num_rows) {
        if (num_rows == 0)
            return 0;

        const auto *pos = data_in;
        const auto *end = pos + num_rows;
        T current = *pos;
        ++pos;
        size_t count = 0;
        do {
            if (*pos == current)
                ++count;

            current = *pos;
            ++pos;
        } while (pos != end);

        return count > 0 ? std::make_optional((num_rows - count) * sizeof(T)) : std::nullopt;
    }

    size_t encode(const T *data_in, size_t num_rows, uint8_t *data_out) {
        if (num_rows == 0)
            return 0;

        const auto *pos = data_in;
        const auto *end = pos + num_rows;
        auto *target = data_out;
        State state{*pos, 0};
        ++pos;
        do {
            state.check(target, *pos);
            ++pos;
        } while (pos != end);
        state.flush(target);

        return target - data_out;
    }

    size_t decode(const uint8_t *data_in, size_t bytes, T *data_out) {
        util::check(bytes > sizeof(State), "Not enough bytes in run-length encoding");
        if (bytes == 0)
            return 0;

        const auto *pos = data_in;
        const auto *end = pos + bytes;
        auto *target = data_out;
        do {
            const auto *state = reinterpret_cast<const State*>(pos);
            const auto val = state->current_;
            auto count = static_cast<int32_t>(state->count_);
            do {
                *target++ = val;
                --count;
            } while (count >= 0);

            pos += sizeof(State);
        } while (pos < end);

        return target - data_out;
    }
};
}