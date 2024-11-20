/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <cstdint>
#include <cstddef>
#include <limits>

#include <arcticdb/codec/fastlanes_common.hpp>

namespace arcticdb {

template <typename T>
void rsum(const uint8_t *__restrict a_in_p, uint8_t* __restrict a_out_p, const uint8_t* __restrict a_base_p) {
    auto out = reinterpret_cast<uint8_t *>(a_out_p);
    const auto in = reinterpret_cast<const uint8_t *>(a_in_p);
    const auto base = reinterpret_cast<const uint8_t *>(a_base_p);

    for (auto lane = 0U; lane < Helper<T>::num_lanes; ++lane) {
        uint8_t register_0;
        uint8_t tmp;
        tmp = base[lane];
        loop<T, Helper<T>::num_bits>([lane, base, in, &tmp, &out, &register_0](auto j) {
            register_0 = in[index(j, lane)];
            tmp = tmp + register_0;
            out[index(j, lane)] = tmp;
        });
    }
}




} // nam