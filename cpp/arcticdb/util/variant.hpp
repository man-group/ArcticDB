/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <variant>

namespace arcticdb::util {

template<class... Ts>
struct overload : Ts... {
    using Ts::operator()...;
};
template<class... Ts>
overload(Ts...) -> overload<Ts...>;

template<class Variant, class... Ts>
auto variant_match(Variant&& v, Ts... ts)
{
    return std::visit(overload{ts...}, v);
}

template<class Variant, class... Ts>
auto variant_match(const Variant&& v, Ts... ts)
{
    return std::visit(overload{ts...}, v);
}

} // namespace arcticdb::util
