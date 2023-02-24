/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <variant>

namespace arcticdb::util {

template<class... Ts> struct overload : Ts... { using Ts::operator()...; };
template<class... Ts> overload(Ts...) -> overload<Ts...>;

template<class Variant, class... Ts>
auto variant_match(Variant && v, Ts... ts){
    return std::visit(overload{ts...}, v);
}

template<class Variant, class... Ts>
auto variant_match(const Variant && v, Ts... ts){
    return std::visit(overload{ts...}, v);
}

} // arctic::util
