/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/ref_key.hpp>
#include <variant>
#include <folly/Range.h>

namespace arcticdb::entity {

template<class It>
using Range = folly::Range<It>;

using VariantKey = std::variant<entity::AtomKey, entity::RefKey>;

using IterateTypeVisitor = std::function<void(VariantKey &&key)>;

// Aliases to clarify usage and allow more detailed typing in the future, similar to aliases for AtomKey:
/** Should be a SNAPSHOT_REF key or the legacy SNAPSHOT AtomKey. */
using SnapshotVariantKey = VariantKey;

inline AtomKey& to_atom(VariantKey& vk) {
    return std::get<AtomKey>(vk);
}

inline const AtomKey& to_atom(const VariantKey& vk) {
    return std::get<AtomKey>(vk);
}

inline AtomKey to_atom(VariantKey&& vk) {
    return std::get<AtomKey>(std::move(vk));
}

inline RefKey& to_ref(VariantKey& vk) {
    return std::get<RefKey>(vk);
}

inline const RefKey& to_ref(const VariantKey& vk) {
    return std::get<RefKey>(vk);
}

inline std::string_view variant_key_view(const VariantKey &vk) {
    return std::visit([](const auto &key) { return key.view(); }, vk);
}

inline KeyType variant_key_type(const VariantKey &vk) {
    return std::visit([](const auto &key) { return key.type(); }, vk);
}

inline const StreamId& variant_key_id(const VariantKey &vk) {
    return std::visit([](const auto &key) -> const StreamId& { return key.id(); }, vk);
}

inline bool variant_key_id_empty(const VariantKey &vk) {
    return std::visit([](const auto &key) {
        return !std::holds_alternative<NumericId>(key.id()) && std::get<StringId>(key.id()).empty();
    }, vk);
}

} // namespace arcticdb::entity

namespace fmt {
using namespace arcticdb::entity;

template<>
struct formatter<arcticdb::entity::VariantKey> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::entity::VariantKey &k, FormatContext &ctx) const {
        return format_to(ctx.out(), "{}", variant_key_view(k));
    }
};

} //namespace fmt
