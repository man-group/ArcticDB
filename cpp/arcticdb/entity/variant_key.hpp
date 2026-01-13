/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/ref_key.hpp>
#include <variant>
#include <ranges>

namespace arcticdb::entity {

template<class It>
using Range = std::ranges::subrange<It>;

using VariantKey = std::variant<entity::AtomKey, entity::RefKey>;

using IterateTypeVisitor = std::function<void(VariantKey&& key)>;

using IterateTypePredicate = std::function<bool(VariantKey&& key)>;

// Aliases to clarify usage and allow more detailed typing in the future, similar to aliases for AtomKey:
/** Should be a SNAPSHOT_REF key or the legacy SNAPSHOT AtomKey. */
using SnapshotVariantKey = VariantKey;

template<typename KeyType>
requires std::same_as<std::decay_t<KeyType>, VariantKey>
decltype(auto) to_atom(KeyType&& vk) {
    return std::get<AtomKey>(std::forward<KeyType>(vk));
}

template<typename KeyType>
requires std::same_as<std::decay_t<KeyType>, VariantKey>
decltype(auto) to_ref(KeyType&& vk) {
    return std::get<RefKey>(std::forward<KeyType>(vk));
}

inline std::string_view variant_key_view(const VariantKey& vk) {
    return std::visit([](const auto& key) { return key.view(); }, vk);
}

inline KeyType variant_key_type(const VariantKey& vk) {
    return std::visit([](const auto& key) { return key.type(); }, vk);
}

inline const StreamId& variant_key_id(const VariantKey& vk) {
    return std::visit([](const auto& key) -> const StreamId& { return key.id(); }, vk);
}

inline bool variant_key_id_empty(const VariantKey& vk) {
    return std::visit(
            [](const auto& key) {
                return !std::holds_alternative<NumericId>(key.id()) && std::get<StringId>(key.id()).empty();
            },
            vk
    );
}

inline bool variant_key_id_starts_with(const VariantKey& vk, std::string_view prefix) {
    const auto& stream_id = variant_key_id(vk);
    return util::variant_match(
            stream_id,
            [prefix](const StringId& id) { return id.starts_with(prefix); },
            [prefix](NumericId id) { return std::to_string(id).starts_with(prefix); }
    );
}

} // namespace arcticdb::entity

namespace fmt {
using namespace arcticdb::entity;

template<>
struct formatter<arcticdb::entity::VariantKey> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::entity::VariantKey& k, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", variant_key_view(k));
    }
};

} // namespace fmt
