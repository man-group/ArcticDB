/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/format.h>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/key.hpp>

namespace arcticdb::entity {
    class RefKey {
    public:

        RefKey(StreamId id, KeyType key_type, bool old_type = false);

        RefKey() = default;
        RefKey(const RefKey &other) = default;
        RefKey &operator=(const RefKey &other) = default;
        RefKey(RefKey &&other) = default;
        RefKey &operator=(RefKey &&other) = default;

        const StreamId& id() const;
        const KeyType& type() const;
        KeyType& type();
        bool is_old_type() const;
        void change_type(KeyType new_type);

        friend bool operator==(const RefKey &l, const RefKey &r);

        friend bool operator!=(const RefKey &l, const RefKey &r);

        //TODO Neither key sorts by type
        friend bool operator<(const RefKey &l, const RefKey &r);

        std::string_view view() const;

        void set_string() const;
    private:

        StreamId id_;
        KeyType key_type_ = KeyType::UNDEFINED;
        mutable std::string str_;
        bool old_type_;

    };
} // namespace arcticdb::entity

template<>
struct fmt::formatter<arcticdb::entity::RefKey>
{
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::entity::RefKey& number, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "{}:{}", number.type(), number.id());
    }
};

//TODO this is operating on the pretty-printed version and is needlessly inefficient
namespace std {
template<>
struct hash<arcticdb::entity::RefKey> {
    inline arcticdb::HashedValue operator()(const arcticdb::entity::RefKey &k) const noexcept {
        auto view = k.view();
        return arcticdb::hash(const_cast<uint8_t * >(reinterpret_cast<const uint8_t *>(view.data())), view.size());
    }
};
}


