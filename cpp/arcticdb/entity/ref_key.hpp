/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <fmt/format.h>
#include <arcticdb/entity/key.hpp>

namespace arcticdb::entity {
class RefKey {
  public:
    RefKey(StreamId id, KeyType key_type, bool old_type = false) :
        id_(std::move(id)),
        key_type_(key_type),
        old_type_(old_type) {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                !std::holds_alternative<StringId>(id_) || !std::get<StringId>(id_).empty(),
                "{} names cannot be empty strings",
                key_type == KeyType::VERSION_REF ? "Symbol" : "Snapshot"
        );
        util::check(
                old_type || is_ref_key_class(key_type),
                "Can't create ref key with non-ref key class keytype {}",
                key_type
        );
    }

    RefKey() = default;
    RefKey(const RefKey& other) = default;
    RefKey& operator=(const RefKey& other) = default;
    RefKey(RefKey&& other) = default;
    RefKey& operator=(RefKey&& other) = default;

    const StreamId& id() const { return id_; }
    const auto& type() const { return key_type_; }
    auto& type() { return key_type_; }
    auto is_old_type() const { return old_type_; }
    void change_type(KeyType new_type) { key_type_ = new_type; }

    friend bool operator==(const RefKey& l, const RefKey& r) { return l.type() == r.type() && l.id() == r.id(); }

    friend bool operator!=(const RefKey& l, const RefKey& r) { return !(l == r); }

    // TODO Neither key sorts by type
    friend bool operator<(const RefKey& l, const RefKey& r) { return l.id() < r.id(); }

    std::string_view view() const {
        if (str_.empty())
            set_string();
        return std::string_view{str_};
    }

    void set_string() const;

  private:
    StreamId id_;
    KeyType key_type_ = KeyType::UNDEFINED;
    mutable std::string str_;
    bool old_type_;
};
} // namespace arcticdb::entity

namespace fmt {
template<>
struct formatter<RefKey> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const RefKey& k, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}:{}", k.type(), k.id());
    }
};

} // namespace fmt

// TODO this is operating on the pretty-printed version and is needlessly inefficient
namespace std {
template<>
struct hash<arcticdb::entity::RefKey> {
    inline arcticdb::HashedValue operator()(const arcticdb::entity::RefKey& k) const noexcept {
        return arcticdb::hash(k.view());
    }
};
} // namespace std

namespace arcticdb::entity {
// Note: this needs to be defined after formatters.
inline void RefKey::set_string() const { str_ = fmt::format("{}", *this); }
} // namespace arcticdb::entity
