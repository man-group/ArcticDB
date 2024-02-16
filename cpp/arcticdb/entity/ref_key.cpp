/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <fmt/format.h>
#include <arcticdb/entity/ref_key.hpp>

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

namespace arcticdb::entity {

RefKey::RefKey(StreamId id, KeyType key_type, bool old_type):
                id_(std::move(id)),
                key_type_(key_type),
                old_type_(old_type) {
            util::check(!std::holds_alternative<StringId>(id_) || !std::get<StringId>(id_).empty(), "Empty symbol in reference key");
            util::check(old_type || is_ref_key_class(key_type), "Can't create ref key with non-ref key class keytype {}", key_type);
        }

const StreamId& RefKey::id() const { return id_; }
const KeyType& RefKey::type() const { return key_type_; }
KeyType& RefKey::type() { return key_type_; }
bool RefKey::is_old_type() const { return old_type_; }
void RefKey::change_type(KeyType new_type) {
    key_type_ = new_type;
}

bool operator==(const RefKey &l, const RefKey &r) {
    return l.type() == r.type()
           && l.id() == r.id();
}

bool operator!=(const RefKey &l, const RefKey &r) {
    return !(l == r);
}

//TODO Neither key sorts by type
bool operator<(const RefKey &l, const RefKey &r) {
    return l.id() < r.id();
}

std::string_view RefKey::view() const { if(str_.empty()) set_string(); return std::string_view{str_}; }

void RefKey::set_string() const {
    str_ = fmt::format("{}", *this);
}
} // namespace arcticdb::entity
