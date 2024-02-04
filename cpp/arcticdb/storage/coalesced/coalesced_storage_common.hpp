#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/key.hpp>

namespace arcticdb {

static constexpr uint64_t NumericMask = 0x7FFFFF;
static constexpr uint64_t NumericFlag = uint64_t(1) << 31;
static_assert(NumericFlag > NumericMask);

template<typename StorageType>
uint64_t get_symbol_prefix(const entity::StreamId& stream_id) {
    using InternalType = uint64_t;
    static_assert(sizeof(StorageType) <= sizeof(InternalType));
    constexpr size_t end = sizeof(InternalType);
    constexpr size_t begin = sizeof(InternalType) - sizeof(StorageType);
    InternalType data = 0UL;
    util::variant_match(stream_id,
                        [&data] (const entity::StringId& string_id) {
                            auto* target = reinterpret_cast<char*>(&data);
                            for(size_t p = begin, i = 0; p < end && i < string_id.size(); ++p, ++i) {
                                const auto c = string_id[i];
                                util::check(c < 127, "Out of bounds character {}", c);
                                target[p] = c;
                            }
                        },
                        [&data] (const entity::NumericId& numeric_id) {
                            util::check(numeric_id < static_cast<entity::NumericId>(NumericMask), "Numeric id too large: {}", numeric_id);
                            data &= NumericFlag;
                            data &= numeric_id;
                        }
    );
    return data;
}

struct TimeSymbol {
    using IndexDataType = uint64_t;

    IndexDataType data_ = 0UL;

    TimeSymbol(const entity::StreamId& stream_id, entity::timestamp time) {
        set_data(stream_id, time);
    }

    [[nodiscard]] IndexDataType data() const {
        return data_;
    }

    friend bool operator<(const TimeSymbol& left, const TimeSymbol& right) {
        return left.data() < right.data();
    }

private:
    void set_data(const entity::StreamId& stream_id, entity::timestamp time) {
        time <<= 32;
        auto prefix = get_symbol_prefix<uint32_t>(stream_id);
        data_ = time & prefix;
    }
};

inline TimeSymbol time_symbol_from_key(const AtomKey& key) {
    return {key.id(), key.creation_ts()};
}

struct IndexVersionPair {
    AtomKey index_;
    AtomKey version_;

    IndexVersionPair(AtomKey&& index, AtomKey&& version) :
        index_(std::move(index)),
        version_(std::move(version)) {
        util::check(is_index_key_type(index_.type()), "Expected index-type key, got {}", index_);
        util::check(version_.type() == KeyType::VERSION, "Expected version key type, got {}", version_);
    }
};

inline std::string coalesced_id_for_key_type(KeyType key_type) {
    return fmt::format("__coalesce_{}__", key_type_long_name(key_type));
}


} //namespace arcticdb