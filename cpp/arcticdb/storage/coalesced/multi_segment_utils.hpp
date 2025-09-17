#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/string_pool.hpp>

/*
 * Contains similar functions to stream_utils.hpp but assumes that many keys are mixed in together, so we can't
 * guarantee that either the id types or the index types will be the same, so we track those with a uint8_t column.
 */
namespace arcticdb {

static constexpr uint64_t NumericMask = 0x7FFFFF;
static constexpr uint64_t NumericFlag = uint64_t(1) << 31;
static_assert(NumericFlag > NumericMask);

template<typename StorageType>
uint64_t get_symbol_prefix(const StreamId& stream_id) {
    using InternalType = uint64_t;
    static_assert(sizeof(StorageType) <= sizeof(InternalType));
    constexpr size_t end = sizeof(InternalType);
    constexpr size_t begin = sizeof(InternalType) - sizeof(StorageType);
    StorageType data{};
    util::variant_match(
            stream_id,
            [&](const StringId& string_id) {
                auto* target = reinterpret_cast<char*>(&data);
                for (size_t p = begin, i = 0; p < end && i < string_id.size(); ++p, ++i) {
                    const auto c = string_id[i];
                    util::check(c < 127, "Out of bounds character {}", c);
                    target[i] = c;
                }
            },
            [&data](const NumericId& numeric_id) {
                util::check(numeric_id < static_cast<NumericId>(NumericMask), "Numeric id too large: {}", numeric_id);
                data &= NumericFlag;
                data &= numeric_id;
            }
    );
    return data;
}

enum class IdType : uint8_t { String, Numeric };

struct TimeSymbol {
    using IndexDataType = uint64_t;

    IndexDataType data_ = 0UL;

    TimeSymbol(const StreamId& stream_id, entity::timestamp time) { set_data(stream_id, time); }

    [[nodiscard]] IndexDataType data() const { return data_; }

    friend bool operator<(const TimeSymbol& left, const TimeSymbol& right) { return left.data() < right.data(); }

  private:
    void set_data(const StreamId& stream_id, entity::timestamp time) {
        time <<= 32;
        auto prefix = get_symbol_prefix<uint32_t>(stream_id);
        data_ = time | prefix;
    }
};

inline TimeSymbol time_symbol_from_key(const AtomKey& key) { return {key.id(), key.creation_ts()}; }
template<typename FieldType>
position_t as_pos(FieldType id_type) {
    return static_cast<position_t>(id_type);
}

template<typename FieldType>
StreamId get_id(position_t pos, const SegmentInMemory& segment) {
    auto id_type = IdType(segment.scalar_at<uint8_t>(pos, as_pos(FieldType::id_type)).value());
    const auto id = segment.scalar_at<uint64_t>(pos, as_pos(FieldType::stream_id));
    switch (id_type) {
    case IdType::String:
        return StreamId{std::string{segment.const_string_pool().get_const_view(id.value())}};
    case IdType::Numeric:
        return StreamId{static_cast<NumericId>(id.value())};
    default:
        util::raise_rte("Unknown id type {}", static_cast<int>(id_type));
    }
}

template<typename FieldType>
IndexValue get_index(position_t pos, FieldType field, const SegmentInMemory& segment) {
    auto index_type = VariantType(segment.scalar_at<uint8_t>(pos, as_pos(FieldType::index_type)).value());
    const auto index = segment.scalar_at<uint64_t>(pos, as_pos(field));
    switch (index_type) {
    case VariantType::STRING_TYPE:
        return StreamId{std::string{segment.const_string_pool().get_const_view(index.value())}};
    case VariantType::NUMERIC_TYPE:
        return StreamId{static_cast<NumericId>(index.value())};
    default:
        util::raise_rte("Unknown index type {} in multi_segment", static_cast<int>(index_type));
    }
}

template<typename FieldType>
entity::AtomKey get_key(position_t pos, const SegmentInMemory& segment) {
    const auto id = get_id<FieldType>(pos, segment);
    const auto key_type_num = segment.scalar_at<int32_t>(pos, as_pos(FieldType::key_type)).value();
    const auto key_type = entity::key_type_from_int(key_type_num);
    const auto version_id = segment.scalar_at<uint64_t>(pos, as_pos(FieldType::version_id)).value();
    const auto content_hash = segment.scalar_at<uint64_t>(pos, as_pos(FieldType::content_hash)).value();
    const auto creation_ts = segment.scalar_at<uint64_t>(pos, as_pos(FieldType::creation_ts)).value();
    const auto start_index = get_index(pos, FieldType::start_index, segment);
    const auto end_index = get_index(pos, FieldType::end_index, segment);

    auto key = atom_key_builder()
                       .version_id(version_id)
                       .content_hash(content_hash)
                       .creation_ts(creation_ts)
                       .start_index(start_index)
                       .end_index(end_index)
                       .build(id, key_type);

    return key;
}

template<typename FieldType>
void set_index(const IndexValue& index, FieldType field, SegmentInMemory& segment, bool set_type) {
    util::variant_match(
            index,
            [&segment, field, set_type](const StringIndex& string_index) {
                auto offset = segment.string_pool().get(std::string_view(string_index));
                segment.set_scalar<uint64_t>(as_pos(field), offset.offset());
                if (set_type)
                    segment.set_scalar<uint8_t>(
                            as_pos(FieldType::index_type), static_cast<uint8_t>(VariantType::STRING_TYPE)
                    );
            },
            [&segment, field, set_type](const NumericIndex& numeric_index) {
                segment.set_scalar<uint64_t>(as_pos(field), numeric_index);
                if (set_type)
                    segment.set_scalar<uint8_t>(
                            as_pos(FieldType::index_type), static_cast<uint8_t>(VariantType::NUMERIC_TYPE)
                    );
            }
    );
}

template<typename FieldType>
void set_id(const AtomKey& key, SegmentInMemory& segment) {
    util::variant_match(
            key.id(),
            [&segment](const StringId& string_id) {
                auto offset = segment.string_pool().get(std::string_view(string_id));
                segment.set_scalar<uint64_t>(as_pos(FieldType::stream_id), offset.offset());
                segment.set_scalar<uint8_t>(as_pos(FieldType::id_type), static_cast<uint8_t>(IdType::String));
            },
            [&segment](const NumericId& numeric_id) {
                segment.set_scalar<uint64_t>(as_pos(FieldType::stream_id), numeric_id);
                segment.set_scalar<uint8_t>(as_pos(FieldType::id_type), static_cast<uint8_t>(IdType::Numeric));
            }
    );
}

template<typename FieldType>
void set_key(const AtomKey& key, SegmentInMemory& segment) {
    set_id<FieldType>(key, segment);
    segment.set_scalar(as_pos(FieldType::version_id), key.version_id());
    set_index(key.start_index(), FieldType::start_index, segment, true);
    set_index(key.end_index(), FieldType::end_index, segment, false);
    segment.set_scalar(as_pos(FieldType::creation_ts), key.creation_ts());
    segment.set_scalar(as_pos(FieldType::content_hash), key.content_hash());
    segment.set_scalar<int32_t>(as_pos(FieldType::key_type), static_cast<int32_t>(key.type()));
}

} // namespace arcticdb
