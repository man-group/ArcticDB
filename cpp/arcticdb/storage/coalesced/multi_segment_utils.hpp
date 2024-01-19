#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

/*
 * Contains similar functions to stream_utils.hpp but assumes that many keys are mixed in together, so we can't guarantee that
 * either the id types or the index types will be the same, so we track those with a uint8_t column.
 */
namespace arcticdb {

enum class IdType : uint8_t {
    String,
    Numeric
};

template <typename FieldType>
position_t as_pos(FieldType id_type) {
    return static_cast<position_t>(id_type);
}

template <typename FieldType>
StreamId get_id(position_t pos, const SegmentInMemory& segment) {
    auto id_type = IdType(segment.scalar_at<uint8_t>(pos, as_pos(FieldType::id_type)).value());
    const auto id = segment.scalar_at<uint64_t>(pos, as_pos(FieldType::stream_id));
    switch(id_type) {
    case IdType::String:
        return StreamId{std::string{segment.const_string_pool().get_const_view(id.value())}};
    case IdType::Numeric:
        return StreamId{static_cast<NumericId>(id.value())};
    default:
        util::raise_rte("Unknown id type {}", static_cast<int>(id_type));
    }
}

template <typename FieldType>
IndexValue get_index(position_t pos, FieldType field, const SegmentInMemory& segment) {
    auto index_type = VariantType(segment.scalar_at<uint8_t>(pos, as_pos(FieldType::index_type)).value());
    const auto index = segment.scalar_at<uint64_t>(pos, as_pos(field));
    switch(index_type) {
    case VariantType::STRING_TYPE:
        return StreamId{std::string{segment.const_string_pool().get_const_view(index.value())}};
    case VariantType::NUMERIC_TYPE:
        return StreamId{static_cast<NumericId>(index.value())};
    default:
        util::raise_rte("Unknown index type {}", static_cast<int>(index_type));
    }
}

template <typename FieldType>
entity::AtomKey get_key(position_t pos, const SegmentInMemory& segment) {
    return atom_key_builder()
        .version_id(segment.scalar_at<uint64_t>(pos, as_pos(FieldType::version_id)).value())
        .content_hash(segment.scalar_at<uint64_t>(pos, as_pos(FieldType::content_hash)).value())
        .creation_ts(segment.scalar_at<uint64_t>(pos, as_pos(FieldType::creation_ts)).value())
        .start_index(get_index(pos, FieldType::start_index, segment))
        .end_index(get_index(pos, FieldType::end_index, segment))
        .build(get_id<FieldType>(pos, segment), entity::key_type_from_int(segment.scalar_at<uint8_t>(pos, as_pos(FieldType::key_type)).value()));
}

template <typename FieldType>
void set_index(const IndexValue &index, FieldType field, SegmentInMemory& segment) {
    util::variant_match(index,
        [&segment, field](const StringIndex &string_index) {
            auto offset = segment.string_pool().get(std::string_view(string_index));
            segment.set_scalar<uint64_t>(as_pos(field), offset.offset());
            segment.set_scalar<uint64_t>(as_pos(FieldType::index_type),
                                          static_cast<uint8_t>(VariantType::STRING_TYPE));
        },
        [&segment, field](const NumericIndex &numeric_index) {
            segment.set_scalar<uint64_t>(as_pos(field), numeric_index);
            segment.set_scalar<uint64_t>(as_pos(FieldType::index_type),
                                          static_cast<uint8_t>(VariantType::NUMERIC_TYPE));
        });
}

template <typename FieldType>
void set_id(const AtomKey &key, SegmentInMemory& segment) {
    util::variant_match(key.id(),
        [&segment](const StringId &string_id) {
            auto offset = segment.string_pool().get(std::string_view(string_id));
            segment.set_scalar<uint64_t>(as_pos(FieldType::stream_id), offset.offset());
            segment.set_scalar<uint64_t>(as_pos(FieldType::id_type),
                                          static_cast<uint8_t>(IdType::String));
        },
        [&segment](const NumericId &numeric_id) {
            segment.set_scalar<uint64_t>(as_pos(FieldType::stream_id), numeric_id);
            segment.set_scalar<uint64_t>(as_pos(FieldType::id_type),
                                          static_cast<uint8_t>(IdType::Numeric));
        });
}

template <typename FieldType> 
void set_key(const AtomKey& key, SegmentInMemory& segment) {
    set_id<FieldType>(key, segment);
    segment.set_scalar(as_pos(FieldType::version_id), key.version_id());
    set_index(key.start_index(), FieldType::start_index, segment);
    set_index(key.end_index(), FieldType::end_index, segment);
    segment.set_scalar(as_pos(FieldType::creation_ts), key.creation_ts());
    segment.set_scalar(as_pos(FieldType::content_hash), key.content_hash());
    segment.set_scalar(as_pos(FieldType::key_type), static_cast<char>(key.type()));

}

} //namespace arcticdb

