#pragma once

#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/coalesced/multi_segment_utils.hpp>

namespace arcticdb::storage {

enum class MultiSegmentFields : uint32_t {
    time_symbol,
    stream_id,
    version_id,
    start_index,
    end_index,
    creation_ts,
    content_hash,
    index_type,
    id_type,
    key_type,
    offset,
    size
};

inline StreamDescriptor multi_segment_descriptor(StreamId stream_id) {
    return stream_descriptor(
            std::move(stream_id),
            stream::RowCountIndex(),
            {scalar_field(DataType::INT64, "time_symbol"),
             scalar_field(DataType::UINT64, "stream_id"),
             scalar_field(DataType::UINT64, "version_id"),
             scalar_field(DataType::UINT64, "start_index"),
             scalar_field(DataType::UINT64, "end_index"),
             scalar_field(DataType::UINT64, "creation_ts"),
             scalar_field(DataType::UINT64, "content_hash"),
             scalar_field(DataType::UINT8, "index_type"),
             scalar_field(DataType::UINT8, "id_type"),
             scalar_field(DataType::UINT32, "key_type"),
             scalar_field(DataType::UINT64, "offset"),
             scalar_field(DataType::UINT64, "size")}
    );
}

template<typename FieldType>
std::pair<size_t, size_t> get_offset_and_size(size_t pos, const SegmentInMemory& segment) {
    auto result = std::make_pair(
            segment.scalar_at<uint64_t>(pos, as_pos(FieldType::offset)).value(),
            segment.scalar_at<uint64_t>(pos, as_pos(FieldType::size)).value()
    );
    ARCTICDB_DEBUG(
            log::storage(),
            "At pos {}, multi segment header found offset and size {}:{}",
            pos,
            result.first,
            result.second
    );
    return result;
}

class MultiSegmentHeader {
    SegmentInMemory segment_;
    std::mutex mutex_;

  public:
    using TimeSymbolTag = ScalarTagType<DataTypeTag<DataType::INT64>>;

    explicit MultiSegmentHeader(StreamId id) : segment_(multi_segment_descriptor(std::move(id))) {}

    explicit MultiSegmentHeader(SegmentInMemory segment) : segment_(std::move(segment)) {}

    MultiSegmentHeader() = default;

    void set_segment(SegmentInMemory&& segment) { segment_ = std::move(segment); }

    void initalize(StreamId id, size_t num_rows) {
        segment_ = SegmentInMemory{multi_segment_descriptor(std::move(id)), num_rows, AllocationType::DYNAMIC};
    }

    void add_key_and_offset(const AtomKey& key, uint64_t offset, uint64_t size) {
        auto time_sym = time_symbol_from_key(key).data();
        std::lock_guard lock(mutex_);
        ARCTICDB_DEBUG(
                log::storage(), "Adding key {} with offset {} and {} bytes, time_sym: {}", key, offset, size, time_sym
        );
        segment_.set_scalar(as_pos(MultiSegmentFields::time_symbol), time_sym);
        set_key<MultiSegmentFields>(key, segment_);
        segment_.set_scalar(as_pos(MultiSegmentFields::offset), offset);
        segment_.set_scalar(as_pos(MultiSegmentFields::size), size);
        segment_.end_row();
    }

    void sort() { segment_.sort(0); }

    [[nodiscard]] const SegmentInMemory& segment() const { return segment_; }

    SegmentInMemory&& detach_segment() { return std::move(segment_); }

    [[nodiscard]] std::optional<std::pair<size_t, size_t>> get_offset_for_key(const AtomKey& key) const {
        ARCTICDB_DEBUG(log::storage(), "Multi segment header searching for key {}", key);
        const auto& time_symbol_column = segment_.column(0);
        const auto time_symbol = time_symbol_from_key(key);
        auto start_pos = std::lower_bound(
                time_symbol_column.begin<TimeSymbolTag>(), time_symbol_column.end<TimeSymbolTag>(), time_symbol.data()
        );
        if (start_pos == time_symbol_column.end<TimeSymbolTag>()) {
            ARCTICDB_DEBUG(log::storage(), "Reached end of column looking for symbol {}", key);
            return std::nullopt;
        }

        ARCTICDB_DEBUG(
                log::storage(), "Start pos for time symbol {} is {}", time_symbol.data(), start_pos.get_offset()
        );
        const auto& creation_ts_column = segment_.column(as_pos(MultiSegmentFields::creation_ts));
        using CreationTsTag = ScalarTagType<DataTypeTag<DataType::UINT64>>;
        auto creation_it = creation_ts_column.begin<CreationTsTag>();
        creation_it.advance(start_pos.get_offset());
        auto creation_ts_pos =
                std::lower_bound(creation_it, creation_ts_column.end<CreationTsTag>(), key.creation_ts());
        if (creation_ts_pos == creation_ts_column.end<CreationTsTag>()) {
            ARCTICDB_DEBUG(log::storage(), "Reached end of column looking for timestamp {}", key.creation_ts());
            return std::nullopt;
        }

        ARCTICDB_DEBUG(
                log::storage(),
                "Starting at creation timestamp {} at offset {}",
                *creation_ts_pos,
                creation_ts_pos.get_offset()
        );
        while (*creation_ts_pos == static_cast<uint64_t>(key.creation_ts())) {
            const auto creation_ts_offset = creation_ts_pos.get_offset();
            if (const auto found_key = get_key<MultiSegmentFields>(creation_ts_offset, segment_); found_key == key) {
                ARCTICDB_DEBUG(log::storage(), "Got key {} from multi-segment header", key);
                return get_offset_and_size<MultiSegmentFields>(creation_ts_offset, segment_);
            }

            ++creation_ts_pos;
        }
        ARCTICDB_DEBUG(log::storage(), "Failed to find offset for key {}", key);
        return std::nullopt;
    }
};

} // namespace arcticdb::storage
