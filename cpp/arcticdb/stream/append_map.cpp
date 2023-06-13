/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <pipeline/query.hpp>
#include <pipeline/python_output_frame.hpp>
#include <pipeline/frame_slice.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <pipeline/frame_utils.hpp>

namespace arcticdb::stream {

using namespace arcticdb::pipelines;
using namespace arcticdb::stream;

struct AppendMapEntry {
    AppendMapEntry() = default;

    pipelines::SliceAndKey slice_and_key_;
    std::optional<entity::AtomKey> next_key_;
    uint64_t total_rows_ = 0;

    const entity::StreamDescriptor& descriptor() const
    {
        return *slice_and_key_.slice_.desc();
    }

    entity::StreamDescriptor& descriptor()
    {
        return *slice_and_key_.slice_.desc();
    }

    const pipelines::FrameSlice& slice() const
    {
        return slice_and_key_.slice_;
    }

    const entity::AtomKey& key() const
    {
        return slice_and_key_.key();
    }

    friend bool operator<(const AppendMapEntry& l, const AppendMapEntry& r)
    {
        const auto& right_key = r.key();
        const auto& left_key = l.key();
        if (left_key.start_index() == right_key.start_index())
            return left_key.end_index() < right_key.end_index();

        return left_key.start_index() < right_key.start_index();
    }
};

AppendMapEntry
entry_from_key(const std::shared_ptr<stream::StreamSource>& store, const entity::AtomKey& key, bool load_data);

std::pair<std::optional<entity::AtomKey>, size_t> read_head(const std::shared_ptr<stream::StreamSource>& store,
    StreamId stream_id);

std::vector<AppendMapEntry> get_incomplete_append_slices_for_stream_id(const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    bool via_iteration,
    bool load_data);

inline bool has_appends_key(const std::shared_ptr<stream::StreamSource>& store, const RefKey& ref_key)
{
    return store->key_exists(ref_key).get();
}

inline std::vector<AppendMapEntry>
load_via_iteration(const std::shared_ptr<Store>& store, const StreamId& stream_id, bool load_data)
{
    auto prefix = std::holds_alternative<StringId>(stream_id) ? std::get<StringId>(stream_id) : std::string();

    std::vector<AppendMapEntry> output;

    store->iterate_type(KeyType::APPEND_DATA, [&store, load_data, &output, &stream_id](const auto& vk) {
        const auto& key = to_atom(vk);
        if (key.id() != stream_id)
            return;

        auto entry = entry_from_key(store, key, load_data);

        output.emplace_back(std::move(entry));
    });
    return output;
}

std::set<StreamId> get_incomplete_symbols(const std::shared_ptr<Store>& store)
{
    std::set<StreamId> output;

    store->iterate_type(KeyType::APPEND_DATA, [&output](const auto& vk) { output.insert(variant_key_id(vk)); });
    return output;
}

std::set<StreamId> get_incomplete_refs(const std::shared_ptr<Store>& store)
{
    std::set<StreamId> output;
    store->iterate_type(KeyType::APPEND_REF, [&output](const auto& vk) { output.insert(variant_key_id(vk)); });
    return output;
}

std::set<StreamId> get_active_incomplete_refs(const std::shared_ptr<Store>& store)
{
    std::set<StreamId> output;
    std::set<VariantKey> ref_keys;
    store->iterate_type(KeyType::APPEND_REF, [&ref_keys](const auto& vk) { ref_keys.insert(vk); });
    for (const auto& vk : ref_keys) {
        auto stream_id = variant_key_id(vk);
        auto [next_key, _] = read_head(store, stream_id);
        if (next_key && store->key_exists(next_key.value()).get()) {
            output.insert(stream_id);
        }
    }
    return output;
}

void fix_slice_rowcounts(std::vector<AppendMapEntry>& entries, size_t complete_rowcount)
{
    for (auto& entry : entries) {
        complete_rowcount = entry.slice_and_key_.slice_.fix_row_count(complete_rowcount);
    }
}

void write_parallel(const std::shared_ptr<Store>& store, const StreamId& stream_id, InputTensorFrame&& frame)
{
    // TODO: dynamic bucketize doesn't work with incompletes
    (void)write_incomplete_frame(store, stream_id, std::move(frame), std::nullopt).get();
}

std::vector<SliceAndKey> get_incomplete(const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const pipelines::FilterRange& range,
    uint64_t last_row,
    bool via_iteration,
    bool load_data)
{
    using namespace arcticdb::pipelines;

    std::unique_ptr<arcticdb::proto::descriptors::TimeSeriesDescriptor> unused;
    auto entries = get_incomplete_append_slices_for_stream_id(store, stream_id, via_iteration, load_data);

    util::variant_match(
        range,
        [](const RowRange&) { util::raise_rte("Only timestamp based ranges supported for filtering."); },
        [&entries](const IndexRange& index_range) {
            entries.erase(std::remove_if(std::begin(entries),
                              std::end(entries),
                              [&](const auto& entry) {
                                  return !intersects(index_range, entry.slice_and_key_.key().index_range());
                              }),
                std::end(entries));
        },
        [](const auto&) {
            // Don't know what to do with this index
        });

    fix_slice_rowcounts(entries, last_row);
    std::vector<SliceAndKey> output;
    for (const auto& entry : entries)
        output.push_back(entry.slice_and_key_);

    return output;
}

void write_head(const std::shared_ptr<Store>& store, const AtomKey& next_key, size_t total_rows)
{
    ARCTICDB_DEBUG(log::version(), "Writing append map head with key {}", next_key);
    auto any = pack_timeseries_descriptor({}, total_rows, next_key, {});
    SegmentInMemory segment;
    segment.set_metadata(std::move(any));
    store->write(KeyType::APPEND_REF, next_key.id(), std::move(segment)).get();
}

folly::Future<arcticdb::entity::VariantKey> write_incomplete_frame(const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    InputTensorFrame&& frame,
    std::optional<AtomKey>&& next_key)
{
    using namespace arcticdb::pipelines;

    auto index_range = frame.index_range;
    auto segment = segment_from_frame(std::move(frame), 0, std::move(next_key));
    return store->write(KeyType::APPEND_DATA,
        VersionId(0),
        stream_id,
        index_range.start_,
        index_range.end_,
        std::move(segment));
}

void remove_incomplete_segments(const std::shared_ptr<Store>& store, const StreamId& stream_id)
{
    delete_keys_of_type_for_stream(store, stream_id, KeyType::APPEND_DATA);
}

//TODO move this somewhere else
StreamDescriptor merge_descriptors(const StreamDescriptor& original,
    const std::vector<StreamDescriptor::FieldsCollection>& entries,
    const std::unordered_set<std::string_view>& filtered_set,
    const std::optional<IndexDescriptor>& default_index)
{

    std::vector<std::string_view> merged_fields;
    std::unordered_map<std::string_view, arcticdb::proto::descriptors::TypeDescriptor> merged_fields_map;

    for (const auto& field : original.fields()) {
        merged_fields.push_back(field.name());
        merged_fields_map.try_emplace(field.name(), field.type_desc());
    }

    auto index = empty_index();
    if (original.index().uninitialized()) {
        if (default_index) {
            auto temp_idx = default_index_type_from_descriptor(default_index->proto());
            util::variant_match(temp_idx, [&merged_fields, &merged_fields_map](const auto& idx) {
                using IndexType = std::decay_t<decltype(idx)>;
                merged_fields.emplace_back(idx.name());
                merged_fields_map.try_emplace(idx.name(),
                    static_cast<TypeDescriptor>(typename IndexType::TypeDescTag{}).proto());
            });
        } else
            util::raise_rte("Descriptor has uninitialized index and no default supplied");
    } else {
        index = index_type_from_descriptor(original);
    }

    const bool has_index = !std::holds_alternative<RowCountIndex>(index);

    // Merge all the fields for all slices, apart from the index which we already have from the first descriptor.
    // Note that we preserve the ordering as we see columns, especially the index which needs to be column 0.
    for (const auto& fields : entries) {
        if (has_index)
            util::variant_match(index, [&fields](const auto& idx) { idx.check(fields); });

        for (size_t idx = has_index ? 1u : 0u; idx < static_cast<size_t>(fields.size()); ++idx) {
            const auto& field = fields[idx];
            auto type_desc = type_desc_from_proto(field.type_desc());
            if (filtered_set.empty() || (filtered_set.find(field.name()) != filtered_set.end())) {
                if (auto existing = merged_fields_map.find(field.name()); existing != merged_fields_map.end()) {
                    auto existing_type_desc = type_desc_from_proto(existing->second);
                    if (existing_type_desc != type_desc) {
                        log::version().info("Got incompatible types: {} {}", existing_type_desc, type_desc);
                        auto new_descriptor = has_valid_common_type(existing_type_desc, type_desc);
                        if (new_descriptor) {
                            merged_fields_map[field.name()] = new_descriptor->proto();
                        } else {
                            util::raise_rte("No valid common type between {} and {} for column {}",
                                existing_type_desc,
                                type_desc,
                                field.name());
                        }
                    }
                } else {
                    merged_fields.push_back(field.name());
                    merged_fields_map.try_emplace(field.name(), type_desc);
                }
            }
        }
    }
    StreamDescriptor::FieldsCollection new_fields{};
    for (const auto& field_name : merged_fields) {
        auto new_field = new_fields.Add();
        new_field->set_name(field_name.data(), field_name.size());
        new_field->mutable_type_desc()->CopyFrom(merged_fields_map[field_name]);
    }
    return arcticdb::entity::StreamDescriptor{original.id(), get_descriptor_from_index(index), std::move(new_fields)};
}

StreamDescriptor merge_descriptors(const StreamDescriptor& original,
    const std::vector<StreamDescriptor::FieldsCollection>& entries,
    const std::vector<std::string>& filtered_columns,
    const std::optional<IndexDescriptor>& default_index)
{
    std::unordered_set<std::string_view> filtered_set(filtered_columns.begin(), filtered_columns.end());
    return merge_descriptors(original, entries, filtered_set, default_index);
}

StreamDescriptor merge_descriptors(const StreamDescriptor& original,
    const std::vector<SliceAndKey>& entries,
    const std::vector<std::string>& filtered_columns,
    const std::optional<IndexDescriptor>& default_index)
{
    std::vector<StreamDescriptor::FieldsCollection> fields;
    for (const auto& entry : entries) {
        fields.push_back(entry.slice_.desc()->fields());
    }
    return merge_descriptors(original, fields, filtered_columns, default_index);
}

StreamDescriptor merge_descriptors(const std::shared_ptr<Store>& store,
    const StreamDescriptor& original,
    const std::vector<SliceAndKey>& entries,
    const std::unordered_set<std::string_view>& filtered_set,
    const std::optional<IndexDescriptor>& default_index)
{
    std::vector<StreamDescriptor::FieldsCollection> fields;
    for (const auto& entry : entries) {
        fields.push_back(entry.segment(store).descriptor().fields());
    }
    return merge_descriptors(original, fields, filtered_set, default_index);
}

std::vector<AppendMapEntry>
load_via_list(const std::shared_ptr<Store>& store, const StreamId& stream_id, bool load_data)
{
    using namespace arcticdb::pipelines;

    std::unique_ptr<arcticdb::proto::descriptors::TimeSeriesDescriptor> tsd;
    ARCTICDB_DEBUG(log::version(), "Getting incomplete segments for stream {}", stream_id);
    ARCTICDB_SAMPLE_DEFAULT(GetIncomplete)

    auto [next_key, total_rows] = read_head(store, stream_id);
    std::vector<AppendMapEntry> output;

    try {
        while (next_key) {
            auto entry = entry_from_key(store, next_key.value(), load_data);
            next_key = entry.next_key_;
            output.emplace_back(std::move(entry));
        }
    } catch (const storage::KeyNotFoundException&) {
        // Most likely compacted up to this point
    }
    return output;
}

std::pair<std::optional<AtomKey>, size_t> read_head(const std::shared_ptr<StreamSource>& store, StreamId stream_id)
{
    auto ref_key = RefKey{std::move(stream_id), KeyType::APPEND_REF};
    auto output = std::make_pair<std::optional<AtomKey>, size_t>(std::nullopt, 0);

    if (!has_appends_key(store, ref_key))
        return output;

    auto fut = store->read(ref_key);
    auto [key, seg] = std::move(fut).get();
    auto tsd = timeseries_descriptor_from_segment(seg);
    if (tsd.has_next_key()) {
        output.first = decode_key(tsd.next_key());
    }

    output.second = tsd.total_rows();
    return output;
}

std::pair<arcticdb::proto::descriptors::TimeSeriesDescriptor, std::optional<SegmentInMemory>>
get_descriptor_and_data(const std::shared_ptr<StreamSource>& store, const AtomKey& k, bool load_data)
{
    if (load_data) {
        auto [key, seg] = store->read(k).get();
        auto tsd = timeseries_descriptor_from_any(*seg.metadata());
        return std::make_pair(std::move(tsd), std::make_optional<SegmentInMemory>(std::move(seg)));
    } else {
        auto [key, metadata, descriptor] = store->read_metadata_and_descriptor(k).get();
        util::check(static_cast<bool>(metadata), "Failed to get append metadata from key {}", key);
        util::check(!std::holds_alternative<StringId>(variant_key_id(key)) ||
                        !std::get<StringId>(variant_key_id(key)).empty(),
            "Unexpected empty id");
        auto tsd = timeseries_descriptor_from_any(metadata.value());
        *tsd.mutable_stream_descriptor() = std::move(descriptor);
        return std::make_pair(std::move(tsd), std::nullopt);
    }
}

AppendMapEntry create_entry(const arcticdb::proto::descriptors::TimeSeriesDescriptor& tsd)
{
    AppendMapEntry entry;

    if (tsd.has_next_key())
        entry.next_key_ = decode_key(tsd.next_key());

    entry.total_rows_ = tsd.total_rows();
    return entry;
}

AppendMapEntry entry_from_key(const std::shared_ptr<StreamSource>& store, const AtomKey& key, bool load_data)
{
    auto [tsd, seg] = get_descriptor_and_data(store, key, load_data);
    auto entry = create_entry(tsd);
    auto descriptor = std::make_shared<StreamDescriptor>();
    auto index_field_count = tsd.stream_descriptor().index().field_count();
    auto field_count = tsd.stream_descriptor().fields().size();
    auto frame_slice = FrameSlice{std::make_shared<StreamDescriptor>(std::move(*tsd.mutable_stream_descriptor())),
        ColRange{index_field_count, field_count},
        RowRange{0, entry.total_rows_}};
    entry.slice_and_key_ = SliceAndKey{std::move(frame_slice), key, std::move(seg)};
    return entry;
}

void append_incomplete(const std::shared_ptr<Store>& store, const StreamId& stream_id, InputTensorFrame&& frame)
{
    using namespace arcticdb::proto::descriptors;
    using namespace arcticdb::stream;
    ARCTICDB_SAMPLE_DEFAULT(AppendIncomplete)
    ARCTICDB_DEBUG(log::version(), "Writing incomplete frame for stream {}", stream_id);

    auto [next_key, total_rows] = read_head(store, stream_id);
    total_rows += frame.num_rows;
    auto desc = frame.desc.clone();
    auto new_key = write_incomplete_frame(store, stream_id, std::move(frame), std::move(next_key)).get();

    ARCTICDB_DEBUG(log::version(),
        "Wrote incomplete frame for stream {}, {} rows, total rows {}",
        stream_id,
        frame.num_rows,
        total_rows);
    write_head(store, to_atom(new_key), total_rows);
}

void append_incomplete_segment(const std::shared_ptr<Store>& store, const StreamId& stream_id, SegmentInMemory&& seg)
{
    using namespace arcticdb::proto::descriptors;
    using namespace arcticdb::stream;
    ARCTICDB_SAMPLE_DEFAULT(AppendIncomplete)
    ARCTICDB_DEBUG(log::version(), "Writing incomplete segment for stream {}", stream_id);

    auto [next_key, total_rows] = read_head(store, stream_id);

    auto start_index = TimeseriesIndex::start_value_for_segment(seg);
    auto end_index = TimeseriesIndex::end_value_for_segment(seg);
    auto seg_row_count = seg.row_count();

    auto any = pack_timeseries_descriptor({}, seg_row_count, next_key, {});
    seg.set_metadata(std::move(any));
    auto new_key =
        store->write(arcticdb::stream::KeyType::APPEND_DATA, 0, stream_id, start_index, end_index, std::move(seg))
            .get();

    total_rows += seg_row_count;
    ARCTICDB_DEBUG(log::version(),
        "Wrote incomplete frame for stream {}, {} rows, total rows {}",
        stream_id,
        seg_row_count,
        total_rows);
    write_head(store, to_atom(std::move(new_key)), total_rows);
}

std::vector<AppendMapEntry> get_incomplete_append_slices_for_stream_id(const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    bool via_iteration,
    bool load_data)
{
    using namespace arcticdb::pipelines;
    std::vector<AppendMapEntry> entries;

    if (via_iteration) {
        entries = load_via_iteration(store, stream_id, load_data);
    } else {
        entries = load_via_list(store, stream_id, load_data);
    }

    if (!entries.empty()) {
        auto index_desc = entries[0].descriptor().index();

        if (index_desc.type() != IndexDescriptor::ROWCOUNT) {
            std::sort(std::begin(entries), std::end(entries));
        } else {
            // Can't sensibly sort rowcount indexes, so you'd better have written them in the right order
            std::reverse(std::begin(entries), std::end(entries));
        }
    }
    return entries;
}

std::optional<int64_t> latest_incomplete_timestamp(const std::shared_ptr<Store>& store, const StreamId& stream_id)
{
    auto [next_key, total_rows] = read_head(store, stream_id);
    if (next_key && store->key_exists(next_key.value()).get())
        return next_key.value().end_time();

    return std::nullopt;
}
} // namespace arcticdb::stream
