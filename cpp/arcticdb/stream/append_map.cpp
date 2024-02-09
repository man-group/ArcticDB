/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <pipeline/query.hpp>
#include <pipeline/frame_slice.hpp>
#include <util/key_utils.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>

namespace arcticdb {

using namespace arcticdb::pipelines;
using namespace arcticdb::stream;

struct AppendMapEntry {
    AppendMapEntry() = default;

    pipelines::SliceAndKey slice_and_key_;
    std::optional<entity::AtomKey> next_key_;
    uint64_t total_rows_ = 0;

    const entity::StreamDescriptor& descriptor() const {
        return *slice_and_key_.slice_.desc();
    }

    entity::StreamDescriptor& descriptor() {
        return *slice_and_key_.slice_.desc();
    }

    const pipelines::FrameSlice& slice() const {
        return slice_and_key_.slice_;
    }

    const entity::AtomKey & key() const{
        return slice_and_key_.key();
    }

    friend bool operator<(const AppendMapEntry& l, const AppendMapEntry& r) {
        const auto& right_key = r.key();
        const auto& left_key = l.key();
        if(left_key.start_index() == right_key.start_index())
            return  left_key.end_index() < right_key.end_index();

        return left_key.start_index() < right_key.start_index();
    }
};


AppendMapEntry entry_from_key(
    const std::shared_ptr<stream::StreamSource>& store,
    const entity::AtomKey& key,
    bool load_data);

//std::pair<std::optional<entity::AtomKey>, size_t> read_head(
//    const std::shared_ptr<stream::StreamSource>& store,
//    StreamId stream_id);

std::vector<AppendMapEntry> get_incomplete_append_slices_for_stream_id(
    const std::shared_ptr<Store> &store,
    const StreamId &stream_id,
    bool via_iteration,
    bool load_data);

inline bool has_appends_key(
    const std::shared_ptr<stream::StreamSource>& store,
    const RefKey& ref_key) {
    return store->key_exists(ref_key).get();
}

inline std::vector<AppendMapEntry> load_via_iteration(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    bool load_data
) {
    auto prefix = std::holds_alternative<StringId>(stream_id) ? std::get<StringId>(stream_id) : std::string();

    std::vector<AppendMapEntry> output;

    store->iterate_type(KeyType::APPEND_DATA, [&store, load_data, &output, &stream_id] (const auto& vk) {
        const auto& key = to_atom(vk);
        if(key.id() != stream_id)
            return;

        auto entry = entry_from_key(store, key, load_data);

        output.emplace_back(std::move(entry));
    });
    return output;
}

std::set<StreamId> get_incomplete_symbols(const std::shared_ptr<Store>& store) {
    std::set<StreamId> output;

    store->iterate_type(KeyType::APPEND_DATA, [&output] (const auto& vk) {
        output.insert(variant_key_id(vk));
    });
    return output;
}

std::set<StreamId> get_incomplete_refs(const std::shared_ptr<Store>& store) {
    std::set<StreamId> output;
    store->iterate_type(KeyType::APPEND_REF, [&output] (const auto& vk) {
        output.insert(variant_key_id(vk));
    });
    return output;
}

std::set<StreamId> get_active_incomplete_refs(const std::shared_ptr<Store>& store) {
    std::set<StreamId> output;
    std::set<VariantKey> ref_keys;
    store->iterate_type(KeyType::APPEND_REF, [&ref_keys] (const auto& vk) {
        ref_keys.insert(vk);
    });
    for (const auto& vk: ref_keys) {
        const auto& stream_id = variant_key_id(vk);
        auto [next_key, _] = read_head(store, stream_id);
        if (next_key && store->key_exists(next_key.value()).get()) {
            output.insert(stream_id);
        }
    }
    return output;
}

void fix_slice_rowcounts(std::vector<AppendMapEntry>& entries, size_t complete_rowcount) {
    for(auto& entry : entries) {
        complete_rowcount = entry.slice_and_key_.slice_.fix_row_count(static_cast<ssize_t>(complete_rowcount));
    }
}

TimeseriesDescriptor pack_timeseries_descriptor(
    StreamDescriptor&& descriptor,
    size_t total_rows,
    std::optional<AtomKey>&& next_key,
    arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta) {
    util::check(descriptor.proto().has_index(), "Stream descriptor without index in pack_timeseries_descriptor");
    auto tsd = make_timeseries_descriptor(total_rows, std::move(descriptor), std::move(norm_meta), std::nullopt, std::nullopt, std::move(next_key), false);
    if(ConfigsMap::instance()->get_int("VersionStore.Encoding", 1) == 1) {
        tsd.copy_to_self_proto();
    }
    return tsd;
}

SegmentInMemory incomplete_segment_from_frame(
    const std::shared_ptr<pipelines::InputTensorFrame>& frame,
    size_t existing_rows,
    std::optional<entity::AtomKey>&& prev_key,
    bool allow_sparse
    ) {
    using namespace arcticdb::stream;

    auto offset_in_frame = 0;
    auto slice_num_for_column = 0;
    const auto num_rows = frame->num_rows;
    auto index_tensor = std::move(frame->index_tensor);
    const bool has_index = frame->has_index();
    const auto index = std::move(frame->index);
    SegmentInMemory output;
    auto field_tensors = std::move(frame->field_tensors);

    std::visit([&](const auto& idx) {
        using IdxType = std::decay_t<decltype(idx)>;
        using SingleSegmentAggregator = Aggregator<IdxType, FixedSchema, NeverSegmentPolicy>;

        auto timeseries_desc = index_descriptor_from_frame(frame, existing_rows, std::move(prev_key));
        util::check(!timeseries_desc.fields().empty(), "Expected fields not to be empty in incomplete segment");
        auto norm_meta = timeseries_desc.proto().normalization();
        StreamDescriptor descriptor(std::make_shared<StreamDescriptor::Proto>(std::move(*timeseries_desc.mutable_proto().mutable_stream_descriptor())), timeseries_desc.fields_ptr());
        SingleSegmentAggregator agg{FixedSchema{descriptor, index}, [&](auto&& segment) {
            auto tsd = pack_timeseries_descriptor(std::move(descriptor), existing_rows + num_rows, std::move(prev_key), std::move(norm_meta));
            segment.set_timeseries_descriptor(std::move(tsd));
            output = std::forward<SegmentInMemory>(segment);
        }};

        if (has_index) {
            util::check(static_cast<bool>(index_tensor), "Expected index tensor for index type {}", agg.descriptor().index());
            auto opt_error = aggregator_set_data(agg.descriptor().field(0).type(), index_tensor.value(), agg, 0, num_rows, offset_in_frame, slice_num_for_column,
                                num_rows, allow_sparse);
            if (opt_error.has_value()) {
                opt_error->raise(agg.descriptor().field(0).name());
            }
        }

        for(auto col = 0u; col < field_tensors.size(); ++col) {
            auto dest_col = col + agg.descriptor().index().field_count();
            auto &tensor = field_tensors[col];
            auto opt_error = aggregator_set_data(agg.descriptor().field(dest_col).type(), tensor, agg, dest_col, num_rows, offset_in_frame, slice_num_for_column,
                                num_rows, allow_sparse);
            if (opt_error.has_value()) {
                opt_error->raise(agg.descriptor().field(dest_col).name());
            }
        }

        agg.end_block_write(num_rows);
        agg.commit();
        }, index);

    ARCTICDB_DEBUG(log::version(), "Constructed segment from frame of {} rows and {} columns at offset {}", output.row_count(), output.num_columns(), output.offset());
    return output;
}

folly::Future<arcticdb::entity::VariantKey> write_incomplete_frame(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const std::shared_ptr<InputTensorFrame>& frame,
    std::optional<AtomKey>&& next_key)  {
    using namespace arcticdb::pipelines;

    if (!index_is_not_timeseries_or_is_sorted_ascending(frame)) {
        sorting::raise<ErrorCode::E_UNSORTED_DATA>("When writing/appending staged data in parallel, input data must be sorted.");
    }

    auto index_range = frame->index_range;
    auto segment = incomplete_segment_from_frame(frame, 0, std::move(next_key), false);
    return store->write(
        KeyType::APPEND_DATA,
        VersionId(0),
        stream_id,
        index_range.start_,
        index_range.end_,
        std::move(segment));
}

void write_parallel(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const std::shared_ptr<InputTensorFrame>& frame) {
    // TODO: dynamic bucketize doesn't work with incompletes
    (void)write_incomplete_frame(store, stream_id, frame, std::nullopt).get();
}

std::vector<SliceAndKey> get_incomplete(
    const std::shared_ptr<Store> &store,
    const StreamId &stream_id,
    const pipelines::FilterRange &range,
    uint64_t last_row,
    bool via_iteration,
    bool load_data) {
    using namespace arcticdb::pipelines;

    std::unique_ptr<TimeseriesDescriptor> unused;
    auto entries = get_incomplete_append_slices_for_stream_id(store, stream_id, via_iteration, load_data);

    util::variant_match(range,
                        [](const RowRange &) {
                            util::raise_rte("Only timestamp based ranges supported for filtering.");
                        },
                        [&entries](const IndexRange &index_range) {
                            entries.erase(
                                std::remove_if(std::begin(entries), std::end(entries), [&] (const auto& entry) {
                                    return !intersects(index_range, entry.slice_and_key_.key().index_range());
                                }),
                                std::end(entries));
                        },
                        [](const auto &) {
                            // Don't know what to do with this index
                        }
    );

    fix_slice_rowcounts(entries, last_row);
    std::vector<SliceAndKey> output;
    output.reserve(entries.size());
    for(const auto& entry : entries)
        output.push_back(entry.slice_and_key_);

    return output;
}

void write_head(const std::shared_ptr<Store>& store, const AtomKey& next_key, size_t total_rows) {
    ARCTICDB_DEBUG(log::version(), "Writing append map head with key {}", next_key);
    auto desc = stream_descriptor(next_key.id(), RowCountIndex{}, {});
    SegmentInMemory segment(desc);
    auto tsd = pack_timeseries_descriptor(std::move(desc), total_rows, next_key, {});
    segment.set_timeseries_descriptor(std::move(tsd));
    store->write(KeyType::APPEND_REF, next_key.id(), std::move(segment)).get();
}

void remove_incomplete_segments(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id) {
    delete_keys_of_type_for_stream(store, stream_id, KeyType::APPEND_DATA);
}

std::vector<AppendMapEntry> load_via_list(
        const std::shared_ptr<Store>& store,
        const StreamId& stream_id,
        bool load_data) {
    using namespace arcticdb::pipelines;

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

std::pair<std::optional<AtomKey>, size_t> read_head(const std::shared_ptr<StreamSource>& store, StreamId stream_id) {
    auto ref_key = RefKey{std::move(stream_id), KeyType::APPEND_REF};
    auto output = std::make_pair<std::optional<AtomKey>, size_t>(std::nullopt, 0);

    if(!has_appends_key(store, ref_key))
        return output;

    auto fut = store->read(ref_key);
    auto [key, seg] = std::move(fut).get();
    const auto& tsd = seg.index_descriptor();
    if(tsd.proto().has_next_key()) {
        output.first = decode_key(tsd.proto().next_key());
    }

    output.second = tsd.proto().total_rows();
    return output;
}

std::pair<TimeseriesDescriptor, std::optional<SegmentInMemory>> get_descriptor_and_data(
    const std::shared_ptr<StreamSource>& store,
    const AtomKey& k,
    bool load_data,
    storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) {
    if(load_data) {
        auto [key, seg] = store->read_sync(k, opts);
        return std::make_pair(TimeseriesDescriptor{seg.timeseries_proto(), seg.index_fields()}, std::make_optional<SegmentInMemory>(seg));
    } else {
        auto [key, tsd] = store->read_timeseries_descriptor(k, opts).get();
        return std::make_pair(std::move(tsd), std::nullopt);
    }
}

AppendMapEntry create_entry(const arcticdb::proto::descriptors::TimeSeriesDescriptor& tsd) {
    AppendMapEntry entry;

    if(tsd.has_next_key())
        entry.next_key_ = decode_key(tsd.next_key());

    entry.total_rows_ = tsd.total_rows();
    return entry;
}

AppendMapEntry entry_from_key(const std::shared_ptr<StreamSource>& store, const AtomKey& key, bool load_data) {
    auto opts = storage::ReadKeyOpts{};
    opts.dont_warn_about_missing_key = true;
    auto [tsd, seg] = get_descriptor_and_data(store, key, load_data, opts);
    auto entry = create_entry(tsd.proto());
    auto descriptor = std::make_shared<StreamDescriptor>();
    auto desc = std::make_shared<StreamDescriptor>(tsd.as_stream_descriptor());
    auto index_field_count = desc->index().field_count();
    auto field_count = desc->fields().size();
    if(seg)
        seg->attach_descriptor(desc);

    auto frame_slice = FrameSlice{desc, ColRange{index_field_count, field_count}, RowRange{0, entry.total_rows_}};
    entry.slice_and_key_ = SliceAndKey{std::move(frame_slice), key, std::move(seg)};
    return entry;
}

void append_incomplete(
        const std::shared_ptr<Store>& store,
        const StreamId& stream_id,
        const std::shared_ptr<InputTensorFrame>& frame) {
    using namespace arcticdb::proto::descriptors;
    using namespace arcticdb::stream;
    ARCTICDB_SAMPLE_DEFAULT(AppendIncomplete)
    ARCTICDB_DEBUG(log::version(), "Writing incomplete frame for stream {}", stream_id);

    auto [next_key, total_rows] = read_head(store, stream_id);
    const auto num_rows = frame->num_rows;
    total_rows += num_rows;
    auto desc = frame->desc.clone();
    auto new_key = write_incomplete_frame(store, stream_id, frame, std::move(next_key)).get();


    ARCTICDB_DEBUG(log::version(), "Wrote incomplete frame for stream {}, {} rows, total rows {}", stream_id, num_rows, total_rows);
    write_head(store, to_atom(new_key), total_rows);
}

void append_incomplete_segment(
        const std::shared_ptr<Store>& store,
        const StreamId& stream_id,
        SegmentInMemory &&seg) {
    using namespace arcticdb::proto::descriptors;
    using namespace arcticdb::stream;
    ARCTICDB_SAMPLE_DEFAULT(AppendIncomplete)
    ARCTICDB_DEBUG(log::version(), "Writing incomplete segment for stream {}", stream_id);

    auto [next_key, total_rows] = read_head(store, stream_id);

    auto start_index = TimeseriesIndex::start_value_for_segment(seg);
    auto end_index = TimeseriesIndex::end_value_for_segment(seg);
    auto seg_row_count = seg.row_count();

    auto tsd = pack_timeseries_descriptor(seg.descriptor().clone(), seg_row_count, std::move(next_key), {});
    seg.set_timeseries_descriptor(std::move(tsd));
    util::check(static_cast<bool>(seg.metadata()), "Expected metadata");
    auto new_key = store->write(
            arcticdb::stream::KeyType::APPEND_DATA,
            0,
            stream_id,
            start_index,
            end_index,
            std::move(seg)).get();

    total_rows += seg_row_count;
    ARCTICDB_DEBUG(log::version(), "Wrote incomplete frame for stream {}, {} rows, total rows {}", stream_id, seg_row_count, total_rows);
    write_head(store, to_atom(std::move(new_key)), total_rows);
}

std::vector<AppendMapEntry> get_incomplete_append_slices_for_stream_id(
        const std::shared_ptr<Store> &store,
        const StreamId &stream_id,
        bool via_iteration,
        bool load_data) {
    using namespace arcticdb::pipelines;
    std::vector<AppendMapEntry> entries;

    if(via_iteration) {
        entries = load_via_iteration(store, stream_id, load_data);
    } else {
        entries = load_via_list(store, stream_id, load_data);
    }

    if(!entries.empty()) {
        auto index_desc = entries[0].descriptor().index();

        if (index_desc.type() != IndexDescriptorImpl::Type::ROWCOUNT) {
            std::sort(std::begin(entries), std::end(entries));
        } else {
            // Can't sensibly sort rowcount indexes, so you'd better have written them in the right order
            std::reverse(std::begin(entries), std::end(entries));
        }
    }
    return entries;
}

std::optional<int64_t> latest_incomplete_timestamp(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id
    ) {
    auto [next_key, total_rows] = read_head(store, stream_id);
    if(next_key && store->key_exists(next_key.value()).get())
        return next_key.value().end_time();

    return std::nullopt;
}
}
