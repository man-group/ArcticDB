/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/util/timeouts.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/util/regex_filter.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/pipeline/index_utils.hpp>

#include <folly/gen/Base.h>
#include <folly/Function.h>
#include <folly/Range.h>
#include <folly/futures/Future.h>
#include <folly/Likely.h>
#include <boost/circular_buffer.hpp>

#include <optional>

namespace arcticdb::stream {

template<class IndexType>
StreamDescriptor idx_stream_desc(StreamId stream_id, IndexType index) {
    using DataTypeTag = typename IndexType::TypeDescTag::DataTypeTag;
    // All index segments are row-count indexed in the sense that the keys are
    // already ordered - they don't need an additional index
    return StreamDescriptor{index_descriptor(stream_id, index, {
        scalar_field(DataTypeTag::data_type, "start_index"),
        scalar_field(DataTypeTag::data_type, "end_index"),
        scalar_field(DataType::UINT64, "version_id"),
        scalar_field(stream_id_data_type(stream_id), "stream_id"),
        scalar_field(DataType::UINT64, "creation_ts"),
        scalar_field(DataType::UINT64, "content_hash"),
        scalar_field(DataType::UINT8, "index_type"),
        scalar_field(DataType::UINT8, "key_type")
    })};
}

// This is an augmented index that allows for column slicing which is used for fixed
// schema in the pipeline code
template<class IndexType>
struct IndexSliceDescriptor : StreamDescriptor {
    using DataTypeTag = typename IndexType::TypeDescTag::DataTypeTag;

    explicit IndexSliceDescriptor(const StreamId &stream_id, bool has_column_groups)
            : StreamDescriptor(stream_descriptor(stream_id, IndexType(), {

        scalar_field(DataTypeTag::data_type, "start_index"),
        scalar_field(DataTypeTag::data_type, "end_index"),

        scalar_field(DataType::UINT64, "version_id"),
        scalar_field(stream_id_data_type(stream_id), "stream_id"),
        scalar_field(DataType::UINT64, "creation_ts"),
        scalar_field(DataType::UINT64, "content_hash"),
        scalar_field(DataType::UINT8, "index_type"),
        scalar_field(DataType::UINT8, "key_type"),

        scalar_field(DataType::UINT64, "start_col"),
        scalar_field(DataType::UINT64, "end_col"),
        scalar_field(DataType::UINT64, "start_row"),
        scalar_field(DataType::UINT64, "end_row")
    })) {
        if(has_column_groups) {
            add_field(scalar_field(DataType::UINT64, "hash_bucket"));
            add_field(scalar_field(DataType::UINT64, "num_buckets"));
        }
    }

    static stream::FixedSchema schema(const StreamId &stream_id, bool has_column_groups) {
        IndexSliceDescriptor<IndexType> desc(stream_id, has_column_groups);
        return stream::FixedSchema{desc, IndexType::default_index()};
    }
};

// The compile-time definition of the row and column slices above
using SliceTypeDescriptorTag = TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension ::Dim0>>;

template<class IndexType>
stream::FixedSchema idx_schema(StreamId tsid, const IndexType& index) {
    return stream::FixedSchema{idx_stream_desc(tsid, index), index};
}

inline entity::KeyType key_type_compat(uint8_t kt) {
    auto ret = static_cast<KeyType >(kt);
    //TODO would be nice to retire this at some point
    if(kt > static_cast<uint8_t>(entity::KeyType::UNDEFINED)) {
        constexpr char legacy_key_types[] = {'g', 'G', 'd', 'i', 'V', 'v', 'M', 's', 'l'};
        for(size_t i = 0; i < sizeof(legacy_key_types); ++i) {
            if(static_cast<char>(kt == legacy_key_types[i])) {
                ret = static_cast<entity::KeyType>(i);
                break;
            }
        }
    }

    return ret;
}

template<typename FieldType>
inline KeyType key_type_from_segment(const SegmentInMemory& seg, ssize_t row) {
    auto key_type_val = seg.scalar_at<uint8_t>(row, int(FieldType::key_type)).value();
    return key_type_compat(key_type_val);
}

template<typename FieldType>
inline StreamId stream_id_from_segment(const SegmentInMemory &seg, ssize_t row) {
    if (const auto& fd = seg.descriptor()[int(FieldType::stream_id)]; is_sequence_type(fd.type().data_type()))
        return std::string(seg.string_at(row, int(FieldType::stream_id)).value());
    else
        return seg.scalar_at<timestamp>(row, int(FieldType::stream_id)).value();
}

template<typename FieldType>
auto read_key_row_into_builder(const SegmentInMemory& seg, ssize_t i) {
    return atom_key_builder()
            .gen_id(seg.scalar_at<VersionId>(i, int(FieldType::version_id)).value())
            .creation_ts(seg.scalar_at<timestamp>(i, int(FieldType::creation_ts)).value())
            .content_hash(seg.scalar_at<uint64_t>(i, int(FieldType::content_hash)).value())
            .start_index(pipelines::index::index_start_from_segment<SegmentInMemory, FieldType>(seg, i))
            .end_index(pipelines::index::index_end_from_segment<SegmentInMemory, FieldType>(seg, i));
}

template<typename FieldType>
inline entity::AtomKey read_key_row_impl(const SegmentInMemory &seg, ssize_t i) {
    auto key_type = key_type_from_segment<FieldType>(seg, i);
    auto stream_id = stream_id_from_segment<FieldType>(seg, i);
    auto k = read_key_row_into_builder<FieldType>(seg, i)
            .build(std::move(stream_id), key_type);

    return k;
}

inline entity::AtomKey read_key_row(const SegmentInMemory &seg, ssize_t i) {
    //TODO remove backwards compat after a decent interval
    try {
        auto k = read_key_row_impl<pipelines::index::Fields>(seg, i);
        ARCTICDB_DEBUG(log::storage(), "Read key from row '{}: {}'", k.type(), k.view());
        return k;
    }
    catch(const std::invalid_argument&) {
        auto k = read_key_row_impl<pipelines::index::LegacyFields>(seg, i);
        ARCTICDB_DEBUG(log::storage(), "Read legacy key from row '{}: {}'", k.type(), k.view());
        return k;
    }
}

using KeyMemSegmentPair = std::pair<entity::VariantKey, SegmentInMemory>;

class IndexRangeFilter {
  public:
    explicit IndexRangeFilter(IndexRange index_range) : index_range_(std::move(index_range)) {}

    bool accept_index(const IndexValue &index) {
        return index_range_.accept(index);
    }

    //TODO are we interested in the end field?
    bool key_within_index_range(const entity::AtomKey &key) {
        return accept_index(key.start_index());
    }

  private:
    IndexRange index_range_;
};

template<class KeyIt>
class KeyRangeIterator : public IndexRangeFilter {
  public:
    KeyRangeIterator(const IndexRange &index_range, folly::Range<KeyIt> rg) :
        IndexRangeFilter(index_range), key_rg_(rg), current_(rg.begin()) {}

    std::optional<typename KeyIt::value_type> next(folly::Duration) {
        while (true) {
            if (current_ == key_rg_.end())
                return std::nullopt;
            auto res = *current_;
            ++current_;
            if (key_within_index_range(res))
                return res; // TODO keep track of first / last case
        }
    }

  private:
    folly::Range<KeyIt> key_rg_;
    KeyIt current_;
};

inline auto generate_segments_from_keys(
    arcticdb::stream::StreamSource &read_store,
    std::size_t prefetch_window,
    const storage::ReadKeyOpts opts) {
    using namespace folly::gen;
    return
        map([&read_store](auto &&key) {
            ARCTICDB_DEBUG(log::inmem(), "Getting segment for key {}: {}", key.type(), key.view());
            return read_store.read_sync(std::forward<decltype(key)>(key));
        })
            | window(prefetch_window)
            | move
            | map([opts](auto &&key_seg) {
                try {
                    return std::make_optional(std::forward<decltype(key_seg)>(key_seg));
                } catch(storage::KeyNotFoundException& e) {
                    if (opts.ignores_missing_key_) {
                        return std::optional<ReadKeyOutput>();
                    }
                    throw storage::KeyNotFoundException(std::move(e.keys()));
                }
            })
            | filter() // By default removes falsy
            | map([](auto&& opt) { return std::forward<decltype(opt)>(opt).value(); });
}

inline auto generate_keys_from_segments(
    arcticdb::stream::StreamSource &read_store,
    entity::KeyType expected_key_type,
    std::optional<entity::KeyType> expected_index_type = std::nullopt) {
    return folly::gen::map([expected_key_type, expected_index_type, &read_store](auto &&key_seg) {
        return folly::gen::detail::GeneratorBuilder<entity::AtomKey>() + [&](auto &&yield) {
            std::stack<std::pair<entity::VariantKey, SegmentInMemory>> key_segs;
            key_segs.push(std::forward<decltype(key_seg)>(key_seg));
            while(!key_segs.empty()) {
                auto [key, seg] = std::move(key_segs.top());
                key_segs.pop();
                for (ssize_t i = 0; i < ssize_t(seg.row_count()); ++i) {
                    auto read_key = read_key_row(seg, i);
                    if(read_key.type() != expected_key_type) {
                        util::check_arg(expected_index_type && read_key.type() == expected_index_type.value(),
                            "Found unsupported key type in index segment. Expected {} or (index) {}, actual {}",
                            expected_key_type, expected_index_type.value(), read_key
                        );
                        key_segs.push(read_store.read_sync(read_key));
                    }
                    yield(read_key);
                }
            }
        };
    })
     | folly::gen::concat;
}

template<typename SegmentIteratorType>
std::optional<KeyMemSegmentPair> next_non_empty_segment(SegmentIteratorType &iterator_segments, folly::Duration timeout) {
    std::optional<KeyMemSegmentPair> ks_pair;
    while (!ks_pair) {
        ks_pair = std::move(iterator_segments.next(timeout));
        if (!ks_pair)
            return std::nullopt;
        if (ks_pair.value().second.row_count() == 0)
            ks_pair.reset();
    }
    return ks_pair;
}

template<class KeyIt, int prefetch_capacity>
class SegmentIterator : public IndexRangeFilter {
  public:
    SegmentIterator(const IndexRange &index_range,
                    KeyIt &&key_it,
                    std::shared_ptr<StreamSource> read_store) :
        IndexRangeFilter(index_range),
        key_it_(std::move(key_it)),
        read_store_(std::move(read_store)){
        init_prefetch();
    }

    SegmentIterator(const TimestampRange &ts_rg,
                    KeyIt &&key_it,
                    const std::shared_ptr<StreamSource>& read_store) :
        SegmentIterator(ts_rg.first, ts_rg.second, std::move(key_it), read_store) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(SegmentIterator)

    std::optional<KeyMemSegmentPair> next(folly::Duration timeout) {
        while (true) {
            if (prefetch_buffer_.empty())
                return std::nullopt;

            auto res = std::move(prefetch_buffer_.front()).get(timeout);
            prefetch_buffer_.pop_front();
            enqueue_next_key_read_request(timeout);

            if (key_within_index_range(to_atom(res.first)))
                return std::make_optional(std::move(res));
        }
    }

  private:
    void init_prefetch() {
        for (int p = prefetch_capacity; p != 0 && enqueue_next_key_read_request(); --p) {
            // Do nothing
        }
    }

    bool enqueue_next_key_read_request(folly::Duration timeout = util::timeout::get_default()) {
        if (auto opt_key = key_it_.next(timeout); opt_key) {
            prefetch_buffer_.push_back(std::move(read_store_->read(opt_key.value())));
            return true;
        } else {
            return false;
        }
    }

    KeyIt key_it_;
    std::shared_ptr<StreamSource> read_store_;
    using CircularBuffer = boost::circular_buffer<folly::Future<KeyMemSegmentPair>>;
    CircularBuffer prefetch_buffer_ = CircularBuffer{prefetch_capacity};
};

template<class SegmentIt>
class KeysFromSegIterator : public IndexRangeFilter {
  public:
    KeysFromSegIterator(const IndexRange &index_range, SegmentIt &&seg_it) :
        IndexRangeFilter(index_range), seg_it_(std::move(seg_it)) {}

    KeysFromSegIterator(const IndexRange &index_range, SegmentIt &&seg_it, std::optional<KeyMemSegmentPair> &key_seg) :
        IndexRangeFilter(index_range), seg_it_(std::move(seg_it)), key_seg_(std::move(key_seg)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(KeysFromSegIterator)

    std::optional<entity::AtomKey> next(folly::Duration timeout) {
        while (true) {
            if (!key_seg_ && !(row_id = 0, key_seg_ = next_non_empty_segment(seg_it_, timeout)))
                return std::nullopt;

            auto val = read_key_row(key_seg_.value().second, row_id);
            ++row_id;
            if (row_id == key_seg_.value().second.row_count()) {
                key_seg_ = std::nullopt;
            }
            if (key_within_index_range(val))
                return std::optional<entity::AtomKey>{val};
        }
    }
  private:
    SegmentIt seg_it_;
    std::optional<KeyMemSegmentPair> key_seg_;
    std::size_t row_id = 0;
};

inline std::set<StreamId> filter_by_regex(const std::set<StreamId>& results, const std::optional<std::string> &opt_regex) {
    if (!opt_regex) {
        return results;
    }
    std::set<StreamId> filtered_results;
    util::RegexPattern pattern(opt_regex.value());
    util::Regex regex{pattern};

    for (auto &s_id: results) {
        auto string_id = std::holds_alternative<StringId>(s_id) ? std::get<StringId>(s_id) : std::string();
        if (regex.match(string_id)) {
            filtered_results.insert(s_id);
        }
    }
    return filtered_results;
}

inline std::vector<std::string> get_index_columns_from_descriptor(const TimeseriesDescriptor& descriptor) {
    const auto& norm_info = descriptor.proto().normalization();
    const auto& stream_descriptor = descriptor.proto().stream_descriptor();
    // For explicit integer indexes, the index is actually present in the first column even though the field_count
    // is 0.
    ssize_t index_till;
    const auto& common = norm_info.df().common();
    if(auto idx_type = common.index_type_case(); idx_type == arcticdb::proto::descriptors::NormalizationMetadata_Pandas::kIndex)
        index_till = common.index().is_not_range_index() ? 1 : stream_descriptor.index().field_count();
    else
        index_till = 1 + common.multi_index().field_count();  //# The value of field_count is len(index) - 1

    std::vector<std::string> index_columns;
    for(auto field_idx = 0; field_idx < index_till; ++field_idx)
        index_columns.push_back(stream_descriptor.fields(field_idx).name());

    return index_columns;
}

inline IndexRange get_range_from_segment(const Index& index, const SegmentInMemory& segment) {
    return util::variant_match(index, [&segment] (auto index_type) {
        using IndexType = decltype(index_type);
        auto start = IndexType::start_value_for_segment(segment);
        auto end = IndexType::end_value_for_segment(segment);
        return IndexRange{start, end};
    });
}

template <typename ClockType>
storage::KeySegmentPair make_target_key(KeyType key_type,
                           const StreamId &stream_id,
                           VersionId version_id,
                           const VariantKey &source_key,
                           Segment&& segment) {
    if (is_ref_key_class(key_type)) {
        return {RefKey{stream_id, key_type}, std::move(segment)};
    } else {
        util::check(!is_ref_key_class(variant_key_type(source_key)),
            "Cannot convert ref key {} to {}", source_key, key_type);
        auto& atom_source_key = to_atom(source_key);
        auto new_key = atom_key_builder().version_id(version_id).creation_ts(ClockType::nanos_since_epoch())
            .start_index(atom_source_key.start_index()).end_index(atom_source_key.end_index())
            .content_hash(atom_source_key.content_hash())
            .build(stream_id, key_type);

        return {new_key, std::move(segment)};
    }
}

} // namespace arctic::stream

