/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/magic_num.hpp>

namespace arcticdb {
class Store;
}

namespace arcticdb::pipelines {

struct AxisRange : std::pair<size_t, size_t> {
    using std::pair<size_t, size_t>::pair;

    [[nodiscard]] size_t diff() const { return first > second ? 0 : second - first; }

    [[nodiscard]] bool contains(size_t v) const { return first <= v && v < second; }

    [[nodiscard]] size_t start() const { return first; }

    [[nodiscard]] size_t end() const { return second; }

    [[nodiscard]] bool empty() const { return first >= second; }

    struct Hasher {
        template<class T>
        requires std::is_base_of_v<AxisRange, T>
        std::size_t operator()(const T& r) const {
            // try to make better use of msb lsb given how F14 is implemented
#ifdef _WIN32
            return r.first ^ _byteswap_uint64(r.second);
#else
            return r.first ^ __builtin_bswap64(r.second);
#endif
        }
    };
};

struct ColRange : AxisRange {
    using AxisRange::AxisRange;
};

struct RowRange : AxisRange {
    using AxisRange::AxisRange;
};

/*
 * This class is mostly (exclusively?) used in SliceAndKey objects, where the duplication of the StreamDescriptor
 * with the SegmentInMemory is confusing and error-prone. Where possible do not add more uses of this class and
 * SliceAndKey, prefer RangesAndKey or SegmentAndSlice depending on requirements.
 * FrameSlice stores the vertical (RowRange) and horizontal (ColRange) offsets for a table-subsection, as well as
 * (optionally) a descriptor for the source stream/index data. For dynamic_schema's bucketizing, it also stores
 * the hash bucket it represents and the number of total buckets that are present for the current calculation
 */
struct FrameSlice {
    FrameSlice() = default;

    FrameSlice(
            std::shared_ptr<entity::StreamDescriptor> desc, ColRange col_range, RowRange row_range,
            std::optional<uint64_t> hash = std::nullopt, std::optional<uint64_t> num_buckets = std::nullopt,
            std::optional<std::vector<size_t>> indices = std::nullopt
    ) :
        col_range(std::move(col_range)),
        row_range(std::move(row_range)),
        desc_(std::move(desc)),
        hash_bucket_(hash),
        num_buckets_(num_buckets),
        indices_(std::move(indices)) {}

    FrameSlice(
            ColRange col_range, RowRange row_range, std::optional<size_t> hash_bucket = std::nullopt,
            std::optional<uint64_t> num_buckets = std::nullopt
    ) :
        col_range(std::move(col_range)),
        row_range(std::move(row_range)),
        hash_bucket_(hash_bucket),
        num_buckets_(num_buckets) {}

    explicit FrameSlice(const SegmentInMemory& seg);

    [[nodiscard]] const std::shared_ptr<entity::StreamDescriptor>& desc() const {
        util::check(static_cast<bool>(desc_), "Got null descriptor in frame slice");
        return desc_;
    }

    std::shared_ptr<entity::StreamDescriptor>& desc() {
        util::check(static_cast<bool>(desc_), "Got null descriptor in frame slice");
        return desc_;
    }

    [[nodiscard]] std::optional<size_t> hash_bucket() const { return hash_bucket_; }

    [[nodiscard]] std::optional<size_t> num_buckets() const { return num_buckets_; }

    void set_desc(const std::shared_ptr<entity::StreamDescriptor>& desc) { desc_ = desc; }

    [[nodiscard]] const ColRange& columns() const { return col_range; }
    [[nodiscard]] const RowRange& rows() const { return row_range; }

    [[nodiscard]] std::size_t absolute_field_col(std::size_t col) const {
        if (indices_)
            return indices_->at(col) - desc()->index().field_count();
        else
            return col + col_range.first - desc()->index().field_count();
    }

    [[nodiscard]] const auto& non_index_field(std::size_t pos) const {
        return desc()->field(pos + desc()->index().field_count());
    }

    void adjust_rows(size_t row_count) { row_range.second = row_range.first + row_count; }

    void adjust_columns(size_t column_count) { col_range.second = col_range.first + column_count; }

    ssize_t fix_row_count(ssize_t rows) {
        const auto diff = row_range.diff();
        row_range.first = rows;
        row_range.second = rows + diff;
        return static_cast<ssize_t>(row_range.second);
    }

    friend std::weak_ordering operator<=>(const FrameSlice& a, const FrameSlice& b) {
        return std::tie(a.col_range.first, a.row_range.first) <=> std::tie(b.col_range.first, b.row_range.first);
    }

    friend bool operator==(const FrameSlice& a, const FrameSlice& b) {
        return a.row_range == b.row_range && a.col_range == b.col_range;
    }

    void check_magic() const { magic_.check(); }

    ColRange col_range;
    RowRange row_range;

  private:
    // never contains index field
    std::shared_ptr<entity::StreamDescriptor> desc_;
    std::optional<uint64_t> hash_bucket_;
    std::optional<uint64_t> num_buckets_;
    std::optional<std::vector<size_t>> indices_;
    util::MagicNum<'F', 's', 'l', 'c'> magic_;
};

// Collection of these objects is the input to batch_read_uncompressed
struct RangesAndKey {
    explicit RangesAndKey(const FrameSlice& frame_slice, entity::AtomKey&& key, bool is_incomplete) :
        row_range_(frame_slice.rows()),
        col_range_(frame_slice.columns()),
        key_(std::move(key)),
        is_incomplete_(is_incomplete) {}

    explicit RangesAndKey(RowRange row_range, ColRange col_range, entity::AtomKey key) :
        row_range_(std::move(row_range)),
        col_range_(std::move(col_range)),
        key_(std::move(key)) {}
    RangesAndKey() = delete;
    ARCTICDB_MOVE_COPY_DEFAULT(RangesAndKey)

    const RowRange& row_range() const { return row_range_; }

    const ColRange& col_range() const { return col_range_; }

    timestamp start_time() const { return key_.start_time(); }

    timestamp end_time() const {
        // end_index from the key is 1 nanosecond larger than the index value of the last row in the row-slice
        return key_.end_time() - 1;
    }

    bool is_incomplete() const { return is_incomplete_; }

    friend bool operator==(const RangesAndKey& left, const RangesAndKey& right) {
        return left.row_range_ == right.row_range_ && left.col_range_ == right.col_range_ && left.key_ == right.key_;
    }

    bool operator!=(const RangesAndKey& right) const { return !(*this == right); }

    RowRange row_range_;
    ColRange col_range_;
    entity::AtomKey key_;
    bool is_incomplete_{false};
};

/*
 * The return type of batch_read_uncompressed, the entry point to the processing pipeline.
 * Intended as a replacement for SliceAndKey without the baggage that class has accumulated, use in preference where
 * possible.
 */
struct SegmentAndSlice {
    explicit SegmentAndSlice(RangesAndKey&& ranges_and_key, SegmentInMemory&& segment_in_memory) :
        ranges_and_key_(std::move(ranges_and_key)),
        segment_in_memory_(std::move(segment_in_memory)) {}
    SegmentAndSlice() = delete;
    ARCTICDB_MOVE_COPY_DEFAULT(SegmentAndSlice)

    RangesAndKey ranges_and_key_;
    SegmentInMemory segment_in_memory_;
};

/*
 * Deprecated, use SegmentAndSlice or RangesAndKey where possible.
 * SliceAndKey is a composite type, wrapping:
 * 1. A FrameSlice instance, detailing the context of contained data
 * 2. An optional AtomKey instance, enabling retrieval of segment data from storage
 * 3. An optional SegmentInMemory instance
 *
 * SliceAndKey is designed to abstract the user away from whether the data is in storage or in memory. On read, if the
 * data is in storage it will be retrieved.
 *
 * Note that a single SliceAndKey object can only refer to at most one on-disk segment.
 */
struct SliceAndKey {
    SliceAndKey() = default;

    SliceAndKey(FrameSlice slice, entity::AtomKey key) : slice_(std::move(slice)), key_(std::move(key)) {}

    SliceAndKey(FrameSlice slice, entity::AtomKey key, std::optional<SegmentInMemory> segment) :
        segment_(std::move(segment)),
        slice_(std::move(slice)),
        key_(std::move(key)) {}

    SliceAndKey(SegmentInMemory&& seg, FrameSlice&& slice) : segment_(std::move(seg)), slice_(std::move(slice)) {}

    explicit SliceAndKey(SegmentInMemory&& seg) : segment_(std::move(seg)), slice_(*segment_) {}

    friend bool operator==(const SliceAndKey& left, const SliceAndKey& right) {
        return left.key_ == right.key_ && left.slice_ == right.slice_;
    }

    void ensure_segment(const std::shared_ptr<Store>& store) const;

    SegmentInMemory& segment(const std::shared_ptr<Store>& store);

    const SegmentInMemory& segment() const&;

    SegmentInMemory&& segment() &&;

    SegmentInMemory&& release_segment(const std::shared_ptr<Store>& store) const;

    const SegmentInMemory& segment(const std::shared_ptr<Store>& store) const;

    template<typename Callable>
    auto apply(const std::shared_ptr<Store>& store, Callable&& c) {
        ensure_segment(store);
        return c(*segment_, slice_, key_);
    }

    const FrameSlice& slice() const& { return slice_; }

    FrameSlice& slice() & { return slice_; }

    FrameSlice&& slice() && { return std::move(slice_); }

    bool invalid() const { return (!segment_ && !key_) || (segment_ && segment_->is_null()); }

    const AtomKey& key() const& {
        util::check(static_cast<bool>(key_), "No key found");
        return *key_;
    }

    AtomKey&& key() && {
        util::check(static_cast<bool>(key_), "No key found");
        return std::move(*key_);
    }

    void unset_segment() { segment_ = std::nullopt; }

    void set_segment(SegmentInMemory&& seg) { segment_ = std::move(seg); }

    mutable std::optional<SegmentInMemory> segment_;
    FrameSlice slice_;
    std::optional<entity::AtomKey> key_;
};

inline std::weak_ordering operator<=>(const SliceAndKey& a, const SliceAndKey& b) { return a.slice_ <=> b.slice_; }

} // namespace arcticdb::pipelines

namespace fmt {
template<class T>
struct formatter<T, std::enable_if_t<std::is_base_of_v<arcticdb::pipelines::AxisRange, T>, char>> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const T& rg, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "Range[{:d}, {:d}]", rg.first, rg.second);
    }
};

template<class T>
struct formatter<T, std::enable_if_t<std::is_base_of_v<arcticdb::pipelines::FrameSlice, T>, char>> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const T& slice, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "Rows: {}\tColumns: {}", slice.row_range, slice.col_range);
    }
};

template<>
struct formatter<arcticdb::pipelines::SliceAndKey> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::pipelines::SliceAndKey sk, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}{}", sk.slice(), sk.key());
    }
};

template<>
struct formatter<arcticdb::pipelines::RangesAndKey> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::pipelines::RangesAndKey sk, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{},{},{},{}", sk.row_range(), sk.col_range(), sk.key_, sk.is_incomplete());
    }
};

} // namespace fmt
