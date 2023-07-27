/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/util/timeouts.hpp>
#include <arcticdb/column_store/row_ref.hpp>
#include <arcticdb/pipeline/index_utils.hpp>

#include <folly/gen/Base.h>

#include <chrono>

namespace arcticdb::stream {

template<class SegmentIt, class RowType>
class RowsFromSegIterator : public IndexRangeFilter {
  public:
    RowsFromSegIterator(const IndexRange &index_range, SegmentIt &&seg_it) :
        IndexRangeFilter(index_range), seg_it_(std::move(seg_it)), seg_(std::nullopt), row_id(0) {}

    RowsFromSegIterator &operator=(RowsFromSegIterator &&that) = default;
    RowsFromSegIterator(RowsFromSegIterator &&that) = default;
    RowsFromSegIterator &operator=(const RowsFromSegIterator &that) = delete;
    RowsFromSegIterator(const RowsFromSegIterator &that) = delete;

    std::optional<RowType> next(folly::Duration timeout = util::timeout::get_default()) {
        prev_seg_ = std::nullopt;
        while (true) {
            while (!seg_) {
                auto key_seg = std::move(seg_it_.next(timeout));
                if (!key_seg)
                    return std::nullopt;

                row_id = 0;

                if (key_seg.value().second.row_count() == 0) {
                    seg_ = std::nullopt;
                } else {
                    seg_ = SegmentInMemory(std::move(key_seg.value().second));
                }
            }

            auto index_type = seg_->descriptor().index().type();
            auto res = std::make_optional<RowType>(seg_.value().template make_row_ref<RowType>(row_id));

            // Not filtering rows where we have a rowcount index - the assumption is that it's essentially an un-indexed blob
            // that we need to segment somehow.
            auto accept = index_type == IndexDescriptor::ROWCOUNT || accept_index(pipelines::index::index_start_from_row(res.value(), index_type).value());
            if (++row_id == seg_->row_count()) {
                prev_seg_ = seg_;
                seg_ = std::nullopt;
            }

            if (accept)
                return res;
        }
    }

  private:
    SegmentIt seg_it_;
    std::optional<SegmentInMemory> seg_;
    std::optional<SegmentInMemory> prev_seg_;
    std::size_t row_id;
};

template<class KeyType, typename KeySupplierType, typename RowType>
class StreamReader {
    static constexpr size_t IDX_PREFETCH_WINDOW = 256;
    static constexpr size_t DATA_PREFETCH_WINDOW = 32;

  public:
    using KeyRangeIteratorType = KeyRangeIterator<typename decltype(KeySupplierType()())::const_iterator>;
    using IdxSegmentIteratorType = SegmentIterator<KeyRangeIteratorType, IDX_PREFETCH_WINDOW>;
    using KeysFromSegIteratorType = KeysFromSegIterator<IdxSegmentIteratorType>;
    using DataSegmentIteratorType = SegmentIterator<KeysFromSegIteratorType, DATA_PREFETCH_WINDOW>;
    using RowsIteratorType = RowsFromSegIterator<DataSegmentIteratorType, RowType>;

    StreamReader(KeySupplierType &&gen, std::shared_ptr<StreamSource> store, const storage::ReadKeyOpts opts = storage::ReadKeyOpts{},  const IndexRange &index_range = unspecified_range()) :
        key_gen_(std::move(gen)),
        index_range_(index_range),
        store_(store),
        opts_(opts),
        read_timeout_(util::timeout::get_default()) {
        ARCTICDB_DEBUG(log::inmem(), "Creating stream reader");
    }

    IdxSegmentIteratorType iterator_indexes() {
        auto idx_key_container = key_gen_();
        auto idx_key_rg = folly::range(idx_key_container.begin(), idx_key_container.end());
        auto key_it = KeyRangeIteratorType(index_range_, idx_key_rg);
        auto idx_seg_it = IdxSegmentIteratorType(index_range_, std::move(key_it), store_);
        return idx_seg_it;
    }

    DataSegmentIteratorType iterator_segments() {
        auto idx_seg_it = iterator_indexes();
        auto keys_from_seg = KeysFromSegIteratorType{index_range_, std::move(idx_seg_it)};
        auto res = DataSegmentIteratorType(index_range_, std::move(keys_from_seg), store_);
        return res;
    }

    RowsIteratorType iterator_rows() {
        auto data_seg_it = iterator_segments();
        auto res = RowsIteratorType(index_range_, std::move(data_seg_it));
        return res;
    }

    auto generate_rows() {
        return folly::gen::from(key_gen_())
            | generate_segments_from_keys(*store_, IDX_PREFETCH_WINDOW, opts_)
            | generate_keys_from_segments(*store_, entity::KeyType::TABLE_DATA, entity::KeyType::TABLE_INDEX)
            | generate_segments_from_keys(*store_, DATA_PREFETCH_WINDOW, opts_)
            | generate_rows_from_data_segments();
    }

    auto generate_data_keys() {
        return folly::gen::from(key_gen_())
            | generate_segments_from_keys(*store_, IDX_PREFETCH_WINDOW, opts_)
            | generate_keys_from_segments(*store_, entity::KeyType::TABLE_DATA, entity::KeyType::TABLE_INDEX);
    }

    auto &&generate_rows_from_data_segments() {
        return folly::gen::map([](auto &&key_seg) {
            return folly::gen::detail::GeneratorBuilder<RowRef>() + [&](auto &&yield) {
                auto[key, seg] = std::move(key_seg);
                for (std::size_t i = 0; i < seg.row_count(); ++i) {
                    yield(RowRef{i, seg});
                }
            };
        })
        | folly::gen::concat;
    }

    template<class Visitor>
    void foreach_row(Visitor &&visitor) {
        auto it = iterator_rows();
        for (auto opt_val = it.next(); opt_val; opt_val = it.next()) {
            visitor(*opt_val);
        }
    }

  private:
    KeySupplierType key_gen_;
    IndexRange index_range_;
    std::shared_ptr<StreamSource> store_;
    storage::ReadKeyOpts opts_;
    folly::Duration read_timeout_;
};

}