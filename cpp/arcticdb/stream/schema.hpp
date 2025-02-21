/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/stream/index.hpp>

#include <folly/Function.h>

namespace arcticdb::stream {

using namespace arcticdb::entity;

class FixedSchema {
  public:
    FixedSchema(StreamDescriptor desc, Index index) : desc_(std::move(desc)), index_(std::move(index)) {}

    static FixedSchema default_schema(const Index& index, const StreamId& stream_id) {
        return util::variant_match(index, [&stream_id](auto idx) {
            using IndexType = std::remove_reference_t<decltype(idx)>;
            return FixedSchema(StreamDescriptor(stream_id), IndexType::default_index());
        });
    }

    void check(std::size_t pos, TypeDescriptor td) const {
        util::check_range(pos, desc_.fields().size(), "No field in fixed schema at supplied idx");
        auto exp_td = desc_.fields(pos).type();
        util::check_arg(td == exp_td, "Incompatible type for pos={}, expected {}, actual {}", pos, exp_td, td);
    }

    [[nodiscard]] StreamDescriptor default_descriptor() const { return desc_.clone(); }

    static position_t get_column_idx_by_name(
            SegmentInMemory& seg, std::string_view col_name, TypeDescriptor, size_t, size_t
    ) {
        auto opt_col = seg.column_index(col_name);
        util::check(static_cast<bool>(opt_col), "Column {} not found", col_name);
        return static_cast<position_t>(opt_col.value());
    }

    const Index& index() const { return index_; }

    Index& index() { return index_; }

  private:
    StreamDescriptor desc_;
    Index index_;
};

inline StreamDescriptor default_dynamic_descriptor(const StreamDescriptor& desc, const Index& index) {
    return util::variant_match(index, [&desc](auto idx) { return idx.create_stream_descriptor(desc.id(), {}); });
}

class DynamicSchema {
  public:
    explicit DynamicSchema(const StreamDescriptor& desc, const Index& index) :
        desc_(default_dynamic_descriptor(desc, index)),
        index_(index) {}

    static DynamicSchema default_schema(const Index& index, const StreamId& stream_id) {
        return util::variant_match(index, [stream_id](auto idx) {
            using IndexType = std::remove_reference_t<decltype(idx)>;
            return DynamicSchema(StreamDescriptor(stream_id), IndexType::default_index());
        });
    }

    void check(std::size_t pos ARCTICDB_UNUSED, TypeDescriptor td ARCTICDB_UNUSED) const {}

    static position_t get_column_idx_by_name(
            SegmentInMemory& seg, std::string_view col_name, TypeDescriptor desc, size_t expected_size,
            size_t existing_size
    ) {
        auto opt_col = seg.column_index(col_name);
        if (!opt_col) {
            const size_t init_size = expected_size > existing_size ? expected_size - existing_size : 0;
            FieldWrapper new_field(desc, col_name);
            position_t pos = seg.add_column(new_field.field(), init_size, AllocationType::DYNAMIC);
            ARCTICDB_TRACE(log::version(), "Added column {} to position: {}", col_name, pos);
            return pos;
        } else {
            return static_cast<position_t>(*opt_col);
        }
    }

    const Index& index() const { return index_; }

    Index& index() { return index_; }

    [[nodiscard]] StreamDescriptor default_descriptor() const { return desc_.clone(); }

  private:
    StreamDescriptor desc_;
    Index index_;
};

using VariantSchema = std::variant<FixedSchema, DynamicSchema>;

} // namespace arcticdb::stream