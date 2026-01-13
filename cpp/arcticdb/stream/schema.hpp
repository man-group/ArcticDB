/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb::stream {

using namespace arcticdb::entity;

class FixedSchema {
  public:
    FixedSchema(StreamDescriptor desc, Index index);

    static FixedSchema default_schema(const Index& index, const StreamId& stream_id);

    void check(std::size_t pos, TypeDescriptor td) const;

    [[nodiscard]] StreamDescriptor default_descriptor() const;

    static position_t get_column_idx_by_name(
            SegmentInMemory& seg, std::string_view col_name, TypeDescriptor, size_t, size_t
    );

    const Index& index() const;

    Index& index();

  private:
    StreamDescriptor desc_;
    Index index_;
};

StreamDescriptor default_dynamic_descriptor(const StreamDescriptor& desc, const Index& index);

class DynamicSchema {
  public:
    explicit DynamicSchema(const StreamDescriptor& desc, const Index& index);

    static DynamicSchema default_schema(const Index& index, const StreamId& stream_id);

    void check(std::size_t pos, TypeDescriptor td) const;

    static position_t get_column_idx_by_name(
            SegmentInMemory& seg, std::string_view col_name, TypeDescriptor desc, size_t expected_size,
            size_t existing_size
    );

    const Index& index() const;

    Index& index();

    [[nodiscard]] StreamDescriptor default_descriptor() const;

  private:
    StreamDescriptor desc_;
    Index index_;
};

using VariantSchema = std::variant<FixedSchema, DynamicSchema>;

} // namespace arcticdb::stream